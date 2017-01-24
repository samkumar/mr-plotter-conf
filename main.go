/*
 * Copyright (c) 2017, Sam Kumar <samkumar@berkeley.edu>
 * Copyright (c) 2017, University of California, Berkeley
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package main

import (
    "bufio"
    "context"
    "errors"
    "fmt"
    "os"
    "strings"

    "github.com/SoftwareDefinedBuildings/mr-plotter/accounts"
    etcd "github.com/coreos/etcd/clientv3"
)

const ALL_TAG_SYMBOL = "<ALL STREAMS>"

func main() {
    etcdEndpoint := os.Getenv("ETCD_ENDPOINT")
    if len(etcdEndpoint) == 0 {
        etcdEndpoint = "localhost:2379"
    }
    etcdKeyPrefix := os.Getenv("ETCD_KEY_PREFIX")
    if len(etcdKeyPrefix) != 0 {
        accounts.SetEtcdKeyPrefix(etcdKeyPrefix)
        fmt.Printf("Using Mr. Plotter configuration '%s'\n", etcdKeyPrefix)
    }
    etcdClient, err := etcd.New(etcd.Config{Endpoints: []string{etcdEndpoint}})
    if err != nil {
        fmt.Printf("Could not connect to etcd: %v\n", err)
        os.Exit(1)
    }

    /* Start the REPL. */
    scanner := bufio.NewScanner(os.Stdin)
    for {
        fmt.Print("Mr. Plotter Accounts> ")
        if !scanner.Scan() {
            break
        }
        result := scanner.Text()
        accounts_exec(etcdClient, result)
    }

    fmt.Println()
    if err := scanner.Err(); err != nil {
        fmt.Printf("Exiting: %v\n", err)
    }
}

func sliceToSet(tagSlice []string) map[string]struct{} {
    tagSet := make(map[string]struct{})
    for _, tag := range tagSlice {
        tagSet[tag] = struct{}{}
    }
    return tagSet
}

func setToSlice(tagSet map[string]struct{}) []string {
    tagSlice := make([]string, 0, len(tagSet))
    for tag := range tagSet {
        tagSlice = append(tagSlice, tag)
    }
    return tagSlice
}

var errorTxFail = errors.New("Transacation for atomic update failed; try again")
var errorAlreadyExists = errors.New("Already exists")

var ops = map[string]func(ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
    "adduser": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("adduser - creates a new user account")
            fmt.Println("Usage: adduser username password [tag1] [tag2] ...")
            return nil
        }
        tagSet := sliceToSet(tokens[3:])
        tagSet[accounts.PUBLIC_TAG] = struct{}{}
        acc := &accounts.MrPlotterAccount{Username: tokens[1], Tags: tagSet}
        acc.SetPassword([]byte(tokens[2]))
        success, err := accounts.UpsertAccountAtomically(ctx, etcdClient, acc)
        if !success {
            return errorAlreadyExists
        }
        return err
    },
    "setpassword": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 3 {
            fmt.Println("setpassword - sets a user's password")
            fmt.Println("Usage: setpassword username password")
            return nil
        }
        acc, err := accounts.RetrieveAccount(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        acc.SetPassword([]byte(tokens[2]))
        success, err := accounts.UpsertAccountAtomically(ctx, etcdClient, acc)
        if !success {
            return errorTxFail
        }
        return err
    },
    "rmuser": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 2 {
            fmt.Println("rmuser - deletes user accounts")
            fmt.Println("Usage: rmuser username1 [username2] [username3] ...")
            return nil
        }
        for _, username := range tokens[1:] {
            err := accounts.DeleteAccount(ctx, etcdClient, username)
            if err != nil {
                return err
            }
        }
        return nil
    },
    "rmusers": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 2 {
            fmt.Println("rmusers - deletes all user accounts whose username begins with a certain prefix")
            fmt.Println("Usage: rmusers usernameprefix")
            return nil
        }
        n, err := accounts.DeleteMultipleAccounts(ctx, etcdClient, tokens[1])
        if n == 1 {
            fmt.Println("Deleted 1 account")
        } else {
            fmt.Printf("Deleted %v accounts\n", n)
        }
        return err
    },
    "grant": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("grant - grants a user permission to view streams with one or more tags")
            fmt.Println("Usage: grant username tag1 [tag2] [tag3] ...")
            return nil
        }
        acc, err := accounts.RetrieveAccount(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        for _, tag := range tokens[2:] {
            if _, ok := acc.Tags[tag]; !ok {
                acc.Tags[tag] = struct{}{}
            }
        }
        success, err := accounts.UpsertAccountAtomically(ctx, etcdClient, acc)
        if !success {
            return errorTxFail
        }
        return err
    },
    "revoke": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("revoke - revokes tags from a user's permission list")
            fmt.Println("Usage: revoke username tag1 [tag2] [tag3] ...")
            return nil
        }
        acc, err := accounts.RetrieveAccount(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        for _, tag := range tokens[2:] {
            if tag == accounts.PUBLIC_TAG {
                return fmt.Errorf("All user accounts are assigned the \"%s\" tag\n", accounts.PUBLIC_TAG)
            }
            if _, ok := acc.Tags[tag]; ok {
                delete(acc.Tags, tag)
            }
        }
        success, err := accounts.UpsertAccountAtomically(ctx, etcdClient, acc)
        if !success {
            return errorTxFail
        }
        return err
    },
    "showuser": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 2 {
            fmt.Println("showuser - shows the tags granted to a user or users")
            fmt.Println("Usage: showuser username1 [username2] [username3] ...")
            return nil
        }
        for _, username := range tokens[1:] {
            acc, err := accounts.RetrieveAccount(ctx, etcdClient, username)
            if err != nil {
                return err
            }
            tagSlice := setToSlice(acc.Tags)
            fmt.Printf("%s: %s\n", username, strings.Join(tagSlice, " "))
        }
        return nil
    },
    "lsusers": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 && len(tokens) != 2 {
            fmt.Println("lsusers - shows the tags granted to all user accounts whose names begin with a given prefix")
            fmt.Println("Usage: lsusers [prefix]")
            return nil
        }

        prefix := ""
        if len(tokens) == 2 {
            prefix = tokens[1]
        }

        accs, err := accounts.RetrieveMultipleAccounts(ctx, etcdClient, prefix)
        if err != nil {
            return err
        }

        for _, acc := range accs {
            if acc.Tags == nil {
                fmt.Printf("%s [CORRUPT ENTRY]\n", acc.Username)
            } else {
                tagSlice := setToSlice(acc.Tags)
                fmt.Printf("%s: %s\n", acc.Username, strings.Join(tagSlice, " "))
            }
        }
        return nil
    },
    "deftag": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("deftag - defines a new tag, which is a unit of permissions that can be granted to a user")
            fmt.Println("Usage: deftag tag pathprefix1 [pathprefix2] ...")
            return nil
        }
        if tokens[1] == accounts.ALL_TAG {
            return errorAlreadyExists
        }
        pfxSet := sliceToSet(tokens[2:])
        tagdef := &accounts.MrPlotterTagDef{Tag: tokens[1], PathPrefix: pfxSet}
        success, err := accounts.UpsertTagDefAtomically(ctx, etcdClient, tagdef)
        if !success {
            return errorAlreadyExists
        }
        return err
    },
    "undeftag": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 2 {
            fmt.Println("undeftag - deletes tag definitions")
            fmt.Println("Usage: undeftag username")
            return nil
        }
        for _, tagname := range tokens[1:] {
            if tagname == accounts.ALL_TAG {
                fmt.Printf("Tag \"%s\" cannot be deleted\n", accounts.ALL_TAG)
            }
            err := accounts.DeleteTagDef(ctx, etcdClient, tagname)
            if err != nil {
                return err
            }
        }
        return nil
    },
    "undeftags": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 2 {
            fmt.Println("undeftags - deletes all tag definitions where the tag name begins with a certain prefix")
            fmt.Println("Usage: undeftags prefix")
            return nil
        }
        n, err := accounts.DeleteMultipleTagDefs(ctx, etcdClient, tokens[1])
        if n == 1 {
            fmt.Println("Deleted 1 tag definition")
        } else {
            fmt.Printf("Deleted %v tag definitions\n", n)
        }
        return err
    },
    "addprefix": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("addprefix - adds a path prefix to a tag definition")
            fmt.Println("Usage: addprefix tag prefix1 [prefix2] [prefix3] ...")
            return nil
        }
        if tokens[1] == accounts.ALL_TAG {
            return fmt.Errorf("Cannot modify definition of \"%s\" tag\n", accounts.ALL_TAG)
        }
        tagdef, err := accounts.RetrieveTagDef(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        for _, pfx := range tokens[2:] {
            if _, ok := tagdef.PathPrefix[pfx]; !ok {
                tagdef.PathPrefix[pfx] = struct{}{}
            }
        }
        success, err := accounts.UpsertTagDefAtomically(ctx, etcdClient, tagdef)
        if !success {
            return errorTxFail
        }
        return err
    },
    "rmprefix": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("rmprefix - removes a path prefix from a tag definition")
            fmt.Println("Usage: rmprefix tag prefix1 [prefix2] [prefix3] ...")
            return nil
        }
        if tokens[1] == accounts.ALL_TAG {
            return fmt.Errorf("Cannot modify definition of \"%s\" tag\n", accounts.ALL_TAG)
        }
        tagdef, err := accounts.RetrieveTagDef(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        for _, pfx := range tokens[2:] {
            if _, ok := tagdef.PathPrefix[pfx]; ok {
                if len(tagdef.PathPrefix) == 1 {
                    return errors.New("Each tag must be assigned at least one prefix (use undeftag or undeftags to fully remove a tag)")
                }
                delete(tagdef.PathPrefix, pfx)
            }
        }
        success, err := accounts.UpsertTagDefAtomically(ctx, etcdClient, tagdef)
        if !success {
            return errorTxFail
        }
        return err
    },
    "showtagdef": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 2 {
            fmt.Println("showtagdef - lists the prefixes assigned to a tag")
            fmt.Println("Usage: showtagdef tag1 [tag2] [tag3] ...")
            return nil
        }
        for _, tagname := range tokens[1:] {
            if tokens[1] == accounts.ALL_TAG {
                fmt.Printf("%s: %s\n", accounts.ALL_TAG, ALL_TAG_SYMBOL)
                return nil
            }
            tagdef, err := accounts.RetrieveTagDef(ctx, etcdClient, tagname)
            if err != nil {
                return err
            }
            pfxSlice := setToSlice(tagdef.PathPrefix)
            fmt.Printf("%s: %s\n", tagname, strings.Join(pfxSlice, " "))
        }
        return nil
    },
    "lstagdefs": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 && len(tokens) != 2 {
            fmt.Println("lstagdefs - lists the prefixes assigned to all tags whose names begin with a certain prefix")
            fmt.Println("Usage: lstagdefs [prefix]")
            return nil
        }

        prefix := ""
        if len(tokens) == 2 {
            prefix = tokens[1]
        }

        tagdefs, err := accounts.RetrieveMultipleTagDefs(ctx, etcdClient, prefix)
        if err != nil {
            return err
        }

        if strings.HasPrefix(accounts.ALL_TAG, prefix) {
            fmt.Printf("%s: %s\n", accounts.ALL_TAG, ALL_TAG_SYMBOL)
        }
        for _, tagdef := range tagdefs {
            if tagdef.Tag == accounts.ALL_TAG {
                continue
            }
            if tagdef.PathPrefix == nil {
                fmt.Printf("%s [CORRUPT ENTRY]\n", tagdef.Tag)
            } else {
                pfxSlice := setToSlice(tagdef.PathPrefix)
                fmt.Printf("%s: %s\n", tagdef.Tag, strings.Join(pfxSlice, " "))
            }
        }
        return nil
    },
    "ls": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 && len(tokens) != 2 {
            fmt.Println("ls - lists the path prefixes viewable by each user in the current configuration")
            fmt.Println("Usage: ls [prefix]")
            return nil
        }

        prefix := ""
        if len(tokens) == 2 {
            prefix = tokens[1]
        }

        accs, err := accounts.RetrieveMultipleAccounts(ctx, etcdClient, prefix)
        if err != nil {
            return err
        }

        for _, acc := range accs {
            if acc.Tags == nil {
                fmt.Printf("%s [CORRUPT ENTRY]\n", acc.Username)
            } else {
                tagcache := make(map[string]map[string]struct{})

                prefixes := make(map[string]struct{})
                for tag := range acc.Tags {
                    var tagPfxSet map[string]struct{}
                    var ok bool

                    /* The ALL tag overrides everything. */
                    if tag == accounts.ALL_TAG {
                        prefixes = make(map[string]struct{})
                        prefixes[ALL_TAG_SYMBOL] = struct{}{}
                        break
                    }

                    if tagPfxSet, ok = tagcache[tag]; !ok {
                        tagdef, err := accounts.RetrieveTagDef(ctx, etcdClient, tag)
                        if err != nil {
                            return fmt.Errorf("Could not retrieve tag information for '%s': %v\n", tag, err)
                        }
                        tagPfxSet = tagdef.PathPrefix
                    }
                    for pfx := range tagPfxSet {
                        prefixes[pfx] = struct{}{}
                    }
                }
                fmt.Printf("%s: %s\n", acc.Username, strings.Join(setToSlice(prefixes), " "))
            }
        }
        return nil
    },
    "close": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 {
            fmt.Println("Usage: close")
            return nil
        }
        os.Exit(0)
        return nil
    },
    "exit": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 {
            fmt.Println("Usage: exit")
            return nil
        }
        os.Exit(0)
        return nil
    },
}

func help() {
    commands := make([]string, 0, len(ops))
    for op := range ops {
        commands = append(commands, op)
    }
    fmt.Println("Type one of the following commands and press <Enter> or <Return> to execute it:")
    fmt.Println(strings.Join(commands, " "))
}

func accounts_exec(etcdClient *etcd.Client, cmd string) {
    tokens := strings.Fields(cmd)
    if len(tokens) == 0 {
        return
    }

    opcode := tokens[0]

    if opcode == "help" {
        help()
        return
    }

    if op, ok := ops[opcode]; ok {
        err := op(context.Background(), etcdClient, tokens)
        if err != nil {
            fmt.Printf("Operation failed: %v\n", err)
        }
    } else {
        fmt.Printf("'%s' is not a valid command\n", opcode)
        help()
    }
}
