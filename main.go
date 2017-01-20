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

func tagSliceToSet(tagSlice []string) map[string]struct{} {
    tagSet := make(map[string]struct{})
    for _, tag := range tagSlice {
        tagSet[tag] = struct{}{}
    }
    return tagSet
}

func tagSetToSlice(tagSet map[string]struct{}) []string {
    tagSlice := make([]string, 0, len(tagSet))
    for tag := range tagSet {
        tagSlice = append(tagSlice, tag)
    }
    return tagSlice
}

var errorTxFail = errors.New("Transacation for atomic update failed; try again")

var ops = map[string]func(ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
    "adduser": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("Usage: adduser username password [tag1] [tag2] ...")
            return nil
        }
        tagSet := tagSliceToSet(tokens[3:])
        tagSet["public"] = struct{}{}
        acc := &accounts.MrPlotterAccount{Username: tokens[1], Tags: tagSet}
        acc.SetPassword([]byte(tokens[2]))
        success, err := accounts.UpsertAccountAtomically(ctx, etcdClient, acc)
        if !success {
            return errorTxFail
        }
        return err
    },
    "setpassword": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 3 {
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
        if len(tokens) != 2 {
            fmt.Println("Usage: rmuser username")
            return nil
        }
        return accounts.DeleteAccount(ctx, etcdClient, tokens[1])
    },
    "rmusers": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 2 {
            fmt.Println("Usage: rmusers prefix")
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
    "addtags": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("Usage: addtags username tag1 [tag2] [tag3] ...")
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
    "rmtags": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) < 3 {
            fmt.Println("Usage: rmtags username tag1 [tag2] [tag3] ...")
            return nil
        }
        acc, err := accounts.RetrieveAccount(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        for _, tag := range tokens[2:] {
            if tag == "public" {
                continue
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
    "lstags": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 2 {
            fmt.Println("Usage: lstags username")
            return nil
        }
        acc, err := accounts.RetrieveAccount(ctx, etcdClient, tokens[1])
        if err != nil {
            return err
        }
        tagSlice := tagSetToSlice(acc.Tags)
        fmt.Println(strings.Join(tagSlice, " "))
        return nil
    },
    "lsusers": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 && len(tokens) != 2 {
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
                fmt.Println(acc.Username)
            }
        }
        return nil
    },
    "ls": func (ctx context.Context, etcdClient *etcd.Client, tokens []string) error {
        if len(tokens) != 1 && len(tokens) != 2 {
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
                tagSlice := tagSetToSlice(acc.Tags)
                fmt.Printf("%s: %s\n", acc.Username, strings.Join(tagSlice, " "))
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
