package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Yashas2004/kvraft/kv"
)

func main() {
	servers := flag.String("servers", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated server addresses")
	flag.Parse()

	ck := kv.NewNetClerk(strings.Split(*servers, ","))
	defer ck.Close()

	// One-shot mode: client.exe get mykey
	if flag.NArg() > 0 {
		run(ck, flag.Args())
		return
	}

	// Interactive mode
	fmt.Println("kvraft client. Commands: put <key> <value> | append <key> <value> | get <key> | delete <key> | quit")
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		fields := strings.Fields(scanner.Text())
		for len(fields) > 0 && fields[0] == ">" {
			fields = fields[1:] // ignore a pasted prompt
		}
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "quit" || fields[0] == "exit" {
			break
		}
		run(ck, fields)
	}
}

func run(ck *kv.NetClerk, args []string) {
	start := time.Now()
	command := args[0]

	switch command {
	case "put", "append":
		if len(args) < 3 {
			fmt.Printf("usage: %s <key> <value>\n", command)
			return
		}
		key := []byte(args[1])
		value := []byte(strings.Join(args[2:], " "))
		var err error
		if command == "put" {
			err = ck.Put(key, value)
		} else {
			err = ck.Append(key, value)
		}
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("OK (%v)\n", time.Since(start).Round(time.Microsecond))

	case "get":
		if len(args) != 2 {
			fmt.Println("usage: get <key>")
			return
		}
		value, found, err := ck.Get([]byte(args[1]))
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		if !found {
			fmt.Printf("(not found) (%v)\n", time.Since(start).Round(time.Microsecond))
			return
		}
		fmt.Printf("%s (%v)\n", value, time.Since(start).Round(time.Microsecond))

	case "delete":
		if len(args) != 2 {
			fmt.Println("usage: delete <key>")
			return
		}
		if err := ck.Delete([]byte(args[1])); err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Printf("OK (%v)\n", time.Since(start).Round(time.Microsecond))

	default:
		fmt.Println("unknown command:", command)
	}
}
