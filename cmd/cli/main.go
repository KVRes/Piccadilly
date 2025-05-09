package main

import (
	"fmt"
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/types"
	"github.com/chzyer/readline"
	"os"
	"strings"
)

var cli *client.Client
var addr = types.DEFAULT_ADDR
var dbPath = ""

var rl *readline.Instance

func prompt() string {
	return addr + dbPath + ">"
}

func lenAtLest[T any](arr []T, size int) error {
	if len(arr) >= size {
		return nil
	}
	return fmt.Errorf("need at least %d elements", size)
}

func main() {
	var err error
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	cli, err = client.NewClient(addr)
	if err != nil {
		panic(err)
	}
	rl, err = readline.New(prompt())
	if err != nil {
		panic(err)
	}

	OnConnected()
}

func OnConnected() {
	defer OnDisconnected()
	var cmdErr error
	for {
		line, err := rl.Readline()
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		elems := strings.Split(line, " ")
		elems[0] = strings.ToLower(elems[0])
		switch elems[0] {
		case "cd":
			cmdErr = onCommand(elems, 2, cdCmd)
		case "get":
			cmdErr = onCommand(elems, 2, getCmd)
		case "set":
			cmdErr = onCommand(elems, 3, setCmd)
		case "setex":
			cmdErr = onCommand(elems, 4, setexCmd)
		case "del":
			cmdErr = onCommand(elems, 2, delCmd)
		case "keys":
			cmdErr = onCommand(elems, -1, keysCmd)
		case "ls":
			cmdErr = onCommand(elems, -1, lsCmd)
		case "create":
			cmdErr = onCommand(elems, 2, createCmd)
		case "len":
			cmdErr = onCommand(elems, -1, lenCmd)
		case "clear":
			cmdErr = onCommand(elems, -1, clearCmd)
		case "exit":
			return
		default:
			cmdErr = fmt.Errorf("unknown command: %s", elems[0])
		}
		if cmdErr != nil {
			fmt.Println(cmdErr)
		}
	}
}

func onCommand(eles []string, atLeast int, f func([]string) error) error {
	if atLeast > 0 {
		if err := lenAtLest(eles, atLeast); err != nil {
			return err
		}
	}
	return f(eles)
}

func OnDisconnected() {
	fmt.Println("Disconnected")
}
