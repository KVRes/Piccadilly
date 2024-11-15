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
		switch elems[0] {
		case "cd":
			cmdErr = onCommand(elems, 2, cdCmd)
		case "get":
			cmdErr = onCommand(elems, 2, getCmd)
		case "set":
			cmdErr = onCommand(elems, 3, setCmd)
		case "del":
			cmdErr = onCommand(elems, 2, delCmd)

		case "exit":
			return
		}
		if cmdErr != nil {
			fmt.Println(cmdErr)
		}
	}
}

func cdCmd(elems []string) error {
	path := elems[1]
	err := cli.Connect(path, types.ErrorIfNotExist, types.NoLinear)
	if err != nil {
		return err
	}
	dbPath = cli.GetCurrentPath()
	rl.SetPrompt(prompt())
	return nil
}

func getCmd(elems []string) error {
	key := elems[1]
	val, err := cli.Get(key)
	if err != nil {
		return err
	}
	fmt.Println(val)
	return nil
}

func setCmd(elems []string) error {
	key := elems[1]
	val := elems[2]
	err := cli.Set(key, val)
	if err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

func delCmd(elems []string) error {
	key := elems[1]
	err := cli.Del(key)
	if err != nil {
		return err
	}
	return nil
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
