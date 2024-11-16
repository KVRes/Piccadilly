package main

import (
	"fmt"
	"github.com/KVRes/Piccadilly/types"
)

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

func keysCmd(elems []string) error {
	keys, err := cli.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		fmt.Println(key)
	}
	return nil
}

func lsCmd(elems []string) error {
	ns, err := cli.ListPNodes()
	if err != nil {
		return err
	}
	for _, n := range ns {
		fmt.Println(n)
	}
	return nil
}

func createCmd(elems []string) error {
	ns := elems[1]
	err := cli.CreatePNode(ns)
	return err
}
