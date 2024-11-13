package tests

import (
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/serv"
	"testing"
)

func TestServ(t *testing.T) {
	m := dataset()
	db := serv.NewServer("./data")
	go db.ServeTCP("127.0.0.1:12306")

	cli, err := client.NewClient("127.0.0.1:12306")
	if err != nil {
		t.Fatal(err)
	}
	err = cli.Connect("/kevin/zonda", KV.CreateIfNotExist, KV.Linear)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range m {
		err = cli.Set(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range m {
		val, err := cli.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != v {
			t.Fatal("expected", v, "got", val)
		}
	}

}
