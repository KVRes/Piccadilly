package tests

import (
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/serv"
	"github.com/KVRes/Piccadilly/types"
	"os"
	"testing"
	"time"
)

func TestWatcher(t *testing.T) {
	os.RemoveAll("./data/kevin/zonda")
	db := serv.NewServer("./data")
	go db.ServeTCP("127.0.0.1:12306")

	time.Sleep(1 * time.Second)
	cli, _ := client.NewClient("127.0.0.1:12306")
	cli.Connect("/kevin/zonda", types.CreateIfNotExist, types.NoLinear)

	sub, err := cli.Watch("lock", types.EventAll)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	cli.Set("lock", "1")
	cli.Set("lock", "1")
	cli.Set("lock", "1")

	var noties []client.ErrorableEvent
	for i := 0; i < 1; i++ {
		noties = append(noties, <-sub.Ch)
	}

	for i, v := range noties {
		t.Log(i, v)
	}

}
