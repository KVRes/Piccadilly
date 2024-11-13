package tests

import (
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/serv"
	"testing"
	"time"
)

func TestServ(t *testing.T) {
	N := 10_0000
	m := datasetN(N)
	db := serv.NewServer("./data")
	go db.ServeTCP("127.0.0.1:12306")

	cli, err := client.NewClient("127.0.0.1:12306")
	if err != nil {
		t.Fatal(err)
	}
	err = cli.Connect("/kevin/zonda", KV.CreateIfNotExist, KV.Buffer)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	for k, v := range m {
		err = cli.Set(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	t.Log("WR Time:", elapsed)
	rps := float64(N) / elapsed.Seconds()
	t.Log("WR Perf:", rps, "RPS")

	start = time.Now()
	for k, v := range m {
		val, err := cli.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != v {
			t.Fatal("expected", v, "got", val)
		}
	}
	elapsed = time.Since(start)
	t.Log("RD Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("RD Perf:", rps, "RPS")

}
