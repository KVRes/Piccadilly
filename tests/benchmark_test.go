package tests

import (
	"fmt"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/serv"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KevinZonda/GoX/pkg/panicx"
	"testing"
	"time"
)

func TestBktBenchmark(t *testing.T) {
	wal := WAL.NewFakeWALProvider()

	db := Tablet.NewBucket(Store.NewSwissTableStore(), wal)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "empty.json",
		NoFlush:     true,
		WModel:      types.NoLinear,
	})
	N := 100_0000
	b := &Benchmark{Data: datasetN(N)}
	bl := b.Baseline()
	t.Log("Baseline:", bl)

	elapsed := b.B(func(k, v string) {
		_ = db.Set(k, types.Value{Data: v})
	})
	t.Log("WR Time:", elapsed)
	rps := float64(N) / elapsed.Seconds()
	t.Log("WR Perf:", twoDigit(rps), "RPS")

	elapsed = b.Batch(10000, func(k, v string) {
		_, _ = db.Get(k)
	})
	t.Log("RD Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("RD Perf:", twoDigit(rps), "RPS")
}

func TestBktPprof(t *testing.T) {
	wal := WAL.NewFakeWALProvider()

	db := Tablet.NewBucket(Store.NewSwissTableStore(), wal)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "empty.json",
		NoFlush:     true,
		WModel:      types.NoLinear,
		WBuffer:     10000,
	})
	N := 100_0000
	b := &Benchmark{Data: datasetN(N)}

	b.Pprof("bkt_w", func() {
		b.B(func(k, v string) {
			_ = db.Set(k, types.Value{Data: v})
		})
	})
}

func TestGRPCBenchmark(t *testing.T) {
	db := KV.NewDatabase("./data")
	db.Template.WALType = WAL.FakeWAL
	db.Template.NoFlush = true
	sv := serv.NewServerWithDb(db)
	go sv.ServeTCP("127.0.0.1:12306")

	N := 100_0000
	b := &Benchmark{Data: datasetN(N)}
	bl := b.Baseline()
	pool, err := client.NewPool(10, "127.0.0.1:12306")
	panicx.NotNilErr(err)
	err = pool.Connect("/kevin/zonda", types.CreateIfNotExist, types.NoLinear)
	panicx.NotNilErr(err)
	defer pool.Close()

	var elapsed time.Duration
	var rps float64

	elapsed = b.Batch(1000, func(k, v string) {
		_ = pool.Client().Set(k, v)
	}) - bl
	t.Log("WR Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("WR Perf:", twoDigit(rps), "RPS")

	elapsed = b.B(func(k, v string) {
		_, _ = pool.Client().Get(k)
	}) - bl
	t.Log("RD Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("RD Perf:", twoDigit(rps), "RPS")
}

func twoDigit(f float64) string {
	return fmt.Sprintf("%.2f", f)
}
