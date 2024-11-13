package tests

import (
	"fmt"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/store"
	"testing"
	"time"
)

func TestDBBenchMark(t *testing.T) {
	wal := WAL.NewFakeWAL()

	db := Tablet.NewBucket(store.NewSwissTableStore(), wal)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "empty.json",
		NoFlush:     true,
		WKeySet:     32,
	})
	N := 100_0000
	m := datasetN(N)

	start := time.Now()
	for k, v := range m {
		_ = db.Set(k, v)
	}

	elapsed := time.Since(start)
	t.Log("WR Time:", elapsed)
	rps := float64(N) / elapsed.Seconds()
	t.Log("WR Perf:", twoDigit(rps), "RPS")

	start = time.Now()
	for k, _ := range m {
		db.Get(k)
	}
	elapsed = time.Since(start)
	t.Log("RD Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("RD Perf:", twoDigit(rps), "RPS")
}

func twoDigit(f float64) string {
	return fmt.Sprintf("%.2f", f)
}
