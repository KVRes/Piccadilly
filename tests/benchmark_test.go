package tests

import (
	"fmt"
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
	"testing"
)

func TestDBBenchMark(t *testing.T) {
	wal := WAL.NewFakeWALProvider()

	db := Tablet.NewBucket(Store.NewSwissTableStore(), wal)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "empty.json",
		NoFlush:     true,
		WModel:      types.NoLinear,
	})
	N := 500_0000
	b := &Benchmark{Data: datasetN(N)}
	bl := b.Baseline()

	elapsed := b.BSync(func(k, v string) {
		_ = db.Set(k, types.Value{Data: v})
	}) - bl
	t.Log("WR Time:", elapsed)
	rps := float64(N) / elapsed.Seconds()
	t.Log("WR Perf:", twoDigit(rps), "RPS")

	elapsed = b.B(func(k, v string) {
		_, _ = db.Get(k)
	}) - bl
	t.Log("RD Time:", elapsed)
	rps = float64(N) / elapsed.Seconds()
	t.Log("RD Perf:", twoDigit(rps), "RPS")
}

func twoDigit(f float64) string {
	return fmt.Sprintf("%.2f", f)
}
