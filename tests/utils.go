package tests

import (
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KevinZonda/GoX/pkg/panicx"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"
)

func initDB() *Tablet.Bucket {
	wal, err := WAL.NewJsonWALProvider("WAL.json")
	if err != nil {
		panic(err)
	}

	db := Tablet.NewBucket(Store.NewSwissTableStore(), wal)

	err = db.StartService(Tablet.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "persist.json",
	})

	if err != nil {
		panic(err)
	}
	return db
}

func dataset() map[string]string {
	return datasetN(1000)
}

func datasetN(n int) map[string]string {
	m := make(map[string]string)
	for i := 0; i < n; i++ {
		m["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i+1)
	}
	return m
}

func inDb(t *testing.T, bucket *Tablet.Bucket, m map[string]string) {
	for k, v := range m {
		val, err := bucket.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if val.Data != v {
			t.Fatal("expected", v, "got", val)
		}
	}
}

func notInDb(t *testing.T, bucket *Tablet.Bucket, m map[string]string) {
	for k, _ := range m {
		val, err := bucket.Get(k)
		if err == nil {
			t.Fatal("Need Not Found, but got", k, "->", val)
		}
	}
}

type Benchmark struct {
	Data map[string]string
}

func (b *Benchmark) Baseline() time.Duration {
	return b.B(func(k, v string) {})
}

func (b *Benchmark) B(f func(string, string)) time.Duration {
	wg := sync.WaitGroup{}
	wg.Add(len(b.Data))

	start := time.Now()
	for k, v := range b.Data {
		go func(k, v string) {
			f(k, v)
			wg.Done()
		}(k, v)
	}
	wg.Wait()

	return time.Since(start)
}

func (b *Benchmark) Batch(batch int, f func(string, string)) time.Duration {
	wg := sync.WaitGroup{}

	start := time.Now()
	cur := 0
	for k, v := range b.Data {
		cur++
		wg.Add(1)
		go func(k, v string, wg *sync.WaitGroup) {
			f(k, v)
			wg.Done()
		}(k, v, &wg)

		if cur%batch == 0 {
			wg.Wait()
			wg = sync.WaitGroup{}
		}
	}
	wg.Wait()

	return time.Since(start)
}

func (b *Benchmark) BSync(f func(string, string)) time.Duration {
	start := time.Now()
	for k, v := range b.Data {
		f(k, v)
	}

	return time.Since(start)
}

func (b *Benchmark) Pprof(filename string, f func()) {
	cpu, err := os.Create(filename + "_cpu.pprof")
	panicx.NotNilErr(err)

	mem, err := os.Create(filename + "_mem.pprof")
	panicx.NotNilErr(err)
	defer func() {
		pprof.StopCPUProfile()
		pprof.WriteHeapProfile(mem)
		mem.Close()
		cpu.Close()
	}()
	pprof.StartCPUProfile(cpu)

	f()

}

func Pprof[T any](filename string, f func() T) T {
	cpu, err := os.Create(filename + "_cpu.pprof")
	panicx.NotNilErr(err)

	mem, err := os.Create(filename + "_mem.pprof")
	panicx.NotNilErr(err)
	defer func() {
		pprof.StopCPUProfile()
		pprof.WriteHeapProfile(mem)
		mem.Close()
		cpu.Close()
	}()
	pprof.StartCPUProfile(cpu)

	return f()

}
