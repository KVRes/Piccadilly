package tests

import (
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"strconv"
	"testing"
)

func initDB() *Tablet.Bucket {
	wal, err := WAL.NewJsonWALProvider("WAL.json")
	if err != nil {
		panic(err)
	}

	db := Tablet.NewBucket(store.NewSwissTableStore(), wal)

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
		if val != v {
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
