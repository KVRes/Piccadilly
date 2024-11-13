package tests

import (
	"fmt"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/store"
	"strconv"
	"testing"
)

func initDB() *KV.Bucket {
	wal, err := WAL.NewJsonWALProvider("WAL.json")
	if err != nil {
		panic(err)
	}

	db := KV.NewBucket(store.NewSwissTableStore(), wal)

	err = db.StartService(KV.BucketConfig{
		WALPath:     "WAL.json",
		PersistPath: "persist.json",
	})

	if err != nil {
		panic(err)
	}
	return db
}

func TestDB(t *testing.T) {
	db := initDB()

	m := make(map[string]string)
	for i := 0; i < 1000; i++ {
		m["key"+strconv.Itoa(i)] = "value" + strconv.Itoa(i+1)
	}
	for k, v := range m {
		fmt.Println("Setting:", k, "->", v)
		err := db.Set(k, v)
		if err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range m {
		val, err := db.Get(k)
		if err != nil {
			t.Fatal(err)
		}
		if val != v {
			t.Fatal("expected", v, "got", val)
		}
	}

}
