package main

import (
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/store"
	"log"
)

func main() {
	cmtLog, err := WAL.NewJsonWALProvider("commitlog.json")
	if err != nil {
		log.Println("NewJsonWALProvider failed:", err)
		return
	}

	db := Tablet.NewBucket(store.NewSwissTableStore(), cmtLog)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "commitlog.json",
		PersistPath: "persist.json",
	})
}
