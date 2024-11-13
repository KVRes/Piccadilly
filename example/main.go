package main

import (
	"github.com/KVRes/Piccadilly/KV"
	"log"

	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/store"
)

func main() {
	cmtLog, err := WAL.NewJsonWALProvider("commitlog.json")
	if err != nil {
		log.Println("NewJsonWALProvider failed:", err)
		return
	}

	db := KV.NewBucket(store.NewSwissTableStore(), cmtLog)

	db.StartService(KV.BucketConfig{
		WALPath:     "commitlog.json",
		PersistPath: "persist.json",
	})
}
