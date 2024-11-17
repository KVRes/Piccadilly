package main

import (
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"log"
)

func main() {
	cmtLog, err := WAL.NewJsonWALProvider("commitlog.json")
	if err != nil {
		log.Println("NewJsonWALProvider failed:", err)
		return
	}

	db := Tablet.NewBucket(Store.NewSwissTableStore(), cmtLog)

	db.StartService(Tablet.BucketConfig{
		WALPath:     "commitlog.json",
		PersistPath: "persist.json",
	})
}
