package main

import (
	"log"

	"github.com/KVRes/Piccadilly"
	"github.com/KVRes/Piccadilly/commitlog"
	"github.com/KVRes/Piccadilly/store"
)

func main() {
	cmtLog, err := commitlog.NewJsonFileProvider("commitlog.json")
	if err != nil {
		log.Println("NewJsonFileProvider failed:", err)
		return
	}

	db := Piccadilly.NewBucket(store.NewSwissTableStore(), cmtLog)

	db.StartService(Piccadilly.BucketConfig{
		CommitLogPath: "commitlog.json",
		PersistPath:   "persist.json",
	})
}
