package kv

import (
	"log"
	"os"
	"time"

	cml "github.com/KVRes/Piccadilly/commitlog"
	"github.com/KVRes/Piccadilly/store"
)

type BucketConfig struct {
	CommitLogPath string
	PersistPath   string
	FlushInterval time.Duration
}

type Bucket struct {
	store  store.Store
	cmtLog cml.CommitLog
	cfg    BucketConfig
}

func (b *Bucket) apply(rec cml.Record) error {
	switch rec.StateOper {
	case cml.StateOperSet:
		return b.store.Set(rec.Key, rec.Value)
	}
	return nil
}

func (b *Bucket) StartService(cfg BucketConfig) error {
	// recover from crash
	b.cfg = cfg

	cmlBytes, err := os.ReadFile(cfg.CommitLogPath)
	if err != nil {
		return err
	}

	if err = b.cmtLog.Load(cmlBytes); err != nil {
		return err
	}

	// create persist store
	persistBytes, err := os.ReadFile(cfg.PersistPath)
	if err != nil {
		return err
	}
	if err = b.store.Load(persistBytes); err != nil {
		return err
	}

	if err := b.recoverFromCommitLog(); err != nil {
		return err
	}

	// Give daemon a lock!
	go b.daemon()
	return nil
}

func (b *Bucket) daemon() {
	for {
		time.Sleep(b.cfg.FlushInterval)
		err := b.Flush()
		if err != nil {
			log.Println("Flush failed:", err)
			continue
		}
		b.cmtLog.Truncate()
	}
}

func (b *Bucket) Flush() error {
	_, err := b.cmtLog.Append(cml.NewStateOperRecord(cml.StateOperCheckpoint))
	if err != nil {
		return err
	}

	bytes, err := b.store.SerializeAll()
	if err != nil {
		return err
	}
	err = os.WriteFile(b.cfg.PersistPath, bytes, 0644)
	if err != nil {
		return err
	}

	_, err = b.cmtLog.Append(cml.NewStateOperRecord(cml.StateOperCheckpointOk))
	return err
}

func (b *Bucket) recoverFromCommitLog() error {
	recs, err := b.cmtLog.RecordsSinceLastChkptr()
	if err != nil {
		return err
	}
	for _, rec := range recs {
		if err := b.apply(rec); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bucket) Set(key, value string) error {
	rec := cml.Record{
		StateOper: cml.StateOperSet,
		Key:       key,
		Value:     value,
	}
	_, err := b.cmtLog.Append(rec)
	if err != nil {
		return err
	}
	return b.store.Set(key, value)
}

func (b *Bucket) Get(key string) (string, error) {
	return b.store.Get(key)
}
