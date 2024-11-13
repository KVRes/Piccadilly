package KV

import (
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/utils"
	"log"
	"os"
	"time"

	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/store"
	"github.com/KVRes/Piccadilly/watcher"
)

type BucketConfig struct {
	WALPath       string
	PersistPath   string
	FlushInterval time.Duration
	WBuffer       int
	WKeySet       int
	NoFlush       bool
}

func (b *BucketConfig) Normalise() {
	if b.WBuffer <= 0 {
		b.WBuffer = 100
	}

	if b.WKeySet <= 0 {
		b.WKeySet = 100
	}

	if b.FlushInterval <= 0 {
		b.FlushInterval = 5 * time.Second
	}
}

func toReq(kvp types.KVPair, t types.EventType) internalReq {
	return internalReq{
		KVPair: kvp,
		done:   make(chan error),
		t:      t,
	}
}

type internalReq struct {
	t types.EventType
	types.KVPair
	done chan error
}

func (wr *internalReq) Close() {
	close(wr.done)
}

type Bucket struct {
	store    store.Provider
	wal      WAL.Provider
	cfg      BucketConfig
	wChannel chan internalReq
	Watcher  *watcher.KeyWatcher
}

func NewBucket(store store.Provider, wal WAL.Provider) *Bucket {
	return &Bucket{
		store:   store,
		wal:     wal,
		Watcher: watcher.NewKeyWatcher(),
	}
}

func (b *Bucket) apply(rec WAL.Record) error {
	switch rec.StateOper {
	case WAL.StateOperSet:
		return b.store.Set(rec.Key, rec.Value)
	case WAL.StateOperDel:
		return b.store.Del(rec.Key)
	}
	return nil
}

func (b *Bucket) StartService(cfg BucketConfig) error {
	// recover from crash
	cfg.Normalise()
	b.cfg = cfg

	walBytes, err := os.ReadFile(cfg.WALPath)
	if err == nil {
		err = b.wal.Load(walBytes)
	}
	if err != nil {
		return err
	}

	// create persist store
	_ = utils.CreateFileIfNotExists(cfg.PersistPath)
	persistBytes, err := os.ReadFile(cfg.PersistPath)
	if err != nil {
		return err
	}
	persistBytes = utils.TrimBytes(persistBytes)
	if !utils.IsEmptyBytes(persistBytes) {
		err = b.store.Load(persistBytes)
		if err != nil {
			return err
		}
	}

	if err := b.RecoverFromWAL(); err != nil {
		return err
	}

	// Give daemon a lock!
	go b.daemon()
	go b.writeChannel()
	b.wChannel = make(chan internalReq, cfg.WBuffer)
	return nil
}

func (b *Bucket) appendToWChannel(req internalReq) {
	b.wChannel <- req
}

func (b *Bucket) daemon() {
	for {
		time.Sleep(b.cfg.FlushInterval)
		if !b.cfg.NoFlush {
			err := b.Flush()
			if err != nil {
				log.Println("Flush failed:", err)
				continue
			}
		}

		b.wal.Truncate()
		/*if bytes, err := b.wal.Serialize(); err != nil {
			log.Println("Serialize commitlog failed:", err)
			continue
		} else {
			os.WriteFile(b.cfg.WALPath, bytes, 0644)
		}*/
	}
}

func (b *Bucket) Flush() error {
	_, err := b.wal.Append(WAL.NewStateOperRecord(WAL.StateOperCheckpoint))
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

	_, err = b.wal.Append(WAL.NewStateOperRecord(WAL.StateOperCheckpointOk))
	return err
}

func (b *Bucket) RecoverFromRecords(recs []WAL.Record) error {
	for _, rec := range recs {
		if err := b.apply(rec); err != nil {
			return err
		}
	}
	return nil
}
func (b *Bucket) RecoverFromWAL() error {
	recs, err := b.wal.RecordsSinceLastChkptr()
	if err != nil {
		return err
	}

	return b.RecoverFromRecords(recs)
}
