package Tablet

import (
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/KV/Watcher"
	"github.com/KVRes/Piccadilly/KV/store"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/utils"
	"github.com/KevinZonda/GoX/pkg/iox"
	"log"
	"os"
	"sync"
	"time"
)

type BucketConfig struct {
	WALPath       string
	PersistPath   string
	FlushInterval time.Duration
	LongInterval  time.Duration
	WBuffer       int
	NoFlush       bool
	WModel        types.ConcurrentModel
}

func (b *BucketConfig) Normalise() {
	if b.WBuffer <= 0 {
		b.WBuffer = 128
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
	Watcher  *Watcher.KeyWatcher
	wCount   sync.WaitGroup
}

func NewBucket(store store.Provider, wal WAL.Provider) *Bucket {
	return &Bucket{
		store:   store,
		wal:     wal,
		Watcher: Watcher.NewKeyWatcher(),
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

	b.wChannel = make(chan internalReq, cfg.WBuffer)
	// Give daemon a lock!
	go b.flushThread()
	go b.longDaemonThread()
	go b.writeChannel()
	return nil
}

func (b *Bucket) appendToWChannel(req internalReq) {
	b.wChannel <- req
}

func (b *Bucket) longDaemonThread() {
	if b.cfg.LongInterval <= 0 {
		return
	}
	for {
		time.Sleep(b.cfg.LongInterval)
		b.Watcher.GC()
		b.wal.Truncate()
	}
}

func (b *Bucket) flushThread() {
	if b.cfg.NoFlush || b.cfg.FlushInterval <= 0 {
		return
	}
	for {
		time.Sleep(b.cfg.FlushInterval)

		err := b.Flush()
		log.Printf("[Bkt %p] flushed, err: %v\n", b, err)
		if err != nil {
			log.Println("Flush failed:", err)
			continue

		}
	}
}

func (b *Bucket) Flush() error {
	_, err := b.wal.Append(WAL.NewStateOperRecord(WAL.StateOperCheckpoint))
	if err != nil {
		return err
	}
	b.waitWithTimeout(5 * time.Second)

	bytes, err := b.store.SerializeAll()
	if err != nil {
		return err
	}
	err = iox.WriteAllBytes(b.cfg.PersistPath, bytes)
	if err != nil {
		return err
	}

	_, err = b.wal.Append(WAL.NewStateOperRecord(WAL.StateOperCheckpointOk))
	return err
}

func (b *Bucket) waitWithTimeout(timeout time.Duration) {
	ch := make(chan struct{})
	go func() {
		b.wCount.Wait()
		close(ch)
	}()
	select {
	case <-ch:
	case <-time.After(timeout):
	}
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
