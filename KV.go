package piccadilly

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
	WBuffer       int
	WKeySet       int
}

type KVPair struct {
	key   string
	value string
}

func (kvp KVPair) ToWRequest() wRequest {
	return wRequest{
		KVPair: kvp,
		done:   make(chan error),
	}
}

type wRequest struct {
	KVPair
	done chan error
}

type Bucket struct {
	store    store.Store
	cmtLog   cml.CommitLog
	cfg      BucketConfig
	wChannel chan wRequest
	Watcher  *KeyWatcher
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
	b.wChannel = make(chan wRequest, cfg.WBuffer)
	return nil
}

type keyBuf struct {
	keys []string
}

func (k *keyBuf) init(size int) {
	k.keys = make([]string, size)
}

func (k *keyBuf) findEmpty(key string) int {
	empty := -1
	for i, k := range k.keys {
		if k == "" {
			empty = i
		}
		if k == key {
			return -1
		}
	}
	return empty
}

func (b *Bucket) WriteChannel() {
	keyBuf := &keyBuf{}
	keyBuf.init(b.cfg.WKeySet)
	for {
		kv := <-b.wChannel
		empty := keyBuf.findEmpty(kv.key)
		if empty == -1 {
			// buffer is full, wait for a slot
			go b.appendToWChannel(kv)
			continue
		}
		go func(kvp wRequest, idx int) {
			kvp.done <- b.Set(kv.key, kv.value)
			keyBuf.keys[idx] = ""
		}(kv, empty)
		go b.Watcher.EmitEvent(kv.key, EventSet)

	}

}

func (b *Bucket) appendToWChannel(req wRequest) {
	b.wChannel <- req
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
	rec := cml.NewStateOperRecord(cml.StateOperSet).WithKeyValue(key, value)
	if _, err := b.cmtLog.Append(rec); err != nil {
		return err
	}
	req := KVPair{key, value}.ToWRequest()
	b.appendToWChannel(req)
	return <-req.done
}

func (b *Bucket) Get(key string) (string, error) {
	return b.store.Get(key)
}
