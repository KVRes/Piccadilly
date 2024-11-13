package KV

import (
	"fmt"
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

func ToWRequest(kvp types.KVPair) wRequest {
	return wRequest{
		KVPair: kvp,
		done:   make(chan error),
	}
}

type wRequest struct {
	types.KVPair
	done chan error
}

func (wr *wRequest) Close() {
	close(wr.done)
}

type Bucket struct {
	store    store.Store
	wal      WAL.Provider
	cfg      BucketConfig
	wChannel chan wRequest
	Watcher  *watcher.KeyWatcher
}

func NewBucket(store store.Store, cmtLog WAL.Provider) *Bucket {
	return &Bucket{
		store:   store,
		wal:     cmtLog,
		Watcher: watcher.NewKeyWatcher(),
	}
}

func (b *Bucket) apply(rec WAL.Record) error {
	switch rec.StateOper {
	case WAL.StateOperSet:
		return b.store.Set(rec.Key, rec.Value)
	}
	return nil
}

func (b *Bucket) StartService(cfg BucketConfig) error {
	// recover from crash
	b.cfg = cfg
	b.cfg.Normalise()

	cmlBytes, err := os.ReadFile(cfg.WALPath)
	if err != nil {
		return err
	}

	if err = b.wal.Load(cmlBytes); err != nil {
		return err
	}

	// create persist store
	if _, err = os.Stat(cfg.PersistPath); os.IsNotExist(err) {
		os.WriteFile(cfg.PersistPath, []byte{}, 0644)
	}
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

	if err := b.recoverFromWAL(); err != nil {
		return err
	}

	// Give daemon a lock!
	go b.daemon()
	go b.WriteChannel()
	b.wChannel = make(chan wRequest, cfg.WBuffer)
	return nil
}

func (b *Bucket) WriteChannel() {
	fmt.Println("WriteChannel started")
	keyBuf := newKeyBuf(b.cfg.WKeySet)
	for {
		kv := <-b.wChannel
		empty := keyBuf.findEmpty(kv.Key)
		if empty == -1 {
			// buffer is full, wait for a slot
			go b.appendToWChannel(kv)
			continue
		}
		fmt.Println("WriteChannel setting:", kv.Key)
		go func(kvp wRequest, idx int) {
			kvp.done <- b.store.Set(kv.Key, kv.Value)
			keyBuf.keys[idx] = ""
		}(kv, empty)
		go b.Watcher.EmitEvent(kv.Key, watcher.EventSet)
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

func (b *Bucket) recoverFromWAL() error {
	recs, err := b.wal.RecordsSinceLastChkptr()
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
	rec := WAL.NewStateOperRecord(WAL.StateOperSet).WithKeyValue(key, value)
	if _, err := b.wal.Append(rec); err != nil {
		return err
	}
	req := ToWRequest(types.KVPair{Key: key, Value: value})
	defer req.Close()
	b.appendToWChannel(req)
	return <-req.done
}

func (b *Bucket) Get(key string) (string, error) {
	return b.store.Get(key)
}
