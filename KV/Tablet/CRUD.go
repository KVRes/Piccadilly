package Tablet

import (
	"github.com/KVRes/Piccadilly/WAL"
	"github.com/KVRes/Piccadilly/types"
)

func (b *Bucket) _modifyOper(eventType types.EventType, key string, value string) error {
	rec := WAL.NewStateOperFromEventTypes(eventType).WithKeyValue(key, value)
	if _, err := b.wal.Append(rec); err != nil {
		return err
	}
	req := toReq(types.KVPair{Key: key}, eventType)
	defer req.Close()
	b.appendToWChannel(req)
	return <-req.done
}

func (b *Bucket) Set(key, value string) error {
	return b._modifyOper(types.EventSet, key, value)
}

func (b *Bucket) Del(key string) error {
	return b._modifyOper(types.EventDelete, key, "")
}

func (b *Bucket) Get(key string) (string, error) {
	return b.store.Get(key)
}
