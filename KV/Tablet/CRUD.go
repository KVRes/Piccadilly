package Tablet

import (
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
)

func (b *Bucket) _modifyOper(eventType types.EventType, key string, value types.Value) error {
	rec := WAL.NewStateOperFromEventTypes(eventType).WithKeyValue(key, value)
	if _, err := b.wal.Append(rec); err != nil {
		return err
	}
	req := toReq(types.KVPairV{Key: key, Value: value}, eventType)
	defer req.Close()
	b.appendToWChannel(req)
	return <-req.done
}

func (b *Bucket) Set(key string, value types.Value) error {
	return b._modifyOper(types.EventSet, key, value)
}

func (b *Bucket) Del(key string) error {
	return b._modifyOper(types.EventDelete, key, types.Value{})
}

func (b *Bucket) Get(key string) (types.Value, error) {
	return b.store.Get(key)
}

func (b *Bucket) Keys() ([]string, error) {
	return b.store.Keys()
}
