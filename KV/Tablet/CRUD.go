package Tablet

import (
	"github.com/KVRes/Piccadilly/KV/Store"
	"github.com/KVRes/Piccadilly/KV/WAL"
	"github.com/KVRes/Piccadilly/types"
)

func (b *Bucket) _modifyOper(eventType types.EventType, key string, value types.Value) error {
	b.countId.Add(1)

	rec := WAL.NewStateOperFromEventTypes(eventType).WithKeyValue(key, value)
	if _, err := b.wal.Append(rec); err != nil {
		return err
	}
	req := toReq(types.KVPairV{Key: key, Value: value}, eventType)
	defer req.Close()
	b.sendToWQueue(req)
	return <-req.done
}

func (b *Bucket) sendToWQueue(req internalReq) {
	switch b.cfg.WModel {
	case types.NoLinear:
		go b.noLinearChannel(req)
	default:
		b.wChannel <- req
	}
}

func (b *Bucket) Set(key string, value types.Value) error {
	return b._modifyOper(types.EventSet, key, value)
}

func (b *Bucket) Del(key string) error {
	return b._modifyOper(types.EventDelete, key, types.Value{})
}

func (b *Bucket) Clear() error {
	return b._modifyOper(types.EventClear, "", types.Value{})
}

func (b *Bucket) Get(key string) (types.Value, error) {
	val, err := b.store.Get(key)
	if err != nil {
		return types.Value{}, err
	}
	if val.IsExpired() {
		return types.Value{}, Store.ErrKeyNotFound
	}
	return val, nil
}

func (b *Bucket) Keys() ([]string, error) {
	return b.store.Keys()
}

func (b *Bucket) Len() int {
	return b.store.Len()
}
