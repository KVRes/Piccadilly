package Tablet

import (
	"github.com/KVRes/Piccadilly/types"
)

type ConcurrentModel int

const (
	Linear ConcurrentModel = iota
	NoLinear
)

func (b *Bucket) writeChannel() {
	switch b.cfg.WModel {
	case Linear:
		b.singleChannel()
	case NoLinear:
		b.concurrentChannel()
	}
}

func (b *Bucket) _doWrite(kvp internalReq) (string, bool) {
	v, e := b.store.Get(kvp.Key)
	exist := e == nil
	switch kvp.t {
	case types.EventSet:
		kvp.done <- b.store.Set(kvp.Key, kvp.Value)
	case types.EventDelete:
		kvp.done <- b.store.Del(kvp.Key)
	default:
		kvp.done <- nil
	}
	return v, exist
}

func (b *Bucket) singleChannel() {
	for {
		kvp := <-b.wChannel
		oldV, oldEx := b._doWrite(kvp)
		go b.doEvent(oldV, oldEx, kvp)
	}
}

func (b *Bucket) doEvent(origV string, origExist bool, kvp internalReq) {
	switch kvp.t {
	case types.EventSet:
		if origExist && origV == kvp.Value {
			return
		}
		b.Watcher.EmitEvent(kvp.Key, types.EventSet)
	case types.EventDelete:
		if !origExist {
			return
		}
		b.Watcher.EmitEvent(kvp.Key, types.EventDelete)
	case types.EventAll:
		b.Watcher.EmitEvent(kvp.Key, types.EventAll)
	}
}

func (b *Bucket) concurrentChannel() {
	for {
		kv := <-b.wChannel
		go func(kvp internalReq) {
			oldV, oldEx := b._doWrite(kvp)
			b.doEvent(oldV, oldEx, kvp)
		}(kv)
	}
}
