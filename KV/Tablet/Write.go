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

func (b *Bucket) _doWrite(kvp internalReq) {
	switch kvp.t {
	case types.EventSet:
		kvp.done <- b.store.Set(kvp.Key, kvp.Value)
	case types.EventDelete:
		kvp.done <- b.store.Del(kvp.Key)
	default:
		kvp.done <- nil
	}
}

func (b *Bucket) singleChannel() {
	for {
		kvp := <-b.wChannel
		b._doWrite(kvp)
		go b.Watcher.EmitEvent(kvp.Key, kvp.t)
	}
}

func (b *Bucket) concurrentChannel() {
	for {
		kv := <-b.wChannel
		go func(kvp internalReq) {
			b._doWrite(kvp)
			b.Watcher.EmitEvent(kv.Key, kv.t)
		}(kv)
	}
}
