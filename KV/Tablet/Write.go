package Tablet

import (
	"github.com/KVRes/Piccadilly/types"
)

type ConcurrentModel string

const (
	Linear   ConcurrentModel = "linear"
	NoLinear ConcurrentModel = "nolinear"
)

const (
	LinearGRPC int32 = iota
	NoLinearGRPC
)

func ConcurrentModelI32Cov(m int32) ConcurrentModel {
	switch m {
	case LinearGRPC:
		return Linear
	case NoLinearGRPC:
		return NoLinear
	default:
		return Linear
	}
}

func ConcurrentModelToI32(m ConcurrentModel) int32 {
	switch m {
	case Linear:
		return LinearGRPC
	case NoLinear:
		return NoLinearGRPC
	default:
		return LinearGRPC
	}
}

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
