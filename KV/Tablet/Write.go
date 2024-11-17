package Tablet

import (
	"github.com/KVRes/Piccadilly/types"
	"log"
)

func (b *Bucket) writeChannel() {
	switch b.cfg.WModel {
	case types.Linear:
		b.singleChannel()
	case types.NoLinear:
		b.concurrentChannel()
	default:
		log.Print("unknown write model, use Linear as default")
		b.singleChannel()
	}
}

func (b *Bucket) _doWrite(kvp internalReq, counter bool) (string, bool) {
	if counter {
		// 858w RPS -> 846w RPS
		b.wCount.Add(1)
		defer b.wCount.Done()
	}

	v, e := b.store.Get(kvp.Key) // FIXME: This will cause ~3% perf degrade, but is required for event delta
	exist := e == nil
	switch kvp.t {
	case types.EventSet:
		if exist && v.Data == kvp.Value.Data {
			kvp.done <- nil
			return v.Data, exist
		}
		kvp.done <- b.store.Set(kvp.Key, kvp.Value)
	case types.EventDelete:
		if !exist {
			kvp.done <- nil
			return "", exist
		}
		kvp.done <- b.store.Del(kvp.Key)
	default:
		kvp.done <- nil
	}
	return "", exist
}

func (b *Bucket) singleChannel() {
	for {
		kvp := <-b.wChannel
		oldV, oldEx := b._doWrite(kvp, false)
		go b.doEvent(oldV, oldEx, kvp)
	}
}

func (b *Bucket) doEvent(origV string, origExist bool, kvp internalReq) {
	switch kvp.t {
	case types.EventSet:
		if origExist && origV == kvp.Value.Data {
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
			oldV, oldEx := b._doWrite(kvp, true)
			b.doEvent(oldV, oldEx, kvp)
		}(kv)
	}
}
