package KV

import (
	"github.com/KVRes/Piccadilly/types"
)

func (b *Bucket) writeChannel() {
	if b.cfg.WKeySet == 1 {
		b.singleChannel()
	} else {
		b.bufferChannel()
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

func (b *Bucket) bufferChannel() {
	keyBuf := newKeyBuf(b.cfg.WKeySet)
	for {
		kv := <-b.wChannel
		empty := keyBuf.findEmpty(kv.Key)
		if empty == -1 {
			// buffer is full, wait for a slot
			go b.appendToWChannel(kv)
			continue
		}
		go func(kvp internalReq, idx int) {
			b._doWrite(kvp)
			keyBuf.keys[idx] = ""
			b.Watcher.EmitEvent(kv.Key, kv.t)
		}(kv, empty)
	}
}
