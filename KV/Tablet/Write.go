package Tablet

import (
	"fmt"
	"github.com/KVRes/Piccadilly/types"
)

type ConcurrentModel int

const (
	Linear ConcurrentModel = iota
	Buffer
	NoLinear
)

func (b *Bucket) writeChannel() {
	switch b.cfg.WModel {
	case Linear:
		b.singleChannel()
	case Buffer:
		b.bufferChannel()
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

func (b *Bucket) bufferChannel() {
	keyBuf := newKeyBuf(b.cfg.WKeySet)
	fmt.Println(len(keyBuf.keys))
	for {
		kv := <-b.wChannel
		empty := keyBuf.findEmpty(kv.Key)
		if empty == -1 {
			// buffer is full, wait for a slot
			go b.appendToWChannel(kv)
			continue
		}
		keyBuf.keys[empty] = kv.Key
		go func(kvp internalReq, idx int) {
			b._doWrite(kvp)
			keyBuf.keys[idx] = ""
			b.Watcher.EmitEvent(kv.Key, kv.t)
		}(kv, empty)
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
