package Tablet

import (
	"github.com/KVRes/Piccadilly/types"
	"log"
)

func (b *Bucket) writeChannel() {
	var wFx func()
	switch b.cfg.WModel {
	case types.NoLinear:
		log.Println("NonLinear no need write channel, channel daemon exited")
		return
	case types.Linear:
		wFx = b.linearChannel
	default:
		log.Print("unknown write model, use Linear as default")
	}

	b.loop(wFx)

}

func (b *Bucket) _doWrite(kvp internalReq, counter bool) (types.Value, bool) {
	if counter {
		// 858w RPS -> 846w RPS
		b.wCount.Add(1)
		defer b.wCount.Done()
	}

	if kvp.t == types.EventClear {
		b.store.Clear()
		kvp.done <- nil
		return types.Value{}, false
	}

	v, e := b.store.Get(kvp.Key) // FIXME: This will cause ~3% perf degrade, but is required for event delta
	exist := e == nil
	switch kvp.t {
	case types.EventSet:
		if exist && v.Equals(kvp.Value) {
			kvp.done <- nil
			return v, exist
		}
		kvp.done <- b.store.Set(kvp.Key, kvp.Value)
	case types.EventDelete:
		if !exist {
			kvp.done <- nil
			return v, exist
		}
		kvp.done <- b.store.Del(kvp.Key)
	default:
		kvp.done <- nil
	}
	if !exist {
		return types.Value{}, false
	}
	return v, exist
}

func (b *Bucket) linearChannel() {
	kvp := <-b.wChannel
	oldV, oldEx := b._doWrite(kvp, false)
	go b.doEvent(oldV, oldEx, kvp)
}

func (b *Bucket) noLinearChannel(req internalReq) {
	oldV, oldEx := b._doWrite(req, true)
	go b.doEvent(oldV, oldEx, req)
}

func (b *Bucket) doEvent(origV types.Value, origExist bool, kvp internalReq) {
	switch kvp.t {
	case types.EventSet:
		if origExist && origV.Equals(kvp.Value) {
			return
		}
	case types.EventDelete:
		if !origExist {
			return
		}
	}
	b.Watcher.EmitEvent(kvp.Key, kvp.t)
}
