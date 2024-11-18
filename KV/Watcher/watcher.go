package Watcher

import (
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/utils"
	"sync"
)

type KeyWatcher struct {
	ws map[string][]*watcher
	l  sync.RWMutex
}

type watcher struct {
	sub      Subscriber
	et       types.EventType
	obsolete bool
}

func NewKeyWatcher() *KeyWatcher {
	return &KeyWatcher{ws: make(map[string][]*watcher)}
}

func (w *KeyWatcher) Watch(key string, eventType types.EventType, subscriber Subscriber) {
	w.l.Lock()
	defer w.l.Unlock()
	if _, ok := w.ws[key]; !ok {
		w.ws[key] = []*watcher{}
	}
	_w := &watcher{subscriber, eventType, false}
	subscriber.SetBaseSubscriber(BaseSubscriber{w: _w})
	w.ws[key] = append(w.ws[key], _w)
}

func (w *KeyWatcher) EmitEvent(key string, eventType types.EventType) {
	if w == nil {
		return
	}
	w.l.RLock()
	defer w.l.RUnlock()
	for _, w := range w.ws[key] {
		if w.obsolete {
			continue
		}
		if w.et == eventType || w.et == types.EventAll || eventType == types.EventClear {
			w.sub.Notify(key, eventType)
		}
	}
}

func (w *KeyWatcher) GC() {
	w.l.Lock()
	defer w.l.Unlock()
	for k, v := range w.ws {
		w.ws[k] = utils.Filter(v, func(w *watcher) bool {
			if w == nil {
				return false
			}
			w.sub.Close()
			return !w.obsolete
		})

	}
}

func (w *KeyWatcher) Close() {
	w.l.Lock()
	defer w.l.Unlock()
	for _, v := range w.ws {
		for _, w := range v {
			w.sub.Close()
		}
	}
}
