package watcher

import (
	"sync"
)

type KeyWatcher struct {
	ws map[string][]*watcher
	l  sync.RWMutex
}

type watcher struct {
	sub      Subscriber
	et       EventType
	obsolete bool
}

func NewKeyWatcher() *KeyWatcher {
	return &KeyWatcher{ws: make(map[string][]*watcher)}
}

func (w *KeyWatcher) Watch(key string, eventType EventType, subscriber Subscriber) {
	w.l.Lock()
	defer w.l.Unlock()
	if _, ok := w.ws[key]; !ok {
		w.ws[key] = []*watcher{}
	}
	_w := &watcher{subscriber, eventType, false}
	subscriber.SetBaseSubscriber(BaseSubscriber{w: _w})
	w.ws[key] = append(w.ws[key], _w)
}

func (w *KeyWatcher) EmitEvent(key string, eventType EventType) {
	if w == nil {
		return
	}
	w.l.RLock()
	defer w.l.RUnlock()
	for _, w := range w.ws[key] {
		if w.obsolete {
			continue
		}
		if w.et == eventType {
			w.sub.Notify(key, eventType)
		}
	}
}

func (w *KeyWatcher) GC() {
	w.l.Lock()
	defer w.l.Unlock()
	for k, v := range w.ws {
		w.ws[k] = filter(v, func(w *watcher) bool {
			if w == nil {
				return false
			}
			w.sub.Close()
			return !w.obsolete
		})

	}
}

func filter[T any](slice []T, f func(T) bool) []T {
	var result []T
	for _, v := range slice {
		if f(v) {
			result = append(result, v)
		}
	}
	return result
}

type Event struct {
	EventType EventType
	Key       string
}

type ErrorableEvent struct {
	Event
	Err     error
	IsError bool
}

type EventType int

const (
	EventAll EventType = iota
	EventSet
	EventDelete
)
