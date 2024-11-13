package Piccadilly

import (
	"sync"
)

type KeyWatcher struct {
	ws map[string][]watcher
	l  sync.RWMutex
}

type watcher struct {
	ch       chan struct{}
	et       EventType
	obsolete bool
}

func NewKeyWatcher() *KeyWatcher {
	return &KeyWatcher{ws: make(map[string][]watcher)}
}

func (w *KeyWatcher) Watch(key string, eventType EventType) <-chan struct{} {
	ch := make(chan struct{})
	w.l.Lock()
	defer w.l.Unlock()
	if _, ok := w.ws[key]; !ok {
		w.ws[key] = []watcher{}
	}
	w.ws[key] = append(w.ws[key], watcher{ch, eventType, false})
	return ch
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
			w.ch <- struct{}{}
		}
	}
}

func (w *KeyWatcher) GC() {
	w.l.Lock()
	defer w.l.Unlock()
	for k, v := range w.ws {
		w.ws[k] = filter(v, func(w watcher) bool {
			close(w.ch)
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

type EventType int

const (
	EventAll EventType = iota
	EventSet
	EventDelete
)
