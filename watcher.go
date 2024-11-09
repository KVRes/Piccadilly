package piccadilly

import "sync"

type KeyWatcher struct {
	ws map[string][]watcher
	l  sync.RWMutex
}

type watcher struct {
	ch chan struct{}
	et EventType
}

func (w *KeyWatcher) Watch(key string, eventType EventType) <-chan struct{} {
	w.l.Lock()
	defer w.l.Unlock()
	if _, ok := w.ws[key]; !ok {
		w.ws[key] = []watcher{}
	}
	ch := make(chan struct{})
	w.ws[key] = append(w.ws[key], watcher{ch, eventType})
	return ch
}

func (w *KeyWatcher) EmitEvent(key string, eventType EventType) {
	if w == nil {
		return
	}
	w.l.RLock()
	defer w.l.RUnlock()
	for _, w := range w.ws[key] {
		if w.et == eventType {
			w.ch <- struct{}{}
		}
	}
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
