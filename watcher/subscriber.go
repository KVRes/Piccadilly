package watcher

import "github.com/KVRes/Piccadilly/types"

type BaseSubscriber struct {
	w *watcher
}

func (s *BaseSubscriber) Close() {
	s.w.obsolete = true
}

type Subscriber interface {
	SetBaseSubscriber(w BaseSubscriber)
	Notify(key string, eventType types.EventType)
	Close() error
}

type EventSubscriber struct {
	base BaseSubscriber
	ch   chan struct{}
}

func (s *EventSubscriber) Notify(key string, eventType types.EventType) {
	s.ch <- struct{}{}
}

func (s *EventSubscriber) Close() error {
	s.base.Close()
	close(s.ch)
	return nil
}

func (s *EventSubscriber) SetBaseSubscriber(w BaseSubscriber) {
	s.base = w
}

var _ Subscriber = &EventSubscriber{}
