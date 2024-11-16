package client

import "github.com/KVRes/Piccadilly/types"

type Subscribed struct {
	Ch          chan ErrorableEvent
	Unsubscribe chan struct{}
}

func (s Subscribed) Close() error {
	close(s.Unsubscribe)
	close(s.Ch)
	return nil
}

func (s Subscribed) Customer() *EventCustomer {
	return &EventCustomer{
		Sub: s,
		m:   make(map[types.EventType]EventHandler),
	}
}

type EventCustomer struct {
	Sub     Subscribed
	m       map[types.EventType]EventHandler
	exit    chan struct{}
	started bool
}

type EventHandler func(event ErrorableEvent)

func (e *EventCustomer) On(ts types.EventType, f EventHandler) {
	e.m[ts] = f
}

func (e *EventCustomer) Off(ts types.EventType) {
	delete(e.m, ts)
}

func (e *EventCustomer) Start() {
	if e.started {
		return
	}
	e.exit = make(chan struct{})
	e.started = true
	for {
		select {
		case <-e.exit:
			return
		case event := <-e.Sub.Ch:
			f, ok := e.m[event.EventType]
			if ok {
				f(event)
				return
			}
		}
	}
}

func (e *EventCustomer) Close() error {
	close(e.exit)
	e.started = false
	return e.Sub.Close()
}
