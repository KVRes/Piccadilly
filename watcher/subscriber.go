package watcher

type Subscriber interface {
	Notify(key string, eventType EventType)
	Close() error
}

type EventSubscriber struct {
	ch chan struct{}
}

func (s *EventSubscriber) Notify(key string, eventType EventType) {
	s.ch <- struct{}{}
}

func (s *EventSubscriber) Close() error {
	close(s.ch)
	return nil
}
