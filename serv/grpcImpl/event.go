package grpcImpl

import (
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Watcher"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/types"
)

type EventService struct {
	pb.UnimplementedEventServiceServer
	Watcher *Watcher.KeyWatcher
	Db      *KV.Database
	Debug   bool
}

func (s *EventService) getWatcher(namespace INamespaceGetter) (*Watcher.KeyWatcher, error) {
	if s.Debug {
		return notNilWatcher(s.Watcher)
	}
	bkt, err := getBkt(s.Db, namespace)
	if err != nil {
		return nil, err
	}
	return notNilWatcher(bkt.Watcher)
}

func (s *EventService) SubscribeEvents(req *pb.SubscribeRequest, stream pb.EventService_SubscribeEventsServer) error {
	wat, err := s.getWatcher(req)
	if err != nil {
		return err
	}

	ch := make(chan struct{})
	sub := &GRPCSubscriber{stream: stream, off: ch}
	wat.Watch(req.Key, types.EventType(req.EventType), sub)
	<-ch
	return nil
}

var _ pb.EventServiceServer = &EventService{}

type GRPCSubscriber struct {
	base   Watcher.BaseSubscriber
	stream pb.EventService_SubscribeEventsServer
	off    chan struct{}
}

func (s *GRPCSubscriber) Notify(key string, eventType types.EventType) {
	err := s.stream.Send(&pb.Event{EventVal: key, EventType: int32(eventType)})
	if err != nil {
		s.off <- struct{}{}
	}
}

func (s *GRPCSubscriber) Close() error {
	s.base.Close()
	s.off <- struct{}{}
	return nil
}

func (s *GRPCSubscriber) SetBaseSubscriber(w Watcher.BaseSubscriber) {
	s.base = w
}

var _ Watcher.Subscriber = &GRPCSubscriber{}
