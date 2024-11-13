package grpcImpl

import (
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/types"
	"github.com/KVRes/Piccadilly/watcher"
)

type EventService struct {
	pb.UnimplementedEventServiceServer
	Watcher *watcher.KeyWatcher
}

func (s *EventService) SubscribeEvents(req *pb.SubscribeRequest, stream pb.EventService_SubscribeEventsServer) error {
	ch := make(chan struct{})
	sub := &GRPCSubscriber{stream: stream, off: ch}
	s.Watcher.Watch(req.Key, types.EventType(req.EventType), sub)
	<-ch
	return nil
}

var _ pb.EventServiceServer = &EventService{}

type GRPCSubscriber struct {
	base   watcher.BaseSubscriber
	stream pb.EventService_SubscribeEventsServer
	off    chan struct{}
}

func (s *GRPCSubscriber) Notify(key string, eventType types.EventType) {
	s.stream.Send(&pb.Event{EventVal: key, EventType: int32(eventType)})
}

func (s *GRPCSubscriber) Close() error {
	s.base.Close()
	s.off <- struct{}{}
	return nil
}

func (s *GRPCSubscriber) SetBaseSubscriber(w watcher.BaseSubscriber) {
	s.base = w
}

var _ watcher.Subscriber = &GRPCSubscriber{}
