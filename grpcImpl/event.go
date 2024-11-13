package grpcImpl

import (
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/watcher"
)

type EventService struct {
	pb.UnimplementedEventServiceServer
	w *watcher.KeyWatcher
}

func (s *EventService) SubscribeEvents(req *pb.SubscribeRequest, stream pb.EventService_SubscribeEventsServer) error {
	ch := make(chan struct{})
	sub := &GRPCSubscriber{stream, ch}
	s.w.Watch(req.Key, watcher.EventType(req.EventType), sub)
	<-ch
	return nil
}

func NewEventService() *EventService {
	return &EventService{}
}

var _ pb.EventServiceServer = &EventService{}

type GRPCSubscriber struct {
	stream pb.EventService_SubscribeEventsServer
	off    chan struct{}
}

func (s *GRPCSubscriber) Notify(key string, eventType watcher.EventType) {
	s.stream.Send(&pb.Event{EventVal: key, EventType: int32(eventType)})
}

func (s *GRPCSubscriber) Close() error {
	s.off <- struct{}{}
	return nil
}
