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
}

func (s *EventService) SubscribeEvents(req *pb.SubscribeRequest, stream pb.EventService_SubscribeEventsServer) error {
	wat := s.Watcher
	if req.GetNamespace() != "" {
		pnode, err := s.Db.MustGetStartedPnode(req.GetNamespace())
		if err != nil {
			return err
		}
		wat = pnode.Bkt.Watcher
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
	s.stream.Send(&pb.Event{EventVal: key, EventType: int32(eventType)})
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
