package client

import (
	"context"

	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/watcher"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn *grpc.ClientConn
	ev   pb.EventServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	ev := pb.NewEventServiceClient(conn)
	return &Client{conn, ev}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

type Subscribed struct {
	Ch          <-chan watcher.ErrorableEvent
	Unsubscribe chan struct{}
}

func (c *Client) Watch(key string, eventType watcher.EventType) (Subscribed, error) {
	stream, err := c.ev.SubscribeEvents(context.Background(), &pb.SubscribeRequest{Key: key, EventType: int32(eventType)})
	if err != nil {
		return Subscribed{}, err
	}

	ch := make(chan watcher.ErrorableEvent)
	unsubscribe := make(chan struct{})
	go func() {
		for {
			select {
			case <-unsubscribe:
				return
			default:
				event, err := stream.Recv()
				if err != nil {
					ch <- watcher.ErrorableEvent{Err: err, IsError: true}
					continue
				}
				ch <- watcher.ErrorableEvent{
					Event: watcher.Event{
						Key:       event.EventVal,
						EventType: watcher.EventType(event.EventType),
					},
					Err:     nil,
					IsError: false,
				}
			}
		}
	}()
	return Subscribed{ch, unsubscribe}, nil
}
