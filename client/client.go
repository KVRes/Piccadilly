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
	crud pb.CRUDServiceClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		ev:   pb.NewEventServiceClient(conn),
		crud: pb.NewCRUDServiceClient(conn),
	}, nil
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

func (c *Client) Get(key string) (string, error) {
	resp, err := c.crud.Get(context.Background(), &pb.GetRequest{Key: key})
	if err != nil {
		return "", err
	}
	return resp.GetVal(), nil
}

func (c *Client) Set(key, val string) error {
	_, err := c.crud.Set(context.Background(), &pb.SetRequest{Key: key, Val: val})
	return err
}

func (c *Client) Del(key string) error {
	_, err := c.crud.Del(context.Background(), &pb.DelRequest{Key: key})
	return err
}
