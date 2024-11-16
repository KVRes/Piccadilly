package client

import (
	"context"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"path/filepath"
	"strings"
)

type Client struct {
	conn *grpc.ClientConn
	ev   pb.EventServiceClient
	crud pb.CRUDServiceClient
	mgr  pb.ManagerServiceClient
	path string
}

func (c *Client) GetConn() *grpc.ClientConn {
	return c.conn
}

func (c *Client) Copy() *Client {
	return &Client{
		conn: c.conn,
		ev:   c.ev,
		crud: c.crud,
		mgr:  c.mgr,
		path: c.path,
	}
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
		mgr:  pb.NewManagerServiceClient(conn),
	}, nil
}

func (c *Client) GetCurrentPath() string {
	return "/" + c.path
}

func (c *Client) CleanPath() {
	c.path = ""
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Watch(key string, eventType types.EventType) (Subscribed, error) {
	stream, err := c.ev.SubscribeEvents(context.Background(), &pb.SubscribeRequest{
		Namespace: c.path,
		Key:       key, EventType: int32(eventType)})
	if err != nil {
		return Subscribed{}, err
	}

	ch := make(chan ErrorableEvent)
	unsubscribe := make(chan struct{})
	go func() {
		for {
			select {
			case <-unsubscribe:
				return
			default:
				event, err := stream.Recv()
				if err != nil {
					ch <- ErrorableEvent{Err: err, IsError: true}
					continue
				}
				ch <- ErrorableEvent{
					Event: Event{
						Key:       event.EventVal,
						EventType: types.EventType(event.EventType),
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
	resp, err := c.crud.Get(context.Background(), &pb.GetRequest{
		Namespace: c.path,
		Key:       key,
	})
	if err != nil {
		return "", err
	}
	return resp.GetVal(), nil
}

func (c *Client) Set(key, val string) error {
	_, err := c.crud.Set(context.Background(), &pb.SetRequest{
		Namespace: c.path,
		Key:       key, Val: val})
	return err
}

func (c *Client) Del(key string) error {
	_, err := c.crud.Del(context.Background(), &pb.DelRequest{
		Namespace: c.path, Key: key})
	return err
}

func (c *Client) Keys() ([]string, error) {
	resp, err := c.crud.Keys(context.Background(), &pb.KeysRequest{
		Namespace: c.path,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetKeys(), nil
}

func (c *Client) ListPNodes() ([]string, error) {
	resp, err := c.mgr.List(context.Background(), &pb.ListRequest{
		Namespace: c.path,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetPnodes(), nil
}

func (c *Client) CreatePNode(path string) error {
	path = strings.TrimSpace(path)
	path = strings.Trim(path, "/")
	base := strings.Trim(strings.TrimSpace(c.path), "/")
	_, err := c.mgr.Create(context.Background(), &pb.CreateRequest{
		Namespace: filepath.Join(base, path),
	})
	return err
}

func (c *Client) Connect(path string, strategy types.ConnectStrategy, concu types.ConcurrentModel) error {
	resp, err := c.mgr.Connect(context.Background(),
		&pb.ConnectRequest{
			Namespace: path,
			Strategy:  pb.ConnectionStrategy(int32(strategy)),
			Model:     pb.ConcurrentModel(types.ConcurrentModelToI32(concu))})
	if err == nil {
		c.path = resp.GetNamespace()
	}
	return err
}
