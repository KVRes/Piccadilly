package client

import (
	"github.com/KVRes/Piccadilly/types"
	"google.golang.org/grpc"
	"sync/atomic"
)

type Pool struct {
	clients []*Client
	idx     atomic.Int64
	n       int
}

func NewPool(n int, addr string, opts ...grpc.DialOption) (*Pool, error) {
	p := &Pool{
		n: n,
	}
	for i := 0; i < n; i++ {
		c, err := NewClient(addr, opts...)
		if err != nil {
			return nil, err
		}
		p.clients = append(p.clients, c)
	}
	return p, nil
}

func (p *Pool) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

func (p *Pool) Connect(path string, strategy types.ConnectStrategy, concu types.ConcurrentModel) error {
	for _, c := range p.clients {
		err := c.Connect(path, strategy, concu)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Pool) Client() *Client {
	defer p.idx.Add(1)
	return p.clients[p.idx.Load()%int64(p.n)]
}
