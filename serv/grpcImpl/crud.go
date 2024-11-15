package grpcImpl

import (
	"context"
	"errors"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/pb"
)

var ErrNilBucket = errors.New("nil bucket")

func bktNotNil(bkt *Tablet.Bucket) bool {
	return bkt != nil
}

type CRUDService struct {
	pb.UnimplementedCRUDServiceServer
	B     *Tablet.Bucket
	Db    *KV.Database
	Debug bool
}

func (c *CRUDService) defaultBkt() *Tablet.Bucket {
	if c.Debug {
		return c.B
	}
	return nil
}

func (c *CRUDService) Set(ctx context.Context, request *pb.SetRequest) (*pb.CRUDResponse, error) {
	bkt := c.defaultBkt()
	if request.GetNamespace() != "" {
		pnode, err := c.Db.MustGetStartedPnode(request.GetNamespace())
		if err != nil {
			return nil, err
		}
		bkt = pnode.Bkt
	}
	if bkt == nil {
		return nil, ErrNilBucket
	}
	err := bkt.Set(request.GetKey(), request.GetVal())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

func (c *CRUDService) Get(ctx context.Context, request *pb.GetRequest) (*pb.CRUDResponse, error) {
	bkt := c.defaultBkt()
	if request.GetNamespace() != "" {
		pnode, err := c.Db.MustGetStartedPnode(request.GetNamespace())
		if err != nil {
			return nil, err
		}
		bkt = pnode.Bkt
	}
	if bkt == nil {
		return nil, ErrNilBucket
	}
	val, err := bkt.Get(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil, Val: val}, err
}

func (c *CRUDService) Del(ctx context.Context, request *pb.DelRequest) (*pb.CRUDResponse, error) {
	bkt := c.defaultBkt()
	if request.GetNamespace() != "" {
		pnode, err := c.Db.MustGetStartedPnode(request.GetNamespace())
		if err != nil {
			return nil, err
		}
		bkt = pnode.Bkt
	}
	if bkt == nil {
		return nil, ErrNilBucket
	}
	err := bkt.Del(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

var _ pb.CRUDServiceServer = &CRUDService{}
