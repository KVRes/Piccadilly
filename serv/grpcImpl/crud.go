package grpcImpl

import (
	"context"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/pb"
)

type CRUDService struct {
	pb.UnimplementedCRUDServiceServer
	B     *Tablet.Bucket
	Db    *KV.Database
	Debug bool
}

func (c *CRUDService) getBkt(namespace INamespaceGetter) (*Tablet.Bucket, error) {
	if c.Debug {
		return notNilBkt(c.B)
	}
	bkt, err := getBkt(c.Db, namespace)
	if err != nil {
		return nil, err
	}
	return notNilBkt(bkt)
}

func (c *CRUDService) Set(ctx context.Context, request *pb.SetRequest) (*pb.CRUDResponse, error) {
	bkt, err := c.getBkt(request)
	if err != nil {
		return nil, err
	}
	err = bkt.Set(request.GetKey(), request.GetVal())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

func (c *CRUDService) Get(ctx context.Context, request *pb.GetRequest) (*pb.CRUDResponse, error) {
	bkt, err := c.getBkt(request)
	if err != nil {
		return nil, err
	}
	val, err := bkt.Get(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil, Val: val}, err
}

func (c *CRUDService) Del(ctx context.Context, request *pb.DelRequest) (*pb.CRUDResponse, error) {
	bkt, err := c.getBkt(request)
	if err != nil {
		return nil, err
	}
	err = bkt.Del(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

var _ pb.CRUDServiceServer = &CRUDService{}
