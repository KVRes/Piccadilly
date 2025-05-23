package grpcImpl

import (
	"context"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/KV/Tablet"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/types"
	"time"
)

type CRUDService struct {
	pb.UnimplementedCRUDServiceServer
	B     *Tablet.Bucket
	Db    *KV.Database
	Debug bool
}

func (c *CRUDService) Len(ctx context.Context, namespace *pb.Namespace) (*pb.IntResponse, error) {
	bkt, err := c.getBkt(namespace)
	if err != nil {
		return nil, err
	}

	return &pb.IntResponse{Val: int32(bkt.Len())}, err
}

func (c *CRUDService) Clear(ctx context.Context, namespace *pb.Namespace) (*pb.OkResponse, error) {
	bkt, err := c.getBkt(namespace)
	if err != nil {
		return nil, err
	}
	err = bkt.Clear()
	return &pb.OkResponse{Ok: err == nil}, err
}

func (c *CRUDService) Keys(ctx context.Context, request *pb.KeysRequest) (*pb.KeysResponse, error) {
	bkt, err := c.getBkt(request)
	if err != nil {
		return nil, err
	}
	keys, err := bkt.Keys()
	return &pb.KeysResponse{Ok: err == nil, Keys: keys}, err
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
	val := types.Value{
		Data: request.GetVal(),
	}
	if request.Ttl != nil {
		exp := time.Now().Unix() + int64(request.GetTtl())
		val.Expire = &exp
	}
	err = bkt.Set(request.GetKey(), val)
	return &pb.CRUDResponse{Ok: err == nil}, err
}

func (c *CRUDService) Get(ctx context.Context, request *pb.GetRequest) (*pb.CRUDResponse, error) {
	bkt, err := c.getBkt(request)
	if err != nil {
		return nil, err
	}
	val, err := bkt.Get(request.GetKey())
	if err != nil {
		return nil, err
	}
	return &pb.CRUDResponse{Ok: err == nil, Val: val.Data}, nil
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
