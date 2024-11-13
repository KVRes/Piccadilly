package grpcImpl

import (
	"context"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
)

type CRUDService struct {
	pb.UnimplementedCRUDServiceServer
	B *KV.Bucket
}

func (c CRUDService) Set(ctx context.Context, request *pb.SetRequest) (*pb.CRUDResponse, error) {
	err := c.B.Set(request.GetKey(), request.GetVal())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

func (c CRUDService) Get(ctx context.Context, request *pb.GetRequest) (*pb.CRUDResponse, error) {
	val, err := c.B.Get(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil, Val: val}, err
}

func (c CRUDService) Del(ctx context.Context, request *pb.DelRequest) (*pb.CRUDResponse, error) {
	err := c.B.Del(request.GetKey())
	return &pb.CRUDResponse{Ok: err == nil}, err
}

var _ pb.CRUDServiceServer = &CRUDService{}
