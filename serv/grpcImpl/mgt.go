package grpcImpl

import (
	"context"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/types"
)

type ManagerService struct {
	pb.UnimplementedManagerServiceServer
	Db *KV.Database
}

func (m ManagerService) Connect(ctx context.Context, request *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	_, ns, err := m.Db.Connect(
		request.GetNamespace(),
		types.ConnectStrategy(request.GetStrategy()),
		types.ConcurrentModelI32Cov(int32(request.GetModel())))
	if err != nil {
		return nil, err
	}
	return &pb.ConnectResponse{Ok: err == nil, Namespace: ns}, nil
}

var _ pb.ManagerServiceServer = &ManagerService{}
