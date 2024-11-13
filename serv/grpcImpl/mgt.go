package grpcImpl

import (
	"context"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
)

type ManagerService struct {
	pb.UnimplementedManagerServiceServer
	Db *KV.Database
}

func (m ManagerService) Connect(ctx context.Context, request *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	_, ns, err := m.Db.Connect(
		request.GetNamespace(),
		KV.ConnectStrategy(request.GetStrategy()),
		KV.ConcurrentModel(request.GetModel()))
	if err != nil {
		return nil, err
	}
	return &pb.ConnectResponse{Ok: err == nil, Namespace: ns}, nil
}

var _ pb.ManagerServiceServer = &ManagerService{}
