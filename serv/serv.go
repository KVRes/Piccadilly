package serv

import (
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/serv/grpcImpl"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
	db         *KV.Database
}

func NewServer(basePath string) *Server {
	svr := &Server{
		db:     KV.NewDatabase(basePath),
		Server: grpc.NewServer(),
	}

	crud := &grpcImpl.CRUDService{
		Db: svr.db,
	}
	event := &grpcImpl.EventService{
		Db: svr.db,
	}
	mgmt := &grpcImpl.ManagerService{
		Db: svr.db,
	}

	pb.RegisterCRUDServiceServer(svr.Server, crud)
	pb.RegisterEventServiceServer(svr.Server, event)
	pb.RegisterManagerServiceServer(svr.Server, mgmt)

	return svr
}

func (s *Server) ServeTCP(addr string) error {
	return grpcImpl.RunGRPC(s.Server, addr)
}
