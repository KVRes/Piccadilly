package serv

import (
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/serv/grpcImpl"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
	Db *KV.Database
}

func NewServer(basePath string) *Server {
	svr := &Server{
		Db:     KV.NewDatabase(basePath),
		Server: grpc.NewServer(),
	}

	crud := &grpcImpl.CRUDService{
		Db: svr.Db,
	}
	event := &grpcImpl.EventService{
		Db: svr.Db,
	}
	mgmt := &grpcImpl.ManagerService{
		Db: svr.Db,
	}

	pb.RegisterCRUDServiceServer(svr.Server, crud)
	pb.RegisterEventServiceServer(svr.Server, event)
	pb.RegisterManagerServiceServer(svr.Server, mgmt)

	return svr
}

func (s *Server) ServeTCP(addr string) error {
	return grpcImpl.RunGRPC(s.Server, addr)
}
