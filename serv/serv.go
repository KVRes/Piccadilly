package serv

import (
	"errors"
	"fmt"
	"github.com/KVRes/Piccadilly/KV"
	"github.com/KVRes/Piccadilly/pb"
	"github.com/KVRes/Piccadilly/serv/grpcImpl"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
	Db *KV.Database
}

func panicHandler(p any) error {
	fmt.Println("panic", p)
	return errors.New("panic")
}

func NewServer(basePath string) *Server {
	svr := &Server{
		Db: KV.NewDatabase(basePath),
		Server: grpc.NewServer(
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(panicHandler)),
			)),
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(panicHandler)),
			))),
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
