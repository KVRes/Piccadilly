package grpcImpl

import (
	"google.golang.org/grpc"
	"net"
)

func RunGRPC(serv *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if err = serv.Serve(lis); err != nil {
		return err
	}
	return nil
}
