package tests

import (
	"github.com/KVRes/Piccadilly/client"
	"github.com/KVRes/Piccadilly/grpcImpl"
	"github.com/KVRes/Piccadilly/pb"
	"google.golang.org/grpc"
	"log"
	"testing"
)

func TestGRPC(t *testing.T) {
	db := initDB()
	crud := &grpcImpl.CRUDService{
		B: db,
	}
	event := &grpcImpl.EventService{
		Watcher: db.Watcher,
	}

	serv := grpc.NewServer()
	defer serv.Stop()

	pb.RegisterCRUDServiceServer(serv, crud)
	pb.RegisterEventServiceServer(serv, event)
	go func() {
		err := grpcImpl.RunGRPC(serv, "127.0.0.1:12306")
		if err != nil {
			log.Fatalln(err)
		}
	}()

	cli, err := client.NewClient("127.0.0.1:12306")
	if err != nil {
		log.Fatalln(err)
	}

	m := dataset()
	for k, v := range m {
		_v, _err := cli.Get(k)
		if _err != nil {
			t.Fatal(_err)
		}
		if _v != v {
			t.Fatal("expected", v, "got", _v)
		}
	}

}
