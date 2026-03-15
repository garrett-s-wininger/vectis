package grpctest

import (
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func SetupGRPCServer(t *testing.T, register func(*grpc.Server)) (*grpc.Server, net.Listener, *grpc.ClientConn) {
	t.Helper()

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	server := grpc.NewServer()
	register(server)

	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		listener.Close()
		t.Fatalf("failed to dial: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
		server.Stop()
	})

	return server, listener, conn
}
