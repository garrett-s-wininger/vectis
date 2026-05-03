package grpctest

import (
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	GRPC     *grpc.Server
	Listener net.Listener
	Conn     *grpc.ClientConn
}

func (s *Server) Addr() string {
	return s.Listener.Addr().String()
}

func (s *Server) Close() {
	if s.Conn != nil {
		_ = s.Conn.Close()
	}

	if s.GRPC != nil {
		s.GRPC.Stop()
	}
}

func SetupGRPCServer(t *testing.T, register func(*grpc.Server)) (*grpc.Server, net.Listener, *grpc.ClientConn) {
	server := StartServer(t, register)
	return server.GRPC, server.Listener, server.Conn
}

func StartServer(t *testing.T, register func(*grpc.Server)) *Server {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	return StartServerOnListener(t, listener, register)
}

func StartServerOnListener(t *testing.T, listener net.Listener, register func(*grpc.Server)) *Server {
	t.Helper()

	server := grpc.NewServer()
	register(server)

	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()

	conn, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		_ = listener.Close()
		server.Stop()
		t.Fatalf("failed to dial: %v", err)
	}

	testServer := &Server{
		GRPC:     server,
		Listener: listener,
		Conn:     conn,
	}

	t.Cleanup(func() {
		testServer.Close()
	})

	return testServer
}
