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

type Options struct {
	ServerOptions []grpc.ServerOption
	DialOptions   []grpc.DialOption
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
	return StartServerWithOptions(t, Options{}, register)
}

func StartServerWithOptions(t *testing.T, opts Options, register func(*grpc.Server)) *Server {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}

	return StartServerOnListenerWithOptions(t, listener, opts, register)
}

func StartServerOnListener(t *testing.T, listener net.Listener, register func(*grpc.Server)) *Server {
	return StartServerOnListenerWithOptions(t, listener, Options{}, register)
}

func StartServerOnListenerWithOptions(t *testing.T, listener net.Listener, opts Options, register func(*grpc.Server)) *Server {
	t.Helper()

	server := grpc.NewServer(opts.ServerOptions...)
	register(server)

	go func() {
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Logf("server error: %v", err)
		}
	}()

	dialOpts := opts.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	conn, err := grpc.NewClient(listener.Addr().String(), dialOpts...)
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
