package networking_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/networking"
)

func TestNewClient_NilLogger(t *testing.T) {
	_, err := networking.NewClient(context.Background(), "127.0.0.1:9999",
		func(cc grpc.ClientConnInterface) *grpc.ClientConn { return cc.(*grpc.ClientConn) },
		nil, nil, nil)

	if err == nil {
		t.Fatal("expected error for nil logger, got nil")
	}
}

func TestNewClient_WithMockClock(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()

	c, err := networking.NewClient(context.Background(), "127.0.0.1:9999",
		func(cc grpc.ClientConnInterface) *grpc.ClientConn { return cc.(*grpc.ClientConn) },
		logger, clock, nil)

	if err != nil {
		t.Fatalf("NewClient with lazy gRPC should not error: %v", err)
	}
	defer c.Close()
}

func TestClient_Close(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()

	c, err := networking.NewClient(context.Background(), "127.0.0.1:9999",
		func(cc grpc.ClientConnInterface) *grpc.ClientConn { return cc.(*grpc.ClientConn) },
		logger, clock, nil)

	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Errorf("unexpected close error: %v", err)
	}
}

func TestClient_ClientAccessor(t *testing.T) {
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()

	c, err := networking.NewClient(context.Background(), "127.0.0.1:9999",
		func(cc grpc.ClientConnInterface) *grpc.ClientConn { return cc.(*grpc.ClientConn) },
		logger, clock, nil)

	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if c.Client() == nil {
		t.Error("Client() should not return nil after successful NewClient")
	}
}
