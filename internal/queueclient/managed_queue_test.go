package queueclient

import (
	"testing"

	"google.golang.org/grpc/connectivity"
)

func TestManagingQueueService_GRPCConnectivityState_nilConn(t *testing.T) {
	t.Parallel()
	m := &ManagingQueueService{}
	if got := m.GRPCConnectivityState(); got != connectivity.Shutdown {
		t.Fatalf("nil conn: expected %v, got %v", connectivity.Shutdown, got)
	}
}
