package api

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsTransientEnqueueError(t *testing.T) {
	if isTransientEnqueueError(nil) {
		t.Fatal("nil should not be transient")
	}

	if !isTransientEnqueueError(status.Error(codes.Unavailable, "x")) {
		t.Fatal("Unavailable should be transient")
	}

	if !isTransientEnqueueError(status.Error(codes.DeadlineExceeded, "x")) {
		t.Fatal("DeadlineExceeded should be transient")
	}

	if isTransientEnqueueError(status.Error(codes.InvalidArgument, "x")) {
		t.Fatal("InvalidArgument should not be transient")
	}

	if isTransientEnqueueError(errors.New("plain")) {
		t.Fatal("non-gRPC error should not be transient")
	}
}
