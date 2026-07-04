package faultinject

import (
	"context"
	"errors"
	"testing"
)

func TestScriptFailNextFailsOnce(t *testing.T) {
	script := NewScript()
	point := Point("test.point")
	script.FailNext(point, nil)

	if err := script.Before(context.Background(), point); !errors.Is(err, ErrInjected) {
		t.Fatalf("first hit error = %v, want ErrInjected", err)
	}

	if err := script.Before(context.Background(), point); err != nil {
		t.Fatalf("second hit error = %v, want nil", err)
	}

	if got := script.Hits(point); got != 2 {
		t.Fatalf("hits = %d, want 2", got)
	}
}

func TestScriptFailOnSpecificHit(t *testing.T) {
	script := NewScript()
	point := Point("test.point")
	customErr := errors.New("boom")
	script.FailOn(point, 3, customErr)

	for hit := 1; hit <= 2; hit++ {
		if err := script.Before(context.Background(), point); err != nil {
			t.Fatalf("hit %d error = %v, want nil", hit, err)
		}
	}

	if err := script.Before(context.Background(), point); !errors.Is(err, customErr) {
		t.Fatalf("third hit error = %v, want custom error", err)
	}
}

func TestScriptHonorsContextCancellation(t *testing.T) {
	script := NewScript()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := script.Before(ctx, Point("test.point")); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
