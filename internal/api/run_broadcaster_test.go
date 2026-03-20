package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
)

func TestRunBroadcaster_SubscribeBroadcastUnsubscribe(t *testing.T) {
	logger := mocks.NewMockLogger()
	b := api.NewRunBroadcaster(logger)
	jobID := "job-1"

	ch := b.Subscribe(jobID)
	b.Broadcast(jobID, "run-abc", 1)

	select {
	case message := <-ch:
		var ev api.RunEvent
		if err := json.Unmarshal(message, &ev); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if ev.RunID != "run-abc" || ev.RunIndex != 1 {
			t.Errorf("expected run_id=run-abc run_index=1, got run_id=%s run_index=%d", ev.RunID, ev.RunIndex)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broadcast")
	}

	if ok := b.Unsubscribe(jobID, ch); !ok {
		t.Fatal("expected unsubscribe to succeed")
	}

	// Channel should be closed after unsubscribe.
	_, ok := <-ch
	if ok {
		t.Fatal("expected channel to be closed after unsubscribe")
	}
}
