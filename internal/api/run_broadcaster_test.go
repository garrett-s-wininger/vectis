package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"

	"github.com/gorilla/websocket"
)

func TestRunBroadcaster_SubscribeBroadcastUnsubscribe(t *testing.T) {
	logger := mocks.NewMockLogger()
	b := api.NewRunBroadcaster(logger)
	jobID := "job-1"

	var upgrader = websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade: %v", err)
		}

		ch := b.Subscribe(jobID, conn)
		defer func() {
			if c, ok := b.Unsubscribe(jobID, conn); ok {
				close(c)
			}
			conn.Close()
		}()

		for payload := range ch {
			conn.WriteMessage(websocket.TextMessage, payload)
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	b.Broadcast(jobID, "run-abc", 1)

	_, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var ev api.RunEvent
	if err := json.Unmarshal(message, &ev); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if ev.RunID != "run-abc" || ev.RunIndex != 1 {
		t.Errorf("expected run_id=run-abc run_index=1, got run_id=%s run_index=%d", ev.RunID, ev.RunIndex)
	}
}
