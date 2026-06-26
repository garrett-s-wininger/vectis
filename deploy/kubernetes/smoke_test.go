package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
)

func TestWaitForSmokeLogMarkersReconnectsFromLastSequence(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")

		switch atomic.AddInt32(&requests, 1) {
		case 1:
			if got := r.URL.Query().Get("since_sequence"); got != "" {
				t.Errorf("first since_sequence = %q, want empty", got)
			}

			writeSmokeLogTestEvent(t, w, 1, api.Stream_STREAM_STDOUT, "early log")
			writeSmokeLogTestEvent(t, w, 2, api.Stream_STREAM_CONTROL, `{"event":"completed","status":"success"}`)
		case 2:
			if got := r.URL.Query().Get("since_sequence"); got != "2" {
				t.Errorf("second since_sequence = %q, want 2", got)
			}

			writeSmokeLogTestEvent(t, w, 3, api.Stream_STREAM_STDOUT, "canonical-secret-ok")
		default:
			t.Errorf("unexpected log stream reconnect %d", requests)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var out bytes.Buffer
	err := waitForSmokeLogMarkers(ctx, server.URL, "", "run-1", []string{"canonical-secret-ok"}, &out)
	if err != nil {
		t.Fatalf("waitForSmokeLogMarkers: %v", err)
	}

	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}

	if text := out.String(); strings.Count(text, "early log") != 1 || !strings.Contains(text, "canonical-secret-ok") {
		t.Fatalf("output = %q, want deduplicated replay plus marker", text)
	}
}

func writeSmokeLogTestEvent(t *testing.T, w http.ResponseWriter, sequence int64, stream api.Stream, data string) {
	t.Helper()

	body, err := json.Marshal(struct {
		Stream   int    `json:"stream"`
		Sequence int64  `json:"sequence"`
		Data     string `json:"data"`
	}{
		Stream:   int(stream.Number()),
		Sequence: sequence,
		Data:     data,
	})

	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	_, _ = fmt.Fprintf(w, "id: %s\n", strconv.FormatInt(sequence, 10))
	_, _ = fmt.Fprintf(w, "data: %s\n\n", body)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}
