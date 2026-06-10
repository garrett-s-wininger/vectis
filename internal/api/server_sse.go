package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/authz"
	"vectis/internal/dal"

	"google.golang.org/grpc/metadata"
)

const (
	defaultLogReplayLimit = 10000
	maxLogReplayLimit     = 50000
	maxLogTail            = 50000
)

const (
	getLogsReplayLimitMetadata = "vectis-log-replay-limit"
	getLogsTailMetadata        = "vectis-log-tail"
)

func (s *APIServer) HandleSSEJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	s.streamRunEvents(w, r, jobID, "job: "+jobID)
}

func (s *APIServer) streamRunEvents(w http.ResponseWriter, r *http.Request, subscriptionKey, logSubject string) {
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// NOTE(garrett): Prevent buffering behind various proxies for lower latency.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrStreamingUnsupported)
		return
	}

	clearResponseWriteDeadline(w)

	ch := s.runBroadcaster.Subscribe(subscriptionKey)
	defer s.runBroadcaster.Unsubscribe(subscriptionKey, ch)

	s.logger.Info("SSE client subscribed to runs for %s", logSubject)

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = w.Write([]byte(": keep-alive\n\n"))
			flusher.Flush()
		case payload, ok := <-ch:
			if !ok {
				return
			}
			if _, err := w.Write([]byte("data: ")); err != nil {
				return
			}
			if _, err := w.Write(payload); err != nil {
				return
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *APIServer) GetRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	replay, ok := parseLogReplayRequest(w, r)
	if !ok {
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	holder := s.logClient.Load()
	if holder == nil || holder.client == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "log_service_unavailable", "log service not connected", nil)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming unsupported", nil)
		return
	}

	clearResponseWriteDeadline(w)

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	logCtx := metadata.AppendToOutgoingContext(
		r.Context(),
		getLogsReplayLimitMetadata, strconv.Itoa(replay.ReplayLimit),
	)

	if replay.Tail > 0 {
		logCtx = metadata.AppendToOutgoingContext(logCtx, getLogsTailMetadata, strconv.Itoa(replay.Tail))
	}

	stream, err := holder.client.GetLogs(logCtx, &api.GetLogsRequest{RunId: &runID, SinceSequence: &replay.SinceSequence})
	if err != nil {
		s.logger.Warn("Failed to connect to log service: %v", err)
		writeAPIError(w, http.StatusBadGateway, "log_service_error", "failed to connect to log service", nil)
		return
	}

	sawCompletion := false
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}

		if _, err := w.Write(formatLogChunkEvent(chunk)); err != nil {
			return
		}
		flusher.Flush()

		if chunk.GetCompleted() != api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
			sawCompletion = true
		}
	}

	if !sawCompletion {
		// NOTE(garrett): Use a fresh context for the one-shot fallback — the handler's DB context
		// may have expired if the SSE stream has been alive longer than the timeout.
		fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer fallbackCancel()
		status, found, err := s.runs.GetRunStatus(fallbackCtx, runID)
		if err != nil {
			s.logger.Warn("Log completion fallback DB lookup failed for run %s: %v", runID, err)
		} else if found {
			var completedStatus string
			switch status {
			case "succeeded":
				completedStatus = "success"
			case "failed":
				completedStatus = "failure"
			case "cancelled":
				completedStatus = "cancelled"
			case "abandoned":
				completedStatus = "abandoned"
			case "aborted":
				completedStatus = "aborted"
			}

			if completedStatus != "" {
				inner, _ := json.Marshal(struct {
					Event     string `json:"event"`
					Status    string `json:"status"`
					Synthetic bool   `json:"synthetic"`
				}{"completed", completedStatus, true})

				outer, _ := json.Marshal(struct {
					Timestamp string         `json:"timestamp"`
					Stream    api.Stream     `json:"stream"`
					Sequence  int64          `json:"sequence"`
					Data      string         `json:"data"`
					Completed api.RunOutcome `json:"completed,omitempty"`
				}{time.Now().Format(time.RFC3339Nano), api.Stream_STREAM_CONTROL, -1, string(inner), api.RunOutcome_RUN_OUTCOME_UNKNOWN})

				w.Write(formatSSEDataEvent(-1, outer))
				flusher.Flush()
			}
		}
	}
}

type logReplayRequest struct {
	SinceSequence int64
	Tail          int
	ReplayLimit   int
}

func parseLogReplayRequest(w http.ResponseWriter, r *http.Request) (logReplayRequest, bool) {
	q := r.URL.Query()
	replay := logReplayRequest{ReplayLimit: defaultLogReplayLimit}

	if raw := q.Get("replay_limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > maxLogReplayLimit {
			writeAPIError(w, http.StatusBadRequest, "invalid_replay_limit", fmt.Sprintf("replay_limit must be between 1 and %d", maxLogReplayLimit), nil)
			return logReplayRequest{}, false
		}
		replay.ReplayLimit = n
	}

	if raw := q.Get("tail"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 || n > maxLogTail {
			writeAPIError(w, http.StatusBadRequest, "invalid_tail", fmt.Sprintf("tail must be between 1 and %d", maxLogTail), nil)
			return logReplayRequest{}, false
		}

		replay.Tail = n
		if replay.ReplayLimit > n {
			replay.ReplayLimit = n
		}
	}

	rawSince := q.Get("since_sequence")
	if rawSince == "" {
		rawSince = r.Header.Get("Last-Event-ID")
	}

	if rawSince != "" {
		n, err := strconv.ParseInt(rawSince, 10, 64)
		if err != nil || n < 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_since_sequence", "since_sequence and Last-Event-ID must be non-negative integers", nil)
			return logReplayRequest{}, false
		}

		replay.SinceSequence = n
	}

	return replay, true
}

func formatLogChunkEvent(chunk *api.LogChunk) []byte {
	return formatSSEDataEvent(chunk.GetSequence(), formatLogChunkSSE(chunk))
}

func formatSSEDataEvent(sequence int64, data []byte) []byte {
	var b bytes.Buffer
	if sequence >= 0 {
		_, _ = fmt.Fprintf(&b, "id: %d\n", sequence)
	}

	b.WriteString("data: ")
	b.Write(data)
	b.WriteString("\n\n")

	return b.Bytes()
}

func formatLogChunkSSE(chunk *api.LogChunk) []byte {
	// The log server always sets Timestamp on GetLogs chunks. time.Now() is
	// a safety net for any future code path that leaves the field nil.
	ts := time.Now().Format(time.RFC3339Nano)
	if t := chunk.GetTimestamp(); t != nil {
		ts = t.AsTime().Format(time.RFC3339Nano)
	}

	data := string(chunk.GetData())
	b, err := json.Marshal(struct {
		Timestamp string         `json:"timestamp"`
		Stream    api.Stream     `json:"stream"`
		Sequence  int64          `json:"sequence"`
		Data      string         `json:"data"`
		Completed api.RunOutcome `json:"completed,omitempty"`
	}{
		Timestamp: ts,
		Stream:    chunk.GetStream(),
		Sequence:  chunk.GetSequence(),
		Data:      data,
		Completed: chunk.GetCompleted(),
	})

	if err != nil {
		b = []byte(`{}`)
	}

	return b
}

// clearResponseWriteDeadline removes the per-response write deadline so
// http.Server.WriteTimeout does not terminate long-lived SSE streams.
func clearResponseWriteDeadline(w http.ResponseWriter) {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
}
