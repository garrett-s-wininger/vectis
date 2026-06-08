package cellingress

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces/mocks"

	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
)

func newCellIngressRequest(method, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	req.Host = "localhost"
	return req
}

func TestHTTPServerSetsMaxHeaderBytes(t *testing.T) {
	srv := HTTPServer("127.0.0.1:0", http.NotFoundHandler())
	if srv.MaxHeaderBytes != httpsecurity.DefaultMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", srv.MaxHeaderBytes, httpsecurity.DefaultMaxHeaderBytes)
	}
}

func TestServerAppliesSecurityHeaders(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	assertSecurityHeader(t, rr, "X-Content-Type-Options", "nosniff")
	assertSecurityHeader(t, rr, "X-Frame-Options", "DENY")
	assertSecurityHeader(t, rr, "Referrer-Policy", "no-referrer")
	assertSecurityHeader(t, rr, "Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()")
	assertSecurityHeader(t, rr, "Cross-Origin-Opener-Policy", "same-origin")
	assertSecurityHeader(t, rr, "Cross-Origin-Resource-Policy", "same-origin")
	assertSecurityHeader(t, rr, "Cross-Origin-Embedder-Policy", "require-corp")
	assertSecurityHeader(t, rr, "Origin-Agent-Cluster", "?1")
	assertSecurityHeader(t, rr, "X-Permitted-Cross-Domain-Policies", "none")
	assertSecurityHeader(t, rr, "X-Download-Options", "noopen")
	assertSecurityHeader(t, rr, "Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	if got := rr.Header().Get("Strict-Transport-Security"); got != "" {
		t.Fatalf("Strict-Transport-Security over HTTP = %q, want empty", got)
	}
}

func TestServerAllowsConfiguredHost(t *testing.T) {
	t.Setenv("VECTIS_CELL_INGRESS_ALLOWED_HOSTS", "ingress.example:8085")
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	req.Host = "ingress.example:8085"
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	assertNoStore(t, rr)
}

func TestServerRejectsUntrustedHost(t *testing.T) {
	t.Setenv("VECTIS_CELL_INGRESS_ALLOWED_HOSTS", "ingress.example")
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	req.Host = "evil.example"
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusBadRequest, "invalid_host_header")
	assertNoStore(t, rr)
	assertSecurityHeader(t, rr, "X-Content-Type-Options", "nosniff")
	assertSecurityHeader(t, rr, "X-Frame-Options", "DENY")
}

func TestServerAppliesHSTSForDirectTLS(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	req.TLS = &tls.ConnectionState{}
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	assertSecurityHeader(t, rr, "Strict-Transport-Security", "max-age=31536000")
}

func TestSubmitExecutionAcceptsLocalEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}
	assertNoStore(t, rr)

	var resp submitExecutionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.Status != "accepted" {
		t.Fatalf("response status: got %q, want accepted", resp.Status)
	}

	if resp.CellID != "iad-a" {
		t.Fatalf("response cell_id: got %q, want iad-a", resp.CellID)
	}

	if resp.TaskID != "run-1:root" || resp.TaskAttemptID != "run-1:root:attempt:1" {
		t.Fatalf("response task identity: got task=%q attempt=%q", resp.TaskID, resp.TaskAttemptID)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued requests: got %d, want 1", len(reqs))
	}

	if reqs[0].GetJob().GetRunId() != "run-1" {
		t.Fatalf("queued run id: got %q, want run-1", reqs[0].GetJob().GetRunId())
	}
}

func TestSubmitExecutionRequiresAllowedProducerIdentityWhenConfigured(t *testing.T) {
	tests := []struct {
		name       string
		tlsState   *tls.ConnectionState
		wantStatus int
		wantCode   string
		wantQueued int
	}{
		{
			name:       "missing peer cert",
			wantStatus: http.StatusForbidden,
			wantCode:   "authorization_denied",
		},
		{
			name:       "wrong producer identity",
			tlsState:   tlsStateWithURI(t, "spiffe://vectis.local/service/worker"),
			wantStatus: http.StatusForbidden,
			wantCode:   "authorization_denied",
		},
		{
			name:       "allowed producer identity",
			tlsState:   tlsStateWithURI(t, "spiffe://vectis.local/service/api"),
			wantStatus: http.StatusAccepted,
			wantQueued: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			viper.Set("service_identity.cell_ingress_allowed_producer_identities", []string{"spiffe://vectis.local/service/api"})

			queue := mocks.NewMockQueueService()
			srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
			req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "iad-a")))
			req.TLS = tt.tlsState
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			if tt.wantCode != "" {
				assertErrorCode(t, rr, tt.wantStatus, tt.wantCode)
			} else if rr.Code != tt.wantStatus {
				t.Fatalf("status: got %d, want %d; body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}

			if got := len(queue.GetJobRequests()); got != tt.wantQueued {
				t.Fatalf("queued requests: got %d, want %d", got, tt.wantQueued)
			}
		})
	}
}

func TestProducerIdentityPolicyDoesNotBlockCellIngressHealth(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("service_identity.cell_ingress_allowed_producer_identities", []string{"spiffe://vectis.local/service/api"})

	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}
}

func TestSubmitExecutionRequiresJobRequest(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newExecutionRequest(t, strings.NewReader(`{}`))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusBadRequest, "missing_job_request")
}

func TestSubmitExecutionRequiresEnvelope(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	jobReq := validJobRequestForCell(t, "iad-a")
	delete(jobReq.Metadata, cell.ExecutionEnvelopeMetadataKey)
	req := newExecutionRequest(t, executionBody(t, jobReq))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusBadRequest, "missing_execution_envelope")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestSubmitExecutionRejectsWrongCell(t *testing.T) {
	queue := mocks.NewMockQueueService()
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "pdx-b")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusConflict, "wrong_cell")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestSubmitExecutionReturnsQueueUnavailable(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue closed"))
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusServiceUnavailable, "queue_unavailable")
}

func TestSubmitExecutionDurablyAcceptsBeforeReturningQueueUnavailable(t *testing.T) {
	queue := mocks.NewMockQueueService()
	queue.SetEnqueueError(errors.New("queue closed"))
	acceptances := &recordingAcceptanceStore{}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCellAttempt(t, "iad-a", 2)))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusServiceUnavailable, "queue_unavailable")

	got := acceptances.snapshot()
	if got.ExecutionID != "execution-1" {
		t.Fatalf("accepted execution: got %q, want execution-1", got.ExecutionID)
	}

	if got.RunID != "run-1" || got.JobID != "job-1" || got.CellID != "iad-a" {
		t.Fatalf("unexpected acceptance: %+v", got)
	}

	if got.TaskID != "run-1:root" || got.TaskKey != dal.RootTaskKey || got.TaskAttemptID != "run-1:root:attempt:2" || got.Attempt != 2 {
		t.Fatalf("unexpected acceptance task identity: %+v", got)
	}

	if got.DefinitionJSON == "" || got.RequestJSON == "" {
		t.Fatalf("acceptance should include definition and request JSON: %+v", got)
	}

	if acceptances.failedExecutionID != "execution-1" || !strings.Contains(acceptances.failedMessage, "queue closed") {
		t.Fatalf("expected enqueue failure marker, got execution=%q message=%q", acceptances.failedExecutionID, acceptances.failedMessage)
	}

	if acceptances.enqueuedExecutionID != "" {
		t.Fatalf("expected no enqueue success marker, got %q", acceptances.enqueuedExecutionID)
	}
}

func TestSubmitExecutionMarksDurableAcceptanceEnqueued(t *testing.T) {
	queue := mocks.NewMockQueueService()
	acceptances := &recordingAcceptanceStore{}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusAccepted, rr.Body.String())
	}

	if acceptances.enqueuedExecutionID != "execution-1" {
		t.Fatalf("expected enqueue success marker for execution-1, got %q", acceptances.enqueuedExecutionID)
	}

	if acceptances.failedExecutionID != "" {
		t.Fatalf("expected no enqueue failure marker, got %q", acceptances.failedExecutionID)
	}
}

func TestSubmitExecutionRejectsWrongCellBeforeDurableAccept(t *testing.T) {
	queue := mocks.NewMockQueueService()
	acceptances := &recordingAcceptanceStore{}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "pdx-b")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusConflict, "wrong_cell")
	if got := acceptances.snapshot(); got.ExecutionID != "" {
		t.Fatalf("wrong-cell request should not be durably accepted, got %+v", got)
	}
}

func TestSubmitExecutionReturnsConflictWhenAcceptanceConflicts(t *testing.T) {
	queue := mocks.NewMockQueueService()
	acceptances := &recordingAcceptanceStore{err: dal.ErrConflict}
	srv := NewQueueServer("iad-a", queue, mocks.NewMockLogger())
	srv.SetAcceptanceStore(acceptances)
	req := newExecutionRequest(t, executionBody(t, validJobRequestForCell(t, "iad-a")))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusConflict, "execution_conflict")
	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queued requests: got %d, want 0", got)
	}
}

func TestHealthEndpoints(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	for _, path := range []string{"/health/live", "/health/ready"} {
		t.Run(path, func(t *testing.T) {
			req := newCellIngressRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
			}
			assertNoStore(t, rr)
		})
	}
}

func TestRouteGuardRejectsUnknownCellIngressRoute(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/cell/v1/not-found", nil)
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusNotFound, "route_not_found")
	assertNoStore(t, rr)
}

func TestRouteGuardRejectsUnsafeCellIngressRequestTargets(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	tests := []struct {
		name   string
		target string
	}{
		{name: "absolute form", target: "http://example.test/health/live"},
		{name: "encoded path", target: "/%68ealth/live"},
		{name: "encoded slash", target: "/health%2Flive"},
		{name: "dot segment", target: "/health/../health/live"},
		{name: "duplicate slash", target: "/health//live"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newCellIngressRequest(http.MethodGet, tt.target, nil)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			assertErrorCode(t, rr, http.StatusBadRequest, "invalid_request_target")
			assertNoStore(t, rr)
			assertSecurityHeader(t, rr, "X-Content-Type-Options", "nosniff")
		})
	}
}

func TestRouteGuardRejectsCellIngressMethodMismatch(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	tests := []struct {
		name      string
		method    string
		path      string
		wantAllow string
	}{
		{name: "health post", method: http.MethodPost, path: "/health/live", wantAllow: "GET, HEAD"},
		{name: "execution get", method: http.MethodGet, path: "/cell/v1/executions", wantAllow: "POST"},
		{name: "execution trace", method: http.MethodTrace, path: "/cell/v1/executions", wantAllow: "POST"},
		{name: "lowercase post", method: "post", path: "/cell/v1/executions", wantAllow: "POST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newCellIngressRequest(tt.method, tt.path, nil)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			assertErrorCode(t, rr, http.StatusMethodNotAllowed, "method_not_allowed")
			assertNoStore(t, rr)
			if got := rr.Header().Get("Allow"); got != tt.wantAllow {
				t.Fatalf("Allow = %q, want %q", got, tt.wantAllow)
			}
		})
	}
}

func TestRouteGuardRejectsCellIngressReadRequestBodies(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			req := newCellIngressRequest(method, "/health/live", strings.NewReader("body"))
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			assertErrorCode(t, rr, http.StatusBadRequest, "request_body_not_allowed")
			assertNoStore(t, rr)
		})
	}
}

func TestRouteGuardRejectsCellIngressUnsupportedMediaType(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{name: "missing"},
		{name: "text plain", contentType: "text/plain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
			req := newCellIngressRequest(http.MethodPost, "/cell/v1/executions", strings.NewReader(`{}`))
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			assertErrorCode(t, rr, http.StatusUnsupportedMediaType, "unsupported_media_type")
			assertNoStore(t, rr)
		})
	}
}

func TestRouteGuardRejectsCellIngressOversizedBody(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newExecutionRequest(t, strings.NewReader(strings.Repeat("a", maxExecutionRequestBytes+1)))
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusRequestEntityTooLarge, "request_body_too_large")
	assertNoStore(t, rr)
}

func TestSubmitExecutionRejectsStreamingOversizedBody(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	body := `{"job_request":"` + strings.Repeat("a", maxExecutionRequestBytes+1) + `"}`
	req := newExecutionRequest(t, strings.NewReader(body))
	req.ContentLength = -1
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusRequestEntityTooLarge, "request_body_too_large")
	assertNoStore(t, rr)
}

func TestRouteGuardRejectsCellIngressMethodOverrideHeaders(t *testing.T) {
	for _, header := range []string{"X-HTTP-Method", "X-HTTP-Method-Override", "X-Method-Override"} {
		t.Run(header, func(t *testing.T) {
			srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
			req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
			req.Header.Set(header, http.MethodDelete)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			assertErrorCode(t, rr, http.StatusBadRequest, "method_override_forbidden")
			assertNoStore(t, rr)
		})
	}
}

func TestRouteGuardRejectsUnacceptableCellIngressMediaType(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
	req.Header.Set("Accept", "text/html")
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	assertErrorCode(t, rr, http.StatusNotAcceptable, "not_acceptable")
	assertNoStore(t, rr)
}

func TestRouteGuardAllowsJSONCellIngressAcceptHeaders(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())

	for _, accept := range []string{"", "application/json", "application/*", "*/*"} {
		t.Run(accept, func(t *testing.T) {
			req := newCellIngressRequest(http.MethodGet, "/health/live", nil)
			req.Header.Set("Accept", accept)
			rr := httptest.NewRecorder()

			srv.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
			}
		})
	}
}

func TestRouteGuardAllowsHEADForCellIngressHealth(t *testing.T) {
	srv := NewQueueServer("iad-a", mocks.NewMockQueueService(), mocks.NewMockLogger())
	req := newCellIngressRequest(http.MethodHead, "/health/live", nil)
	rr := httptest.NewRecorder()

	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, http.StatusOK, rr.Body.String())
	}

	assertNoStore(t, rr)
}

func newExecutionRequest(t *testing.T, body io.Reader) *http.Request {
	t.Helper()
	req := newCellIngressRequest(http.MethodPost, "/cell/v1/executions", body)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func executionBody(t *testing.T, req *api.JobRequest) *bytes.Reader {
	t.Helper()

	payload, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}

	body, err := json.Marshal(submitExecutionRequest{JobRequest: payload})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	return bytes.NewReader(body)
}

func validJobRequestForCell(t *testing.T, cellID string) *api.JobRequest {
	t.Helper()

	return validJobRequestForCellAttempt(t, cellID, 1)
}

func validJobRequestForCellAttempt(t *testing.T, cellID string, attempt int) *api.JobRequest {
	t.Helper()

	jobID := "job-1"
	runID := "run-1"
	action := "builtins/shell"
	req := &api.JobRequest{
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Uses: &action,
				With: map[string]string{"command": "echo hi"},
			},
		},
	}

	if _, err := cell.AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             runID,
		JobID:             jobID,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            cellID,
		Attempt:           attempt,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		RunIndex:          5,
	}, 123); err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	return req
}

func tlsStateWithURI(t *testing.T, raw string) *tls.ConnectionState {
	t.Helper()

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URI %q: %v", raw, err)
	}

	return &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{
			{URIs: []*url.URL{u}},
		},
	}
}

func assertErrorCode(t *testing.T, rr *httptest.ResponseRecorder, status int, code string) {
	t.Helper()

	if rr.Code != status {
		t.Fatalf("status: got %d, want %d; body=%s", rr.Code, status, rr.Body.String())
	}

	var resp errorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}

	if resp.Code != code {
		t.Fatalf("error code: got %q, want %q; body=%s", resp.Code, code, rr.Body.String())
	}
}

func assertNoStore(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()

	if got := rr.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}

	if got := rr.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma = %q, want no-cache", got)
	}

	if got := rr.Header().Get("Expires"); got != "0" {
		t.Fatalf("Expires = %q, want 0", got)
	}
}

func assertSecurityHeader(t *testing.T, rr *httptest.ResponseRecorder, key, want string) {
	t.Helper()

	if got := rr.Header().Get(key); got != want {
		t.Fatalf("%s = %q, want %q", key, got, want)
	}
}

type recordingAcceptanceStore struct {
	acceptance          dal.CellExecutionAcceptance
	err                 error
	enqueuedExecutionID string
	failedExecutionID   string
	failedMessage       string
}

func (s *recordingAcceptanceStore) AcceptExecution(ctx context.Context, acceptance dal.CellExecutionAcceptance) (bool, error) {
	if s.err != nil {
		return false, s.err
	}

	s.acceptance = acceptance
	return true, nil
}

func (s *recordingAcceptanceStore) snapshot() dal.CellExecutionAcceptance {
	return s.acceptance
}

func (s *recordingAcceptanceStore) ListPendingQueueHandoffs(ctx context.Context, cutoffUnixNano int64, limit int) ([]dal.CellExecutionQueueHandoff, error) {
	return nil, nil
}

func (s *recordingAcceptanceStore) MarkEnqueued(ctx context.Context, executionID string, enqueuedAtUnixNano int64) error {
	s.enqueuedExecutionID = executionID
	return nil
}

func (s *recordingAcceptanceStore) MarkEnqueueFailed(ctx context.Context, executionID string, attemptedAtUnixNano int64, message string) error {
	s.failedExecutionID = executionID
	s.failedMessage = message
	return nil
}
