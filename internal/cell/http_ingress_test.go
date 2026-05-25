package cell

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestHTTPExecutionIngressSubmitsToCellIngress(t *testing.T) {
	got := make(chan *api.JobRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: got %s, want POST", r.Method)
		}

		if r.URL.Path != "/cell/v1/executions" {
			t.Fatalf("path: got %s, want /cell/v1/executions", r.URL.Path)
		}

		if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("content-type: got %q, want application/json", ct)
		}

		var body httpExecutionRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}

		var req api.JobRequest
		if err := protojson.Unmarshal(body.JobRequest, &req); err != nil {
			t.Fatalf("decode job_request: %v", err)
		}

		got <- &req
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	req := validJobRequestForCell(t, "iad-a")
	submission, err := NewExecutionSubmission(req)
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	ingress := NewHTTPExecutionIngress(server.URL, server.Client(), nil)
	if err := ingress.SubmitExecution(t.Context(), submission); err != nil {
		t.Fatalf("SubmitExecution: %v", err)
	}

	posted := <-got
	if posted.GetJob().GetRunId() != req.GetJob().GetRunId() {
		t.Fatalf("posted run id: got %q, want %q", posted.GetJob().GetRunId(), req.GetJob().GetRunId())
	}

	env, ok, err := ExecutionEnvelopeFromRequest(posted)
	if err != nil {
		t.Fatalf("ExecutionEnvelopeFromRequest: %v", err)
	}
	if !ok {
		t.Fatal("expected posted request to include execution envelope")
	}
	if env.CellID != "iad-a" {
		t.Fatalf("posted envelope cell: got %q, want iad-a", env.CellID)
	}
}

func TestHTTPExecutionIngressReturnsNonSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"wrong_cell"}`))
	}))
	defer server.Close()

	submission, err := NewExecutionSubmission(validJobRequestForCell(t, "iad-a"))
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	ingress := NewHTTPExecutionIngress(server.URL, server.Client(), nil)
	err = ingress.SubmitExecution(t.Context(), submission)
	if err == nil {
		t.Fatal("SubmitExecution succeeded, want error")
	}

	if !strings.Contains(err.Error(), "409 Conflict") {
		t.Fatalf("error %q does not include status", err.Error())
	}
}

func TestHTTPExecutionIngressRejectsInvalidEndpoint(t *testing.T) {
	submission, err := NewExecutionSubmission(validJobRequestForCell(t, "iad-a"))
	if err != nil {
		t.Fatalf("NewExecutionSubmission: %v", err)
	}

	ingress := NewHTTPExecutionIngress("grpc://cell-ingress", nil, nil)
	err = ingress.SubmitExecution(t.Context(), submission)
	if err == nil {
		t.Fatal("SubmitExecution succeeded, want error")
	}

	if !strings.Contains(err.Error(), "http or https") {
		t.Fatalf("error %q does not explain endpoint scheme", err.Error())
	}
}
