package spire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	spiretypes "github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"vectis/internal/workloadidentity"
)

type fakeSPIREEntryClient struct {
	createReqs []*entryv1.BatchCreateEntryRequest
	updateReqs []*entryv1.BatchUpdateEntryRequest
	deleteReqs []*entryv1.BatchDeleteEntryRequest
	createResp *entryv1.BatchCreateEntryResponse
	updateResp *entryv1.BatchUpdateEntryResponse
	deleteResp *entryv1.BatchDeleteEntryResponse
	createErr  error
	updateErr  error
	deleteErr  error
}

func (c *fakeSPIREEntryClient) BatchCreateEntry(_ context.Context, req *entryv1.BatchCreateEntryRequest, _ ...grpc.CallOption) (*entryv1.BatchCreateEntryResponse, error) {
	c.createReqs = append(c.createReqs, req)
	if c.createErr != nil {
		return nil, c.createErr
	}
	return c.createResp, nil
}

func (c *fakeSPIREEntryClient) BatchUpdateEntry(_ context.Context, req *entryv1.BatchUpdateEntryRequest, _ ...grpc.CallOption) (*entryv1.BatchUpdateEntryResponse, error) {
	c.updateReqs = append(c.updateReqs, req)
	if c.updateErr != nil {
		return nil, c.updateErr
	}
	return c.updateResp, nil
}

func (c *fakeSPIREEntryClient) BatchDeleteEntry(_ context.Context, req *entryv1.BatchDeleteEntryRequest, _ ...grpc.CallOption) (*entryv1.BatchDeleteEntryResponse, error) {
	c.deleteReqs = append(c.deleteReqs, req)
	if c.deleteErr != nil {
		return nil, c.deleteErr
	}
	return c.deleteResp, nil
}

func TestSPIREServerRegistrarCreatesEntry(t *testing.T) {
	intent := testServerRegistrarIntent(t)
	client := &fakeSPIREEntryClient{
		createResp: &entryv1.BatchCreateEntryResponse{Results: []*entryv1.BatchCreateEntryResponse_Result{{
			Status: status(codes.OK, ""),
			Entry: &spiretypes.Entry{
				Id:        "entry-1",
				ExpiresAt: intent.ExpiresAt.Unix(),
				Hint:      registrationHint(intent),
			},
		}}},
	}
	registrar, err := NewSPIREServerRegistrar(client, WithSPIREServerX509SVIDTTL(30*time.Minute))
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	result, err := registrar.EnsureRegistration(context.Background(), intent)
	if err != nil {
		t.Fatalf("EnsureRegistration: %v", err)
	}

	if !result.Created || !result.Handle.Managed || result.Handle.EntryID != "entry-1" {
		t.Fatalf("result = %+v, want created managed entry-1", result)
	}
	if result.Handle.Key != intent.Key || result.Handle.SPIFFEID != intent.SPIFFEID {
		t.Fatalf("handle = %+v, intent = %+v", result.Handle, intent)
	}

	if len(client.createReqs) != 1 {
		t.Fatalf("create requests = %d, want 1", len(client.createReqs))
	}
	entry := client.createReqs[0].GetEntries()[0]
	if entry.GetSpiffeId().GetTrustDomain() != "prod.example" ||
		entry.GetSpiffeId().GetPath() != "/cell/cell-a/namespace/teams/build/job/job-1/run/run-1/execution/execution-1" {
		t.Fatalf("entry SPIFFE ID = %+v", entry.GetSpiffeId())
	}
	if entry.GetParentId().GetTrustDomain() != "prod.example" ||
		entry.GetParentId().GetPath() != "/spire/agent/worker-node" {
		t.Fatalf("entry parent ID = %+v", entry.GetParentId())
	}
	if got := entry.GetSelectors(); len(got) != 2 ||
		got[0].GetType() != "k8s" || got[0].GetValue() != "sa:vectis:worker" ||
		got[1].GetType() != "unix" || got[1].GetValue() != "uid:1000" {
		t.Fatalf("entry selectors = %+v", got)
	}
	if entry.GetX509SvidTtl() != int32((30 * time.Minute).Seconds()) {
		t.Fatalf("entry X509SvidTtl = %d", entry.GetX509SvidTtl())
	}
	if entry.GetHint() != registrationHint(intent) {
		t.Fatalf("entry hint = %q, want %q", entry.GetHint(), registrationHint(intent))
	}
	if len(client.updateReqs) != 0 {
		t.Fatalf("update requests = %d, want 0", len(client.updateReqs))
	}
}

func TestSPIREServerRegistrarUpdatesManagedExistingEntry(t *testing.T) {
	intent := testServerRegistrarIntent(t)
	existing := &spiretypes.Entry{
		Id:        "entry-1",
		ExpiresAt: intent.ExpiresAt.Add(-time.Minute).Unix(),
		Hint:      registrationHint(intent),
	}
	client := &fakeSPIREEntryClient{
		createResp: &entryv1.BatchCreateEntryResponse{Results: []*entryv1.BatchCreateEntryResponse_Result{{
			Status: status(codes.AlreadyExists, ""),
			Entry:  existing,
		}}},
		updateResp: &entryv1.BatchUpdateEntryResponse{Results: []*entryv1.BatchUpdateEntryResponse_Result{{
			Status: status(codes.OK, ""),
			Entry: &spiretypes.Entry{
				Id:        "entry-1",
				ExpiresAt: intent.ExpiresAt.Unix(),
				Hint:      registrationHint(intent),
			},
		}}},
	}
	registrar, err := NewSPIREServerRegistrar(client)
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	result, err := registrar.EnsureRegistration(context.Background(), intent)
	if err != nil {
		t.Fatalf("EnsureRegistration: %v", err)
	}

	if result.Created || !result.Handle.Managed || result.Handle.EntryID != "entry-1" {
		t.Fatalf("result = %+v, want existing managed entry-1", result)
	}
	if len(client.updateReqs) != 1 {
		t.Fatalf("update requests = %d, want 1", len(client.updateReqs))
	}

	update := client.updateReqs[0]
	if update.GetEntries()[0].GetId() != "entry-1" {
		t.Fatalf("update entry id = %q, want entry-1", update.GetEntries()[0].GetId())
	}
	if !update.GetInputMask().GetExpiresAt() || !update.GetInputMask().GetHint() {
		t.Fatalf("update mask = %+v, want expires_at and hint", update.GetInputMask())
	}
	if update.GetEntries()[0].GetExpiresAt() != intent.ExpiresAt.Unix() {
		t.Fatalf("update expires_at = %d, want %d", update.GetEntries()[0].GetExpiresAt(), intent.ExpiresAt.Unix())
	}
}

func TestSPIREServerRegistrarDoesNotUpdateOrDeleteOperatorManagedExistingEntry(t *testing.T) {
	intent := testServerRegistrarIntent(t)
	client := &fakeSPIREEntryClient{
		createResp: &entryv1.BatchCreateEntryResponse{Results: []*entryv1.BatchCreateEntryResponse_Result{{
			Status: status(codes.AlreadyExists, ""),
			Entry: &spiretypes.Entry{
				Id:        "entry-operator",
				ExpiresAt: intent.ExpiresAt.Add(time.Hour).Unix(),
				Hint:      "operator-managed",
			},
		}}},
		deleteResp: &entryv1.BatchDeleteEntryResponse{Results: []*entryv1.BatchDeleteEntryResponse_Result{{
			Status: status(codes.OK, ""),
			Id:     "entry-operator",
		}}},
	}
	registrar, err := NewSPIREServerRegistrar(client)
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	result, err := registrar.EnsureRegistration(context.Background(), intent)
	if err != nil {
		t.Fatalf("EnsureRegistration: %v", err)
	}

	if result.Created || result.Handle.Managed || result.Handle.EntryID != "entry-operator" {
		t.Fatalf("result = %+v, want existing unmanaged entry-operator", result)
	}
	if len(client.updateReqs) != 0 {
		t.Fatalf("update requests = %d, want 0", len(client.updateReqs))
	}

	if err := registrar.ReleaseRegistration(context.Background(), result.Handle); err != nil {
		t.Fatalf("ReleaseRegistration unmanaged: %v", err)
	}
	if len(client.deleteReqs) != 0 {
		t.Fatalf("delete requests = %d, want 0", len(client.deleteReqs))
	}
}

func TestSPIREServerRegistrarReleaseDeletesManagedEntry(t *testing.T) {
	client := &fakeSPIREEntryClient{
		deleteResp: &entryv1.BatchDeleteEntryResponse{Results: []*entryv1.BatchDeleteEntryResponse_Result{{
			Status: status(codes.OK, ""),
			Id:     "entry-1",
		}}},
	}
	registrar, err := NewSPIREServerRegistrar(client)
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	err = registrar.ReleaseRegistration(context.Background(), RegistrationHandle{
		EntryID: "entry-1",
		Managed: true,
	})
	if err != nil {
		t.Fatalf("ReleaseRegistration: %v", err)
	}
	if len(client.deleteReqs) != 1 || client.deleteReqs[0].GetIds()[0] != "entry-1" {
		t.Fatalf("delete requests = %+v, want entry-1", client.deleteReqs)
	}
}

func TestSPIREServerRegistrarReleaseTreatsNotFoundAsSuccess(t *testing.T) {
	client := &fakeSPIREEntryClient{
		deleteResp: &entryv1.BatchDeleteEntryResponse{Results: []*entryv1.BatchDeleteEntryResponse_Result{{
			Status: status(codes.NotFound, ""),
			Id:     "entry-1",
		}}},
	}
	registrar, err := NewSPIREServerRegistrar(client)
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	if err := registrar.ReleaseRegistration(context.Background(), RegistrationHandle{EntryID: "entry-1", Managed: true}); err != nil {
		t.Fatalf("ReleaseRegistration: %v", err)
	}
}

func TestSPIREServerRegistrarValidationErrors(t *testing.T) {
	if _, err := NewSPIREServerRegistrar(nil); err == nil || !strings.Contains(err.Error(), "client is required") {
		t.Fatalf("NewSPIREServerRegistrar nil error = %v, want client requirement", err)
	}
	if _, err := NewSPIREServerRegistrar(&fakeSPIREEntryClient{}, WithSPIREServerX509SVIDTTL(time.Millisecond)); err == nil || !strings.Contains(err.Error(), "at least 1s") {
		t.Fatalf("NewSPIREServerRegistrar ttl error = %v, want minimum", err)
	}
	if err := ValidateServerAPIAddress("tcp://127.0.0.1:8081"); err == nil || !strings.Contains(err.Error(), "unix://") {
		t.Fatalf("ValidateServerAPIAddress tcp error = %v, want unix requirement", err)
	}
	if err := ValidateServerAPIAddress("unix:///run/spire/server.sock"); err != nil {
		t.Fatalf("ValidateServerAPIAddress unix: %v", err)
	}
	if _, err := ParseSelector("unix:uid:1000"); err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	if _, err := ParseSelector("missing-colon"); err == nil || !strings.Contains(err.Error(), "type:value") {
		t.Fatalf("ParseSelector missing colon error = %v, want type:value", err)
	}
}

func TestSPIREServerRegistrarPropagatesAPIErrors(t *testing.T) {
	intent := testServerRegistrarIntent(t)
	client := &fakeSPIREEntryClient{createErr: errors.New("server unavailable")}
	registrar, err := NewSPIREServerRegistrar(client)
	if err != nil {
		t.Fatalf("NewSPIREServerRegistrar: %v", err)
	}

	_, err = registrar.EnsureRegistration(context.Background(), intent)
	if err == nil || !strings.Contains(err.Error(), "server unavailable") {
		t.Fatalf("EnsureRegistration error = %v, want server unavailable", err)
	}
}

func testServerRegistrarIntent(t *testing.T) RegistrationIntent {
	t.Helper()

	execution := workloadidentity.Execution{
		CellID:            "cell-a",
		NamespacePath:     "/teams/build",
		JobID:             "job-1",
		RunID:             "run-1",
		RunIndex:          1,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		Attempt:           1,
		DefinitionVersion: 1,
		DefinitionHash:    "sha256:abc",
	}
	spiffeID, err := workloadidentity.SPIFFEID("prod.example", "", execution)
	if err != nil {
		t.Fatalf("SPIFFEID: %v", err)
	}

	intent, err := NewExecutionRegistrationIntent(spiffeID, execution, ExecutionRegistrationOptions{
		ParentSPIFFEID: "spiffe://prod.example/spire/agent/worker-node",
		Selectors: []Selector{
			{Type: "unix", Value: "uid:1000"},
			{Type: "k8s", Value: "sa:vectis:worker"},
		},
		ExpiresAt: time.Now().Add(5 * time.Minute),
		MinTTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("NewExecutionRegistrationIntent: %v", err)
	}

	return intent
}

func status(code codes.Code, msg string) *spiretypes.Status {
	return &spiretypes.Status{Code: int32(code), Message: msg}
}
