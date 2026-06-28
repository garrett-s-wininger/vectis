package secrets

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func strp(s string) *string { return &s }

func secretDeliveryTypep(t api.SecretDeliveryType) *api.SecretDeliveryType { return &t }

type fakeProvider struct {
	req    ResolveRequest
	bundle *Bundle
	err    error
}

func (p *fakeProvider) ValidateRef(context.Context, Reference) error { return nil }

func (p *fakeProvider) ProviderKind() string { return "fake" }

func (p *fakeProvider) Resolve(_ context.Context, req ResolveRequest) (Bundle, error) {
	p.req = req
	if p.err != nil {
		return Bundle{}, p.err
	}

	if p.bundle != nil {
		return *p.bundle, nil
	}

	return Bundle{Files: []FileMaterial{{
		ID:   "npm-token",
		Path: "npm/token",
		Data: []byte("secret-value"),
		Mode: DefaultFileMode,
	}}}, nil
}

type fakeAuthorizer struct {
	req ResolveRequest
	err error
}

func (a *fakeAuthorizer) AuthorizeResolve(_ context.Context, req *ResolveRequest) error {
	if req != nil {
		a.req = *req
	}
	return a.err
}

type resolveMetricRecord struct {
	outcome  string
	reason   string
	provider string
	duration time.Duration
}

type recordingResolveMetrics struct {
	records []resolveMetricRecord
}

func (m *recordingResolveMetrics) RecordResolve(_ context.Context, outcome, reason, provider string, duration time.Duration) {
	m.records = append(m.records, resolveMetricRecord{
		outcome:  outcome,
		reason:   reason,
		provider: provider,
		duration: duration,
	})
}

func TestServerResolveSecretsAdaptsProviderResult(t *testing.T) {
	t.Parallel()

	provider := &fakeProvider{}
	authorizer := &fakeAuthorizer{}
	server := NewServer(provider, authorizer)

	resp, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{
		RunId:               strp("run-1"),
		ExecutionId:         strp("execution-1"),
		ExecutionClaimToken: strp("claim-1"),
		Secrets: []*api.SecretReference{{
			Id:  strp("npm-token"),
			Ref: strp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: secretDeliveryTypep(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE),
				Path: strp("npm/token"),
			},
		}},
	})
	if err != nil {
		t.Fatalf("ResolveSecrets: %v", err)
	}

	if authorizer.req.RunID != "run-1" || authorizer.req.ExecutionClaimToken != "claim-1" {
		t.Fatalf("authorizer request = %+v", authorizer.req)
	}

	if provider.req.ExecutionID != "execution-1" || len(provider.req.Secrets) != 1 {
		t.Fatalf("provider request = %+v", provider.req)
	}

	files := resp.GetFiles()
	if len(files) != 1 {
		t.Fatalf("response files = %+v, want one file", files)
	}

	if files[0].GetPath() != "npm/token" || string(files[0].GetData()) != "secret-value" {
		t.Fatalf("response file = %+v", files[0])
	}
}

func TestServerResolveSecretsRecordsSuccessMetricsAndSanitizedLog(t *testing.T) {
	t.Parallel()

	provider := &fakeProvider{}
	authorizer := &fakeAuthorizer{}
	metrics := &recordingResolveMetrics{}
	var log bytes.Buffer
	logger := interfaces.NewLogger("secrets-test").WithOutput(&log)
	server := NewServer(provider, authorizer, WithMetrics(metrics), WithLogger(logger))

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{
		RunId:               strp("run-1"),
		ExecutionId:         strp("execution-1"),
		ExecutionClaimToken: strp("claim-secret"),
		Secrets: []*api.SecretReference{{
			Id:  strp("npm-token"),
			Ref: strp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: secretDeliveryTypep(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE),
				Path: strp("npm/token"),
			},
		}},
	})

	if err != nil {
		t.Fatalf("ResolveSecrets: %v", err)
	}

	if len(metrics.records) != 1 {
		t.Fatalf("metrics records = %+v, want one", metrics.records)
	}

	record := metrics.records[0]
	if record.outcome != resolveOutcomeSuccess || record.reason != resolveReasonOK || record.provider != "fake" || record.duration < 0 {
		t.Fatalf("metric record = %+v", record)
	}

	gotLog := log.String()
	for _, want := range []string{"Secret resolve completed", "run_id=run-1", "execution_id=execution-1", "provider=fake", "outcome=success"} {
		if !strings.Contains(gotLog, want) {
			t.Fatalf("log missing %q:\n%s", want, gotLog)
		}
	}

	for _, leak := range []string{"claim-secret", "encryptedfs://team/npm-token", "secret-value", "npm/token"} {
		if strings.Contains(gotLog, leak) {
			t.Fatalf("log contains sensitive value %q:\n%s", leak, gotLog)
		}
	}
}

func TestServerResolveSecretsRejectsInvalidProviderBundle(t *testing.T) {
	t.Parallel()

	metrics := &recordingResolveMetrics{}
	bundle := Bundle{Files: []FileMaterial{{
		ID:   "deploy-token",
		Path: "deploy/token",
		Data: []byte("not-requested"),
		Mode: DefaultFileMode,
	}}}
	server := NewServer(&fakeProvider{bundle: &bundle}, &fakeAuthorizer{}, WithMetrics(metrics))

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{
		RunId:               strp("run-1"),
		ExecutionId:         strp("execution-1"),
		ExecutionClaimToken: strp("claim-1"),
		Secrets: []*api.SecretReference{{
			Id:  strp("npm-token"),
			Ref: strp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: secretDeliveryTypep(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE),
				Path: strp("npm/token"),
			},
		}},
	})

	if status.Code(err) != codes.Internal {
		t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), codes.Internal)
	}

	if len(metrics.records) != 1 {
		t.Fatalf("metrics records = %+v, want one", metrics.records)
	}

	record := metrics.records[0]
	if record.outcome != resolveOutcomeFailed || record.reason != resolveReasonInvalidBundle || record.provider != "fake" {
		t.Fatalf("metric record = %+v", record)
	}
}

func TestServerResolveSecretsRejectsUnauthorizedRequest(t *testing.T) {
	t.Parallel()

	server := NewServer(&fakeProvider{}, &fakeAuthorizer{err: errors.New("not this execution")})

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), codes.PermissionDenied)
	}
}

func TestServerResolveSecretsRecordsAuthorizationDenial(t *testing.T) {
	t.Parallel()

	metrics := &recordingResolveMetrics{}
	server := NewServer(&fakeProvider{}, &fakeAuthorizer{err: errors.New("not this execution")}, WithMetrics(metrics))

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{
		RunId:               strp("run-1"),
		ExecutionId:         strp("execution-1"),
		ExecutionClaimToken: strp("claim-1"),
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), codes.PermissionDenied)
	}

	if len(metrics.records) != 1 {
		t.Fatalf("metrics records = %+v, want one", metrics.records)
	}

	record := metrics.records[0]
	if record.outcome != resolveOutcomeDenied || record.reason != resolveReasonAuthorizationDenied || record.provider != "fake" {
		t.Fatalf("metric record = %+v", record)
	}
}

func TestServerResolveSecretsRecordsProviderSetLabel(t *testing.T) {
	t.Parallel()

	set := NewProviderSet()
	if err := set.Register("encryptedfs", &recordingProvider{kind: "encryptedfs"}); err != nil {
		t.Fatalf("register encryptedfs: %v", err)
	}

	if err := set.Register("vault", &recordingProvider{kind: "vault"}); err != nil {
		t.Fatalf("register vault: %v", err)
	}

	metrics := &recordingResolveMetrics{}
	server := NewServer(set, &fakeAuthorizer{}, WithMetrics(metrics))

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{
		RunId:               strp("run-1"),
		ExecutionId:         strp("execution-1"),
		ExecutionClaimToken: strp("claim-1"),
		Secrets: []*api.SecretReference{
			{
				Id:  strp("npm-token"),
				Ref: strp("encryptedfs://team/npm-token"),
				Delivery: &api.SecretDelivery{
					Type: secretDeliveryTypep(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE),
					Path: strp("npm/token"),
				},
			},
			{
				Id:  strp("deploy-token"),
				Ref: strp("vault://kv/team/deploy-token"),
				Delivery: &api.SecretDelivery{
					Type: secretDeliveryTypep(api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE),
					Path: strp("deploy/token"),
				},
			},
		},
	})

	if err != nil {
		t.Fatalf("ResolveSecrets: %v", err)
	}

	if len(metrics.records) != 1 {
		t.Fatalf("metrics records = %+v, want one", metrics.records)
	}

	if record := metrics.records[0]; record.provider != "mixed" {
		t.Fatalf("provider label = %q, want mixed", record.provider)
	}
}

func TestServerResolveSecretsMapsProviderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		code    codes.Code
		outcome string
		reason  string
	}{
		{name: "not found", err: ErrNotFound, code: codes.NotFound, outcome: resolveOutcomeNotFound, reason: resolveReasonProviderNotFound},
		{name: "denied", err: ErrDenied, code: codes.PermissionDenied, outcome: resolveOutcomeDenied, reason: resolveReasonProviderDenied},
		{name: "internal", err: errors.New("backend unavailable"), code: codes.Internal, outcome: resolveOutcomeFailed, reason: resolveReasonProviderError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			metrics := &recordingResolveMetrics{}
			server := NewServer(&fakeProvider{err: tt.err}, &fakeAuthorizer{}, WithMetrics(metrics))
			_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{})
			if status.Code(err) != tt.code {
				t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), tt.code)
			}

			if len(metrics.records) != 1 {
				t.Fatalf("metrics records = %+v, want one", metrics.records)
			}

			record := metrics.records[0]
			if record.outcome != tt.outcome || record.reason != tt.reason || record.provider != "fake" {
				t.Fatalf("metric record = %+v, want outcome=%s reason=%s provider=fake", record, tt.outcome, tt.reason)
			}
		})
	}
}
