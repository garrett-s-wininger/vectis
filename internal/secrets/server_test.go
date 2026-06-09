package secrets

import (
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func strp(s string) *string { return &s }

func secretDeliveryTypep(t api.SecretDeliveryType) *api.SecretDeliveryType { return &t }

type fakeProvider struct {
	req ResolveRequest
	err error
}

func (p *fakeProvider) ValidateRef(context.Context, Reference) error { return nil }

func (p *fakeProvider) Resolve(_ context.Context, req ResolveRequest) (Bundle, error) {
	p.req = req
	if p.err != nil {
		return Bundle{}, p.err
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

func (a *fakeAuthorizer) AuthorizeResolve(_ context.Context, req ResolveRequest) error {
	a.req = req
	return a.err
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

func TestServerResolveSecretsRejectsUnauthorizedRequest(t *testing.T) {
	t.Parallel()

	server := NewServer(&fakeProvider{}, &fakeAuthorizer{err: errors.New("not this execution")})

	_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), codes.PermissionDenied)
	}
}

func TestServerResolveSecretsMapsProviderErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "not found", err: ErrNotFound, code: codes.NotFound},
		{name: "denied", err: ErrDenied, code: codes.PermissionDenied},
		{name: "internal", err: errors.New("backend unavailable"), code: codes.Internal},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := NewServer(&fakeProvider{err: tt.err}, &fakeAuthorizer{})
			_, err := server.ResolveSecrets(context.Background(), &api.ResolveSecretsRequest{})
			if status.Code(err) != tt.code {
				t.Fatalf("ResolveSecrets code = %v, want %v", status.Code(err), tt.code)
			}
		})
	}
}
