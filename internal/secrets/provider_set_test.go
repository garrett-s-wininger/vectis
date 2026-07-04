package secrets

import (
	"context"
	"errors"
	"testing"
)

type recordingProvider struct {
	kind string
	reqs []ResolveRequest
	err  error
}

func (p *recordingProvider) ProviderKind() string { return p.kind }

func (p *recordingProvider) ValidateRef(context.Context, Reference) error {
	if p.err != nil {
		return p.err
	}

	return nil
}

func (p *recordingProvider) Resolve(_ context.Context, req ResolveRequest) (Bundle, error) {
	p.reqs = append(p.reqs, req)
	if p.err != nil {
		return Bundle{}, p.err
	}

	ref := req.Secrets[0]
	return Bundle{Files: []FileMaterial{{
		ID:   ref.ID,
		Path: ref.Delivery.Path,
		Data: []byte(p.kind + ":" + ref.ID),
		Mode: DefaultFileMode,
	}}}, nil
}

func TestProviderSetResolveRoutesByScheme(t *testing.T) {
	t.Parallel()

	encryptedfs := &recordingProvider{kind: "encryptedfs"}
	vault := &recordingProvider{kind: "vault"}
	set := NewProviderSet()
	if err := set.Register("encryptedfs", encryptedfs); err != nil {
		t.Fatalf("register encryptedfs: %v", err)
	}

	if err := set.Register("vault", vault); err != nil {
		t.Fatalf("register vault: %v", err)
	}

	bundle, err := set.Resolve(context.Background(), ResolveRequest{
		RunID: "run-1",
		Secrets: []Reference{
			{ID: "one", Ref: "encryptedfs://team/one", Delivery: Delivery{Type: DeliveryTypeFile, Path: "one"}},
			{ID: "two", Ref: "vault://kv/team/two", Delivery: Delivery{Type: DeliveryTypeFile, Path: "two"}},
			{ID: "three", Ref: "encryptedfs://team/three", Delivery: Delivery{Type: DeliveryTypeFile, Path: "three"}},
		},
	})

	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(encryptedfs.reqs) != 2 || encryptedfs.reqs[0].Secrets[0].ID != "one" || encryptedfs.reqs[1].Secrets[0].ID != "three" {
		t.Fatalf("encryptedfs requests = %+v", encryptedfs.reqs)
	}

	if len(vault.reqs) != 1 || vault.reqs[0].Secrets[0].ID != "two" {
		t.Fatalf("vault requests = %+v", vault.reqs)
	}

	if len(bundle.Files) != 3 {
		t.Fatalf("files = %+v, want 3", bundle.Files)
	}

	got := []string{string(bundle.Files[0].Data), string(bundle.Files[1].Data), string(bundle.Files[2].Data)}
	want := []string{"encryptedfs:one", "vault:two", "encryptedfs:three"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("file data = %v, want %v", got, want)
		}
	}
}

func TestProviderSetRejectsUnknownScheme(t *testing.T) {
	t.Parallel()

	set := NewProviderSet()
	if err := set.Register("encryptedfs", &recordingProvider{kind: "encryptedfs"}); err != nil {
		t.Fatalf("register encryptedfs: %v", err)
	}

	_, err := set.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{ID: "vault-token", Ref: "vault://kv/team/token"}},
	})

	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve error = %v, want ErrNotFound", err)
	}
}

func TestProviderSetRegisterValidation(t *testing.T) {
	t.Parallel()

	set := NewProviderSet()
	if err := set.Register("", &recordingProvider{kind: "empty"}); err == nil {
		t.Fatal("expected empty scheme error")
	}

	if err := set.Register("vault", nil); err == nil {
		t.Fatal("expected nil provider error")
	}

	if err := set.Register("Vault", &recordingProvider{kind: "vault"}); err != nil {
		t.Fatalf("register vault: %v", err)
	}

	if err := set.Register("vault", &recordingProvider{kind: "vault2"}); err == nil {
		t.Fatal("expected duplicate provider error")
	}
}

func TestProviderSetProviderKindForRefs(t *testing.T) {
	t.Parallel()

	set := NewProviderSet()
	if err := set.Register("encryptedfs", &recordingProvider{kind: "encryptedfs"}); err != nil {
		t.Fatalf("register encryptedfs: %v", err)
	}

	if err := set.Register("vault", &recordingProvider{kind: "vault"}); err != nil {
		t.Fatalf("register vault: %v", err)
	}

	tests := []struct {
		name string
		refs []Reference
		want string
	}{
		{name: "empty", want: providerKindMulti},
		{name: "single", refs: []Reference{{Ref: "encryptedfs://team/token"}}, want: "encryptedfs"},
		{name: "same", refs: []Reference{{Ref: "vault://kv/a"}, {Ref: "vault://kv/b"}}, want: "vault"},
		{name: "mixed", refs: []Reference{{Ref: "encryptedfs://team/token"}, {Ref: "vault://kv/a"}}, want: "mixed"},
		{name: "unknown", refs: []Reference{{Ref: "knox://team/token"}}, want: providerKindUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := set.ProviderKindForRefs(tt.refs); got != tt.want {
				t.Fatalf("ProviderKindForRefs = %q, want %q", got, tt.want)
			}
		})
	}
}
