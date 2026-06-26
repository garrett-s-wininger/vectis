package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKnoxProviderResolveFetchesPrimaryVersion(t *testing.T) {
	t.Parallel()

	var gotPath, gotAuth, gotUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotAuth = r.Header.Get("Authorization")
		gotUserAgent = r.Header.Get("User-Agent")

		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}

		writeKnoxKeyResponse(t, w, "team:deploy_token", []knoxTestVersion{
			{ID: 1, Data: []byte("old"), Status: "Active"},
			{ID: 2, Data: []byte("super-secret"), Status: "Primary"},
		})
	}))
	defer server.Close()

	provider, err := NewKnoxProvider(server.URL, WithKnoxAuthToken("0m-test-token"))
	if err != nil {
		t.Fatalf("NewKnoxProvider: %v", err)
	}

	bundle, err := provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "deploy-token",
			Ref: "knox://team/deploy_token",
			Delivery: Delivery{
				Type: DeliveryTypeFile,
				Path: "deploy/token",
			},
		}},
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if gotPath != "/v0/keys/team:deploy_token/" {
		t.Fatalf("request path = %q, want /v0/keys/team:deploy_token/", gotPath)
	}

	if gotAuth != "0m-test-token" {
		t.Fatalf("Authorization = %q, want token", gotAuth)
	}

	if gotUserAgent != DefaultKnoxUserAgent {
		t.Fatalf("User-Agent = %q, want %q", gotUserAgent, DefaultKnoxUserAgent)
	}

	if len(bundle.Files) != 1 {
		t.Fatalf("files = %+v, want one", bundle.Files)
	}

	file := bundle.Files[0]
	if file.ID != "deploy-token" || file.Path != "deploy/token" || string(file.Data) != "super-secret" || file.Mode != DefaultFileMode {
		t.Fatalf("file = %+v", file)
	}
}

func TestKnoxProviderSupportsRefForms(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"knox://team:deploy_token":   "team:deploy_token",
		"knox://team/deploy_token":   "team:deploy_token",
		"knox:///team:deploy_token":  "team:deploy_token",
		"knox:team:deploy_token":     "team:deploy_token",
		"knox://team%3Adeploy_token": "team:deploy_token",
	}

	for ref, want := range tests {
		ref := ref
		want := want
		t.Run(ref, func(t *testing.T) {
			t.Parallel()

			got, err := knoxKeyIDFromRef(ref)
			if err != nil {
				t.Fatalf("knoxKeyIDFromRef: %v", err)
			}

			if got != want {
				t.Fatalf("key ID = %q, want %q", got, want)
			}
		})
	}
}

func TestKnoxProviderRejectsUnsafeRefs(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"encryptedfs://team/token",
		"knox://user:pass@team/token",
		"knox://team/token?version=1",
		"knox://team/token#fragment",
		"knox://team/deploy-token",
		"knox://team/path/extra",
		"knox://team/%2e%2e",
	}

	for _, ref := range tests {
		ref := ref
		t.Run(ref, func(t *testing.T) {
			t.Parallel()

			_, err := knoxKeyIDFromRef(ref)
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestKnoxProviderMapsKnoxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		code int
		want error
	}{
		{name: "missing", code: knoxResponseCodeNoKey, want: ErrNotFound},
		{name: "not_found", code: knoxResponseCodeNotFound, want: ErrNotFound},
		{name: "unauthenticated", code: knoxResponseCodeNoAuth, want: ErrDenied},
		{name: "unauthorized", code: knoxResponseCodeDenied, want: ErrDenied},
		{name: "internal", code: 1, want: nil},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				resp := map[string]any{
					"status":  "error",
					"code":    tt.code,
					"message": "knox failed",
					"data":    nil,
				}
				if err := json.NewEncoder(w).Encode(resp); err != nil {
					t.Fatalf("encode response: %v", err)
				}
			}))
			defer server.Close()

			provider, err := NewKnoxProvider(server.URL, WithKnoxAuthToken("token"))
			if err != nil {
				t.Fatalf("NewKnoxProvider: %v", err)
			}

			_, err = provider.Resolve(context.Background(), ResolveRequest{
				Secrets: []Reference{{
					ID:  "token",
					Ref: "knox://team/token",
					Delivery: Delivery{
						Type: DeliveryTypeFile,
						Path: "token",
					},
				}},
			})

			if tt.want == nil {
				if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrDenied) {
					t.Fatalf("error = %v, want provider error without sentinel", err)
				}
				return
			}

			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestKnoxProviderRejectsMismatchedOrOversizedSecrets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		keyID    string
		data     []byte
		maxBytes int64
		want     error
	}{
		{name: "mismatched_key", keyID: "other:token", data: []byte("secret"), want: ErrDenied},
		{name: "oversized", keyID: "team:token", data: []byte("secret"), maxBytes: 3, want: ErrDenied},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeKnoxKeyResponse(t, w, tt.keyID, []knoxTestVersion{{ID: 1, Data: tt.data, Status: "Primary"}})
			}))
			defer server.Close()

			provider, err := NewKnoxProvider(server.URL, WithKnoxAuthToken("token"), WithKnoxMaxSecretBytes(tt.maxBytes))
			if err != nil {
				t.Fatalf("NewKnoxProvider: %v", err)
			}

			_, err = provider.Resolve(context.Background(), ResolveRequest{
				Secrets: []Reference{{
					ID:  "token",
					Ref: "knox://team/token",
					Delivery: Delivery{
						Type: DeliveryTypeFile,
						Path: "token",
					},
				}},
			})

			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestKnoxProviderLoadsAuthTokenFile(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "knox-token")
	if err := os.WriteFile(tokenFile, []byte("0u-file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		writeKnoxKeyResponse(t, w, "team:token", []knoxTestVersion{{ID: 1, Data: []byte("secret"), Status: "Primary"}})
	}))
	defer server.Close()

	provider, err := NewKnoxProvider(server.URL, WithKnoxAuthTokenFile(tokenFile))
	if err != nil {
		t.Fatalf("NewKnoxProvider: %v", err)
	}

	if _, err := provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:       "token",
			Ref:      "knox://team/token",
			Delivery: Delivery{Type: DeliveryTypeFile, Path: "token"},
		}},
	}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if gotAuth != "0u-file-token" {
		t.Fatalf("Authorization = %q, want token from file", gotAuth)
	}
}

func TestKnoxProviderPreservesBaseURLPath(t *testing.T) {
	t.Parallel()

	provider, err := NewKnoxProvider("https://knox.example/root/", WithKnoxAuthToken("token"))
	if err != nil {
		t.Fatalf("NewKnoxProvider: %v", err)
	}

	if got := provider.keyURL("team:token"); got != "https://knox.example/root/v0/keys/team:token/" {
		t.Fatalf("keyURL = %q", got)
	}
}

type knoxTestVersion struct {
	ID     uint64
	Data   []byte
	Status string
}

func writeKnoxKeyResponse(t *testing.T, w http.ResponseWriter, keyID string, versions []knoxTestVersion) {
	t.Helper()

	versionRows := make([]map[string]any, 0, len(versions))
	for _, version := range versions {
		versionRows = append(versionRows, map[string]any{
			"id":     version.ID,
			"data":   base64.StdEncoding.EncodeToString(version.Data),
			"status": version.Status,
			"ts":     1,
		})
	}

	resp := map[string]any{
		"status": "ok",
		"code":   0,
		"data": map[string]any{
			"id":       keyID,
			"acl":      []any{},
			"versions": versionRows,
			"hash":     strings.Repeat("a", 64),
		},
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
