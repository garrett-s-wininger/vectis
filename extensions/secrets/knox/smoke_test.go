package knox

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestRunSmokeAgainstKnoxResponseContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "0m-test-token" {
			writeKnoxErrorResponse(t, w, knoxResponseCodeDenied)
			return
		}

		switch r.URL.EscapedPath() {
		case "/v0/keys/team:smoke_token/":
			writeKnoxKeyResponse(t, w, "team:smoke_token", []knoxTestVersion{{ID: 2, Data: []byte("knox-smoke-secret"), Status: "Primary"}})
		case "/v0/keys/team:missing_token/":
			writeKnoxErrorResponse(t, w, knoxResponseCodeNoKey)
		default:
			writeKnoxErrorResponse(t, w, knoxResponseCodeNotFound)
		}
	}))
	defer server.Close()

	var out bytes.Buffer
	result, err := RunSmoke(context.Background(), SmokeOptions{
		URL:            server.URL,
		AuthToken:      "0m-test-token",
		Ref:            "knox://team/smoke_token",
		ID:             "smoke-token",
		Path:           "smoke/token",
		ExpectedData:   "knox-smoke-secret",
		WrongAuthToken: "bad-token",
		MissingRef:     "knox://team/missing_token",
		Stdout:         &out,
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if result.Status != "ok" ||
		result.Ref != "knox://team/smoke_token" ||
		result.ID != "smoke-token" ||
		result.Path != "smoke/token" ||
		result.Bytes != len("knox-smoke-secret") ||
		!result.WrongTokenDenied ||
		!result.MissingRefDenied {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRunSmokeRequiresExpectedDigest(t *testing.T) {
	_, err := RunSmoke(context.Background(), SmokeOptions{URL: "https://knox.example", AuthToken: "token", Ref: "knox://team/token"})
	if err == nil || !strings.Contains(err.Error(), "expected sha256") {
		t.Fatalf("RunSmoke error = %v, want expected sha256 required", err)
	}
}

func TestKnoxSmokeServerTemplateExposesCertTTL(t *testing.T) {
	b, err := os.ReadFile("../../../deploy/knox/smoke-server/main.go.tmpl")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		"defaultCertTTL",
		"168 * time.Hour",
		"envDuration(\"KNOX_SMOKE_CERT_TTL\", defaultCertTTL)",
		"notAfter := time.Now().Add(ttl)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Knox smoke server template missing %q", want)
		}
	}
}
