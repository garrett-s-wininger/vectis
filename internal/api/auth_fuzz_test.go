package api

import (
	"testing"
)

func FuzzBearerToken(f *testing.F) {
	f.Add("")
	f.Add("Bearer ")
	f.Add("Bearer x")
	f.Add("bearer token-value")
	f.Add("Basic abc")

	f.Fuzz(func(t *testing.T, in string) {
		tok, ok := bearerToken(in)
		if ok && tok == "" {
			t.Fatal("ok with empty token")
		}

		if len(tok) > len(in) {
			t.Fatalf("token longer than input: in=%d tok=%d", len(in), len(tok))
		}
	})
}

func FuzzHashAPIToken(f *testing.F) {
	f.Add("")
	f.Add("secret")

	f.Fuzz(func(t *testing.T, in string) {
		h := hashAPIToken(in)
		if len(h) != 64 {
			t.Fatalf("want 64 hex chars, got %d", len(h))
		}

		for i := 0; i < len(h); i++ {
			c := h[i]
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				t.Fatalf("non-hex at %d: %q", i, h)
			}
		}
	})
}
