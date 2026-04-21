package api

import (
	"testing"
)

func TestHashAPIToken_deterministic(t *testing.T) {
	t.Parallel()

	a := hashAPIToken("same")
	b := hashAPIToken("same")
	if a != b || len(a) != 64 {
		t.Fatalf("unexpected hash: %q len=%d", a, len(a))
	}

	if hashAPIToken("x") == a {
		t.Fatal("different inputs should differ")
	}
}

func TestBearerToken(t *testing.T) {
	t.Parallel()

	cases := []struct {
		header string
		want   string
		ok     bool
	}{
		{"Bearer abc", "abc", true},
		{"bearer abc", "abc", true},
		{"Bearer  spaced  ", "spaced", true},
		{"Bearer", "", false},
		{"Bearer ", "", false},
		{"Basic x", "", false},
		{"", "", false},
	}

	for _, tc := range cases {
		got, ok := bearerToken(tc.header)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("bearerToken(%q) = (%q, %v) want (%q, %v)", tc.header, got, ok, tc.want, tc.ok)
		}
	}
}
