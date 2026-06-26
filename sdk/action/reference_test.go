package action

import (
	"strings"
	"testing"
)

func TestParseReference(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		raw          string
		wantName     string
		wantSelector string
		wantKind     SelectorKind
	}{
		{name: "name only", raw: "acme/cache", wantName: "acme/cache", wantKind: SelectorNone},
		{name: "version selector", raw: "acme/cache@1.2.3", wantName: "acme/cache", wantSelector: "1.2.3", wantKind: SelectorVersion},
		{name: "channel selector", raw: "acme/cache@v1", wantName: "acme/cache", wantSelector: "v1", wantKind: SelectorVersion},
		{name: "digest selector", raw: "acme/cache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantName: "acme/cache", wantSelector: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wantKind: SelectorDigest},
		{name: "trimmed", raw: "  acme/cache@v1  ", wantName: "acme/cache", wantSelector: "v1", wantKind: SelectorVersion},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseReference(tt.raw)
			if err != nil {
				t.Fatalf("ParseReference: %v", err)
			}

			if got.CanonicalName() != tt.wantName || got.Selector != tt.wantSelector || got.SelectorKind != tt.wantKind {
				t.Fatalf("reference mismatch: got %+v, want name=%q selector=%q kind=%q", got, tt.wantName, tt.wantSelector, tt.wantKind)
			}
		})
	}
}

func TestParseReferenceRejectsInvalidShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw  string
		want string
	}{
		{raw: "", want: "is required"},
		{raw: "cache", want: "namespace/name"},
		{raw: "too/many/parts", want: "namespace/name"},
		{raw: "Acme/cache", want: "namespace"},
		{raw: "acme/Cache", want: "name"},
		{raw: "acme/cache@", want: "selector is required"},
		{raw: "acme/cache@v1@v2", want: "at most one selector"},
		{raw: "acme/cache@sha256:bad", want: "selector"},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			t.Parallel()

			_, err := ParseReference(tt.raw)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error for %q, got %v", tt.want, tt.raw, err)
			}
		})
	}
}
