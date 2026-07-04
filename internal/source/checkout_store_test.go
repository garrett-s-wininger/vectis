package source

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckoutStorePathStableAndSafe(t *testing.T) {
	root := t.TempDir()
	store, err := NewCheckoutStore(root)
	if err != nil {
		t.Fatalf("NewCheckoutStore: %v", err)
	}

	first, err := store.Path("github.com/acme/Big Repo.git")
	if err != nil {
		t.Fatalf("Path: %v", err)
	}

	second, err := store.Path("github.com/acme/Big Repo.git")
	if err != nil {
		t.Fatalf("Path repeat: %v", err)
	}

	if first != second {
		t.Fatalf("path should be stable: got %q then %q", first, second)
	}

	rel, err := filepath.Rel(root, first)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		t.Fatalf("path should be under root: root=%q path=%q rel=%q", root, first, rel)
	}

	base := filepath.Base(first)
	if !strings.HasPrefix(base, "github.com-acme-big-repo.git-") {
		t.Fatalf("unexpected checkout path base: %q", base)
	}

	other, err := store.Path("github.com/acme/Big Repo.git#other")
	if err != nil {
		t.Fatalf("Path other: %v", err)
	}
	if first == other {
		t.Fatalf("different repository IDs should not share a checkout path: %q", first)
	}
}

func TestCheckoutStoreRejectsInvalidInput(t *testing.T) {
	if _, err := NewCheckoutStore(""); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("empty root: expected ErrInvalidReference, got %v", err)
	}

	if _, err := NewCheckoutStore("relative"); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("relative root: expected ErrInvalidReference, got %v", err)
	}

	store, err := NewCheckoutStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewCheckoutStore: %v", err)
	}

	if _, err := store.Path(""); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("empty repository ID: expected ErrInvalidReference, got %v", err)
	}

	if _, err := store.Path("bad\x00id"); !errors.Is(err, ErrInvalidReference) {
		t.Fatalf("NUL repository ID: expected ErrInvalidReference, got %v", err)
	}
}
