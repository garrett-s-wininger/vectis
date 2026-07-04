package localpki

import (
	"testing"
)

func TestLoadRequiresInitializedMaterial(t *testing.T) {
	if _, err := Load(t.TempDir()); err == nil {
		t.Fatal("expected missing material error")
	}
}

func TestLoadReturnsInitializedMaterial(t *testing.T) {
	dir := t.TempDir()
	m, err := Ensure(dir)
	if err != nil {
		t.Fatal(err)
	}

	loaded, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	if *loaded != *m {
		t.Fatalf("loaded material = %+v, want %+v", loaded, m)
	}
}
