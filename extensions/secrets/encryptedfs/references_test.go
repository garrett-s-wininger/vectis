package encryptedfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestListReferencesDoesNotFollowSymlinks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, ref := range []string{"encryptedfs://git/a", "encryptedfs://git/with%20space"} {
		if err := WriteEncryptedFSSecretFile(root, ref, []byte("private-token"), testEncryptedFSKey); err != nil {
			t.Fatal(err)
		}
	}

	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(outside, filepath.Join(root, "directory-link")); err != nil {
		t.Fatal(err)
	}

	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "file-link")); err != nil {
		t.Fatal(err)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatal(err)
	}

	refs, err := provider.ListReferences(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(refs, []string{"encryptedfs://git/a", "encryptedfs://git/with%20space"}) {
		t.Fatalf("refs: %v", refs)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if _, err := provider.ListReferences(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
