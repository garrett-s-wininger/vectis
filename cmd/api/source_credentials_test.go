package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/viper"
	encryptedfs "vectis/extensions/secrets/encryptedfs"
)

func TestConfiguredSourceCredentialReferences(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	keyPath := filepath.Join(t.TempDir(), "key")
	key, err := encryptedfs.EnsureEncryptedFSKeyFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	viper.Set(encryptedfs.ConfigKeyRoot, root)
	viper.Set(encryptedfs.ConfigKeyKeyFile, keyPath)
	for ref, value := range map[string]string{"encryptedfs://git/token": "private-token-value", "encryptedfs://git/invalid": `{}`} {
		if err := encryptedfs.WriteEncryptedFSSecretFile(root, ref, []byte(value), key); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(filepath.Join(root, "not-encrypted"), []byte("do-not-expose"), 0o600); err != nil {
		t.Fatal(err)
	}

	resolver, err := newConfiguredSourceRepositoryCredentialResolver(nil)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := configuredSourceCredentialReferences(t.Context(), resolver)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(refs, []string{"encryptedfs://git/token"}) {
		t.Fatalf("unexpected inventory: %v", refs)
	}
}
