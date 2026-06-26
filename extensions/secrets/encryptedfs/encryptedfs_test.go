package encryptedfs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sdksecrets "vectis/sdk/secrets"
)

var testEncryptedFSKey = []byte("0123456789abcdef0123456789abcdef")

func TestEncryptedFSProviderResolveReadsEncryptedSecretFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := WriteEncryptedFSSecretFile(root, "encryptedfs://team/npm-token", []byte("secret-value"), testEncryptedFSKey); err != nil {
		t.Fatalf("write encrypted secret: %v", err)
	}

	onDisk, err := os.ReadFile(filepath.Join(root, "team", "npm-token"))
	if err != nil {
		t.Fatalf("read encrypted secret file: %v", err)
	}

	if strings.Contains(string(onDisk), "secret-value") {
		t.Fatalf("encrypted secret file contains plaintext: %s", onDisk)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	bundle, err := provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "npm-token",
			Ref: "encryptedfs://team/npm-token",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "npm/token",
			},
		}},
	})

	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(bundle.Files) != 1 {
		t.Fatalf("files = %+v, want one", bundle.Files)
	}

	file := bundle.Files[0]
	if file.ID != "npm-token" || file.Path != "npm/token" || string(file.Data) != "secret-value" || file.Mode != DefaultFileMode {
		t.Fatalf("file = %+v", file)
	}
}

func TestEncryptedFSProviderResolveRejectsUnsafeRefs(t *testing.T) {
	t.Parallel()

	provider, err := NewEncryptedFSProvider(t.TempDir(), WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	tests := []string{
		"vault://team/npm-token",
		"encryptedfs://team/../npm-token",
		"encryptedfs://team/npm-token?version=1",
		"encryptedfs://user:pass@team/npm-token",
	}

	for _, ref := range tests {
		t.Run(ref, func(t *testing.T) {
			t.Parallel()

			_, err := provider.Resolve(context.Background(), ResolveRequest{
				Secrets: []Reference{{
					ID:  "bad",
					Ref: ref,
					Delivery: sdksecrets.Delivery{
						Type: sdksecrets.DeliveryTypeFile,
						Path: "bad",
					},
				}},
			})

			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("Resolve error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestEncryptedFSProviderResolveRejectsSymlinkEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := t.TempDir()
	if err := WriteEncryptedFSSecretFile(outside, "encryptedfs://token", []byte("secret-value"), testEncryptedFSKey); err != nil {
		t.Fatalf("write outside encrypted secret: %v", err)
	}

	if err := os.Symlink(filepath.Join(outside, "token"), filepath.Join(root, "token")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	_, err = provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "token",
			Ref: "encryptedfs://token",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "token",
			},
		}},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Resolve error = %v, want ErrDenied", err)
	}
}

func TestEncryptedFSProviderResolveRejectsOversizedSecret(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := WriteEncryptedFSSecretFile(root, "encryptedfs://token", []byte("too-large"), testEncryptedFSKey); err != nil {
		t.Fatalf("write encrypted secret: %v", err)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey), WithMaxSecretBytes(3))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	_, err = provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "token",
			Ref: "encryptedfs://token",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "token",
			},
		}},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Resolve error = %v, want ErrDenied", err)
	}
}

func TestEncryptedFSProviderResolveRejectsPlaintextFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "token"), []byte("secret-value"), 0o600); err != nil {
		t.Fatalf("write plaintext secret: %v", err)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	_, err = provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "token",
			Ref: "encryptedfs://token",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "token",
			},
		}},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Resolve error = %v, want ErrDenied", err)
	}
}

func TestEncryptedFSProviderResolveRejectsSwappedEnvelope(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if err := WriteEncryptedFSSecretFile(root, "encryptedfs://team/source", []byte("secret-value"), testEncryptedFSKey); err != nil {
		t.Fatalf("write encrypted secret: %v", err)
	}

	if err := os.Rename(filepath.Join(root, "team", "source"), filepath.Join(root, "team", "target")); err != nil {
		t.Fatalf("rename encrypted secret: %v", err)
	}

	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(testEncryptedFSKey))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	_, err = provider.Resolve(context.Background(), ResolveRequest{
		Secrets: []Reference{{
			ID:  "token",
			Ref: "encryptedfs://team/target",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "token",
			},
		}},
	})

	if !errors.Is(err, ErrDenied) {
		t.Fatalf("Resolve error = %v, want ErrDenied", err)
	}
}

func TestEncryptedFSKeyFileFormats(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	for name, contents := range map[string][]byte{
		"raw":    testEncryptedFSKey,
		"hex":    []byte("3031323334353637383961626364656630313233343536373839616263646566\n"),
		"base64": []byte("MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=\n"),
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write key file %s: %v", name, err)
		}

		key, err := LoadEncryptedFSKeyFile(path)
		if err != nil {
			t.Fatalf("LoadEncryptedFSKeyFile(%s): %v", name, err)
		}

		if string(key) != string(testEncryptedFSKey) {
			t.Fatalf("key %s = %q", name, key)
		}
	}
}
