package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	encryptedfs "vectis/extensions/secrets/encryptedfs"
	sdksecrets "vectis/sdk/secrets"
)

func TestSecretEncryptedFSPutWritesEncryptedEnvelope(t *testing.T) {
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	keyFile := filepath.Join(t.TempDir(), "encryptedfs.key")

	var out bytes.Buffer
	if err := secretEncryptedFSPut(
		"encryptedfs://team/npm-token",
		root,
		keyFile,
		"-",
		true,
		false,
		strings.NewReader("secret-value"),
		&out,
	); err != nil {
		t.Fatalf("secretEncryptedFSPut: %v", err)
	}

	var result secretEncryptedFSPutResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}

	if result.Status != "ok" || result.Ref != "encryptedfs://team/npm-token" || !result.CreatedKey || result.Bytes != len("secret-value") {
		t.Fatalf("unexpected result: %+v", result)
	}

	onDisk, err := os.ReadFile(filepath.Join(root, "team", "npm-token"))
	if err != nil {
		t.Fatalf("read encrypted secret file: %v", err)
	}

	if strings.Contains(string(onDisk), "secret-value") {
		t.Fatalf("encrypted secret file contains plaintext: %s", onDisk)
	}

	key, err := encryptedfs.LoadEncryptedFSKeyFile(keyFile)
	if err != nil {
		t.Fatalf("LoadEncryptedFSKeyFile: %v", err)
	}

	provider, err := encryptedfs.NewEncryptedFSProvider(root, encryptedfs.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	bundle, err := provider.Resolve(context.Background(), sdksecrets.ResolveRequest{
		Secrets: []sdksecrets.Reference{{
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

	if len(bundle.Files) != 1 || string(bundle.Files[0].Data) != "secret-value" {
		t.Fatalf("unexpected bundle: %+v", bundle)
	}
}

func TestSecretEncryptedFSPutRefusesOverwriteUnlessForced(t *testing.T) {
	root := t.TempDir()
	keyFile := filepath.Join(t.TempDir(), "encryptedfs.key")

	if _, err := encryptedfs.EnsureEncryptedFSKeyFile(keyFile); err != nil {
		t.Fatalf("EnsureEncryptedFSKeyFile: %v", err)
	}

	if err := secretEncryptedFSPut(
		"encryptedfs://team/deploy-token",
		root,
		keyFile,
		"-",
		false,
		false,
		strings.NewReader("first"),
		io.Discard,
	); err != nil {
		t.Fatalf("initial put: %v", err)
	}

	err := secretEncryptedFSPut(
		"encryptedfs://team/deploy-token",
		root,
		keyFile,
		"-",
		false,
		false,
		strings.NewReader("second"),
		io.Discard,
	)

	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("overwrite error = %v, want os.ErrExist", err)
	}

	if err := secretEncryptedFSPut(
		"encryptedfs://team/deploy-token",
		root,
		keyFile,
		"-",
		false,
		true,
		strings.NewReader("second"),
		io.Discard,
	); err != nil {
		t.Fatalf("forced put: %v", err)
	}

	key, err := encryptedfs.LoadEncryptedFSKeyFile(keyFile)
	if err != nil {
		t.Fatalf("LoadEncryptedFSKeyFile: %v", err)
	}

	provider, err := encryptedfs.NewEncryptedFSProvider(root, encryptedfs.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	bundle, err := provider.Resolve(context.Background(), sdksecrets.ResolveRequest{
		Secrets: []sdksecrets.Reference{{
			ID:  "deploy-token",
			Ref: "encryptedfs://team/deploy-token",
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: "deploy/token",
			},
		}},
	})

	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(bundle.Files) != 1 || string(bundle.Files[0].Data) != "second" {
		t.Fatalf("unexpected bundle after force: %+v", bundle)
	}
}
