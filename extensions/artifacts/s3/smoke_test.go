package s3

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestRunSmokeAgainstFakeS3WithAccessKey(t *testing.T) {
	fake := newFakeS3(t)
	defer fake.server.Close()

	var out bytes.Buffer
	result, err := RunSmoke(context.Background(), SmokeOptions{
		Endpoint:        fake.server.URL,
		Region:          "us-west-2",
		Bucket:          "vectis-artifacts",
		Prefix:          "smoke-test",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "SECRET_TEST",
		PathStyle:       true,
		Payload:         "s3 external smoke",
		Stdout:          &out,
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if result.Status != "ok" || result.Bucket != "vectis-artifacts" || result.Prefix != "smoke-test" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if result.BlobKey == "" || result.Digest != sha256Hex("s3 external smoke") || result.Bytes != int64(len("s3 external smoke")) {
		t.Fatalf("bad blob result: %+v", result)
	}

	if result.Stats.BlobFiles != 1 || result.Stats.BlobBytes != result.Bytes || !result.Stats.NewBlobWritable {
		t.Fatalf("bad stats: %+v", result.Stats)
	}

	if got := fake.methodCount("PUT"); got != 1 {
		t.Fatalf("PUT count = %d, want 1", got)
	}

	if auth := fake.lastHeader("Authorization"); !strings.Contains(auth, "Credential=AKIA_TEST/") {
		t.Fatalf("Authorization header = %q, want AKIA_TEST credential", auth)
	}
}

func TestRunSmokeAgainstFakeS3Unsigned(t *testing.T) {
	fake := newFakeS3(t)
	fake.requireAuthorization = false
	defer fake.server.Close()

	var out bytes.Buffer
	result, err := RunSmoke(context.Background(), SmokeOptions{
		Endpoint:  fake.server.URL,
		Region:    "us-west-2",
		Bucket:    "vectis-artifacts",
		Prefix:    "public-smoke-test",
		PathStyle: true,
		Payload:   "s3 external public smoke",
		Stdout:    &out,
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if result.Status != "ok" || result.Bucket != "vectis-artifacts" || result.Prefix != "public-smoke-test" {
		t.Fatalf("unexpected result: %+v", result)
	}

	if result.BlobKey == "" || result.Digest != sha256Hex("s3 external public smoke") || result.Bytes != int64(len("s3 external public smoke")) {
		t.Fatalf("bad blob result: %+v", result)
	}

	if result.Stats.BlobFiles != 1 || result.Stats.BlobBytes != result.Bytes || !result.Stats.NewBlobWritable {
		t.Fatalf("bad stats: %+v", result.Stats)
	}

	if auth := fake.lastHeader("Authorization"); auth != "" {
		t.Fatalf("unsigned smoke sent Authorization header %q", auth)
	}
}

func TestRunSmokeRequiresEndpoint(t *testing.T) {
	_, err := RunSmoke(context.Background(), SmokeOptions{Bucket: "vectis-artifacts"})
	if err == nil || !strings.Contains(err.Error(), "endpoint is required") {
		t.Fatalf("RunSmoke error = %v, want endpoint required", err)
	}
}

func TestS3SmokeMakefileUsesPinnedRecreatedFixtures(t *testing.T) {
	b, err := os.ReadFile("../../../Makefile")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	if !strings.Contains(text, "S3_SMOKE_IMAGE ?= docker.io/chrislusf/seaweedfs:4.36") {
		t.Fatal("S3 smoke Makefile contract must pin the SeaweedFS image")
	}

	section := s3TextBetween(t, text, ".PHONY: s3-smoke-public-up", "DEPLOY_LINUX_OUT ?=")
	for _, want := range []string{
		"$(CONTAINER_CMD) stop \"$(S3_SMOKE_CONTAINER)\"",
		"$(CONTAINER_CMD) rm \"$(S3_SMOKE_CONTAINER)\"",
		"$(CONTAINER_CMD) stop \"$(S3_SMOKE_AUTH_CONTAINER)\"",
		"$(CONTAINER_CMD) rm \"$(S3_SMOKE_AUTH_CONTAINER)\"",
		"s3-smoke-check: s3-smoke-public-check s3-smoke-auth-check",
		"s3-smoke: s3-smoke-public s3-smoke-auth",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("S3 smoke Makefile contract missing %q", want)
		}
	}

	for _, staleStart := range []string{
		"$(CONTAINER_CMD) start \"$(S3_SMOKE_CONTAINER)\"",
		"$(CONTAINER_CMD) start \"$(S3_SMOKE_AUTH_CONTAINER)\"",
		"seaweedfs:latest",
	} {
		if strings.Contains(section, staleStart) || strings.Contains(text, staleStart) {
			t.Fatalf("S3 smoke Makefile contract must not contain %q", staleStart)
		}
	}
}

func s3TextBetween(t *testing.T, text, start, end string) string {
	t.Helper()

	startIndex := strings.Index(text, start)
	if startIndex < 0 {
		t.Fatalf("missing start marker %q", start)
	}

	endIndex := strings.Index(text[startIndex:], end)
	if endIndex < 0 {
		t.Fatalf("missing end marker %q", end)
	}

	return text[startIndex : startIndex+endIndex]
}
