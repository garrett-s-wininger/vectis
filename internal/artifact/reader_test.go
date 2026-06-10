package artifact

import (
	"bytes"
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestRoutedReader_ReadsPinnedArtifactBlob(t *testing.T) {
	ctx := context.Background()
	artifactServer, store := startArtifactRouteServer(t)
	content := []byte("hello artifact reader")

	desc, err := store.Put(ctx, bytes.NewReader(content), PutOptions{})
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	reader, err := NewReaderForArtifact(ctx, dal.ArtifactRecord{
		BlobKey:         desc.Key,
		ArtifactShardID: "pinned",
	}, ReaderOptions{
		Logger:        mocks.NewMockLogger(),
		PinnedAddress: artifactServer.Addr(),
	})

	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer reader.Close()

	gotStat, err := reader.StatBlob(ctx, desc.Key)
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}

	assertBlobDescriptor(t, gotStat, desc.Digest, desc.Size)

	var got bytes.Buffer
	gotDesc, err := reader.WriteBlob(ctx, desc.Key, &got)
	if err != nil {
		t.Fatalf("write blob: %v", err)
	}

	assertBlobDescriptor(t, gotDesc, desc.Digest, desc.Size)
	if !bytes.Equal(got.Bytes(), content) {
		t.Fatalf("downloaded data = %q, want %q", got.Bytes(), content)
	}
}

func TestRoutedReader_ResolvesArtifactShardFromRegistry(t *testing.T) {
	ctx := context.Background()
	logger := mocks.NewMockLogger()
	registryServer := startArtifactRouteRegistry(t, logger)
	artifactServer, store := startArtifactRouteServer(t)
	content := []byte("hello registry artifact reader")

	desc, err := store.Put(ctx, bytes.NewReader(content), PutOptions{})
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	reg := newArtifactRouteRegistryClient(t, registryServer.Addr(), logger)
	registerArtifactRouteEndpoint(t, reg, "artifact-1", artifactServer.Addr(), "iad-a", true)

	reader, err := NewReaderForArtifact(ctx, dal.ArtifactRecord{
		BlobKey:         desc.Key,
		ArtifactShardID: "artifact-1",
	}, ReaderOptions{
		Logger:          logger,
		RegistryAddress: registryServer.Addr(),
	})

	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer reader.Close()

	var got bytes.Buffer
	if _, err := reader.WriteBlob(ctx, desc.Key, &got); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	if !bytes.Equal(got.Bytes(), content) {
		t.Fatalf("downloaded data = %q, want %q", got.Bytes(), content)
	}
}

func TestResolveArtifactReadEndpointRequiresShard(t *testing.T) {
	if _, err := resolveArtifactReadEndpoint(context.Background(), " ", ReaderOptions{PinnedAddress: "127.0.0.1:8086"}); err == nil {
		t.Fatal("expected missing shard id to fail")
	}
}

func TestRoutedReader_MissingBlob(t *testing.T) {
	ctx := context.Background()
	artifactServer, _ := startArtifactRouteServer(t)
	reader, err := NewReaderForArtifact(ctx, dal.ArtifactRecord{
		BlobKey:         BlobKeySHA256(stringOfByte('0', 64)),
		ArtifactShardID: "pinned",
	}, ReaderOptions{
		Logger:        mocks.NewMockLogger(),
		PinnedAddress: artifactServer.Addr(),
	})

	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer reader.Close()

	var got bytes.Buffer
	if _, err := reader.WriteBlob(ctx, BlobKeySHA256(stringOfByte('0', 64)), &got); err == nil {
		t.Fatal("expected missing blob error")
	}
}
