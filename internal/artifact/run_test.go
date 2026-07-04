package artifact

import (
	"context"
	"io"
	"strings"
	"testing"

	"vectis/internal/registry"
)

type metadataStore struct {
	writable bool
}

func (s metadataStore) Put(context.Context, io.Reader, PutOptions) (BlobDescriptor, error) {
	return BlobDescriptor{}, nil
}

func (s metadataStore) Stat(context.Context, string) (BlobDescriptor, error) {
	return BlobDescriptor{}, nil
}

func (s metadataStore) Open(context.Context, string) (BlobDescriptor, io.ReadCloser, error) {
	return BlobDescriptor{}, io.NopCloser(strings.NewReader("")), nil
}

func (s metadataStore) NewBlobWritable() bool {
	return s.writable
}

func TestArtifactServiceMetadataReportsWritableState(t *testing.T) {
	got := artifactServiceMetadata(metadataStore{writable: true})
	if got[registry.MetadataArtifactWriteState] != registry.ArtifactWriteStateWritable {
		t.Fatalf("expected writable metadata, got %+v", got)
	}

	if got[registry.MetadataCellID] != registry.DefaultCellID {
		t.Fatalf("expected default cell metadata, got %+v", got)
	}
}

func TestArtifactServiceMetadataReportsReadOnlyState(t *testing.T) {
	got := artifactServiceMetadata(metadataStore{writable: false})
	if got[registry.MetadataArtifactWriteState] != registry.ArtifactWriteStateReadOnly {
		t.Fatalf("expected read-only metadata, got %+v", got)
	}
}

func TestArtifactDefaultInstanceID(t *testing.T) {
	got := DefaultInstanceID(":8086")
	if got == "" || got == "artifact" {
		t.Fatalf("expected hostname-port instance ID, got %q", got)
	}
}
