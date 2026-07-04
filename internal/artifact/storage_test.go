package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalStore_PutStatAndOpen(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	data := "hello artifact"
	desc, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	wantDigest := sha256Hex(data)
	if desc.Key != BlobKeySHA256(wantDigest) {
		t.Fatalf("key = %q, want %q", desc.Key, BlobKeySHA256(wantDigest))
	}

	if desc.Algorithm != HashSHA256 {
		t.Fatalf("algorithm = %q, want %q", desc.Algorithm, HashSHA256)
	}

	if desc.Digest != wantDigest {
		t.Fatalf("digest = %q, want %q", desc.Digest, wantDigest)
	}

	if desc.Size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", desc.Size, len(data))
	}

	gotStat, err := store.Stat(context.Background(), desc.Key)
	if err != nil {
		t.Fatalf("stat blob: %v", err)
	}

	if gotStat != desc {
		t.Fatalf("stat descriptor = %+v, want %+v", gotStat, desc)
	}

	gotOpen, rc, err := store.Open(context.Background(), desc.Key)
	if err != nil {
		t.Fatalf("open blob: %v", err)
	}
	defer rc.Close()

	if gotOpen != desc {
		t.Fatalf("open descriptor = %+v, want %+v", gotOpen, desc)
	}

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}

	if string(b) != data {
		t.Fatalf("blob data = %q, want %q", string(b), data)
	}
}

func TestValidateBlobDescriptor(t *testing.T) {
	digest := sha256Hex("payload")
	valid := descriptorForSHA256(digest, 7)
	if err := ValidateBlobDescriptor(valid); err != nil {
		t.Fatalf("ValidateBlobDescriptor valid: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*BlobDescriptor)
		want   error
	}{
		{name: "bad algorithm", mutate: func(d *BlobDescriptor) { d.Algorithm = "md5" }, want: ErrInvalidBlobDescriptor},
		{name: "bad key", mutate: func(d *BlobDescriptor) { d.Key = "md5:" + digest }, want: ErrInvalidBlobKey},
		{name: "bad digest", mutate: func(d *BlobDescriptor) { d.Digest = strings.ToUpper(digest) }, want: ErrInvalidDigest},
		{name: "key digest mismatch", mutate: func(d *BlobDescriptor) { d.Digest = sha256Hex("other") }, want: ErrInvalidBlobDescriptor},
		{name: "negative size", mutate: func(d *BlobDescriptor) { d.Size = -1 }, want: ErrInvalidBlobDescriptor},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desc := valid
			tt.mutate(&desc)
			if err := ValidateBlobDescriptor(desc); !errors.Is(err, tt.want) {
				t.Fatalf("ValidateBlobDescriptor error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestLocalStore_PutUsesContentAddressedLayout(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	data := "layout"
	desc, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	wantPath := filepath.Join(dir, "blobs", HashSHA256, desc.Digest[:2], desc.Digest[2:4], desc.Digest+".blob")
	got, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read blob path %s: %v", wantPath, err)
	}

	if string(got) != data {
		t.Fatalf("blob path data = %q, want %q", string(got), data)
	}
}

func TestLocalStore_PutDuplicateIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	data := "duplicate"
	first, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put first blob: %v", err)
	}

	path := store.sha256Path(first.Digest)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat first blob: %v", err)
	}

	second, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put duplicate blob: %v", err)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat duplicate blob: %v", err)
	}

	if second != first {
		t.Fatalf("duplicate descriptor = %+v, want %+v", second, first)
	}

	if !os.SameFile(before, after) {
		t.Fatal("duplicate upload replaced the existing blob")
	}
}

func TestLocalStore_PutDuplicateVerifiesExistingDigest(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	data := "original"
	first, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put first blob: %v", err)
	}

	if err := os.WriteFile(store.sha256Path(first.Digest), []byte("corrupt!"), 0o644); err != nil {
		t.Fatalf("corrupt existing blob: %v", err)
	}

	_, err = store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if !errors.Is(err, ErrBlobDigestMismatch) {
		t.Fatalf("expected ErrBlobDigestMismatch, got %v", err)
	}
}

func TestLocalStore_PutExpectedDigestFastPathAllowsReadOnlyExistingBlob(t *testing.T) {
	freeBytes := uint64(1000)
	store, err := NewLocalStoreWithOptions(t.TempDir(), LocalStoreOptions{
		NewBlobMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: freeBytes, freeInodes: 1}, nil
		},
	})

	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	data := "existing"
	first, err := store.Put(context.Background(), strings.NewReader(data), PutOptions{})
	if err != nil {
		t.Fatalf("put first blob: %v", err)
	}

	freeBytes = 99
	second, err := store.Put(context.Background(), strings.NewReader("not read"), PutOptions{
		ExpectedSHA256: first.Digest,
		ExpectedSize:   first.Size,
		RequireSize:    true,
	})

	if err != nil {
		t.Fatalf("put existing blob below threshold: %v", err)
	}

	if second != first {
		t.Fatalf("existing descriptor = %+v, want %+v", second, first)
	}
}

func TestLocalStore_PutExpectedDigestMismatch(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	errDigest := sha256Hex("different")
	_, err = store.Put(context.Background(), strings.NewReader("actual"), PutOptions{
		ExpectedSHA256: errDigest,
	})

	if !errors.Is(err, ErrBlobDigestMismatch) {
		t.Fatalf("expected ErrBlobDigestMismatch, got %v", err)
	}

	assertEmptyDir(t, store.tmpDir)
}

func TestLocalStore_PutExpectedSizeMismatch(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	_, err = store.Put(context.Background(), strings.NewReader("size"), PutOptions{
		ExpectedSize: 10,
		RequireSize:  true,
	})

	if !errors.Is(err, ErrBlobSizeMismatch) {
		t.Fatalf("expected ErrBlobSizeMismatch, got %v", err)
	}

	assertEmptyDir(t, store.tmpDir)
}

func TestLocalStore_PutRejectsOversizedBlob(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	_, err = store.Put(context.Background(), strings.NewReader("too large"), PutOptions{
		MaxBytes: 3,
	})

	if !errors.Is(err, ErrBlobTooLarge) {
		t.Fatalf("expected ErrBlobTooLarge, got %v", err)
	}

	assertEmptyDir(t, store.tmpDir)
}

func TestLocalStore_ReadOnlyThresholdRejectsNewBlob(t *testing.T) {
	store, err := NewLocalStoreWithOptions(t.TempDir(), LocalStoreOptions{
		NewBlobMinFreeBytes: 100,
		statFS: func(string) (filesystemStats, error) {
			return filesystemStats{freeBytes: 99, freeInodes: 1}, nil
		},
	})

	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	if store.NewBlobWritable() {
		t.Fatal("expected new blob writes to be read-only")
	}

	_, err = store.Put(context.Background(), strings.NewReader("new"), PutOptions{})
	if !errors.Is(err, ErrStoreReadOnly) {
		t.Fatalf("expected ErrStoreReadOnly, got %v", err)
	}
}

func TestLocalStore_StorageStatsCountsCASBlobs(t *testing.T) {
	dir := t.TempDir()
	fsStats := filesystemStats{freeBytes: 2048, freeInodes: 3}
	store, err := NewLocalStoreWithOptions(dir, LocalStoreOptions{
		NewBlobMinFreeBytes: 1024,
		statFS: func(string) (filesystemStats, error) {
			return fsStats, nil
		},
	})
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	if _, err := store.Put(context.Background(), strings.NewReader("alpha"), PutOptions{}); err != nil {
		t.Fatalf("put alpha: %v", err)
	}

	if _, err := store.Put(context.Background(), strings.NewReader("beta"), PutOptions{}); err != nil {
		t.Fatalf("put beta: %v", err)
	}

	if _, err := store.Put(context.Background(), strings.NewReader("alpha"), PutOptions{}); err != nil {
		t.Fatalf("put duplicate alpha: %v", err)
	}

	invalidPath := filepath.Join(dir, "blobs", HashSHA256, "not-a-digest.blob")
	if err := os.WriteFile(invalidPath, []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write invalid blob path: %v", err)
	}

	stats, err := store.StorageStats(context.Background())
	if err != nil {
		t.Fatalf("storage stats: %v", err)
	}

	if stats.BlobFiles != 2 || stats.BlobBytes != int64(len("alpha")+len("beta")) {
		t.Fatalf("stats counted blobs = %+v, want 2 files / %d bytes", stats, len("alpha")+len("beta"))
	}

	if stats.FreeBytes != 2048 || stats.FreeInodes != 3 || !stats.NewBlobWritable {
		t.Fatalf("unexpected filesystem stats: %+v", stats)
	}

	fsStats = filesystemStats{freeBytes: 512, freeInodes: 3}
	stats, err = store.StorageStats(context.Background())
	if err != nil {
		t.Fatalf("storage stats below threshold: %v", err)
	}

	if stats.NewBlobWritable {
		t.Fatalf("expected new blob writable=false below threshold, got %+v", stats)
	}
}

func TestLocalStore_StatMissingBlob(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	_, err = store.Stat(context.Background(), BlobKeySHA256(strings.Repeat("0", sha256.Size*2)))
	if !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("expected ErrBlobNotFound, got %v", err)
	}
}

func TestLocalStore_RejectsInvalidBlobKeys(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	for _, key := range []string{
		"md5:" + strings.Repeat("0", sha256.Size*2),
		BlobKeySHA256(strings.Repeat("F", sha256.Size*2)),
		BlobKeySHA256("../" + strings.Repeat("0", sha256.Size*2-3)),
		BlobKeySHA256(strings.Repeat("0", 10)),
	} {
		if _, err := store.Stat(context.Background(), key); err == nil {
			t.Fatalf("expected invalid key %q to fail", key)
		}
	}
}

func TestLocalStore_LocksStorageDir(t *testing.T) {
	dir := t.TempDir()

	first, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("new first store: %v", err)
	}
	defer first.Close()

	if _, err := NewLocalStore(dir); err == nil {
		t.Fatal("expected second store on same directory to fail")
	}

	if err := first.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	second, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("new store after close: %v", err)
	}
	defer second.Close()
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func assertEmptyDir(t *testing.T, dir string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}

	if len(entries) != 0 {
		t.Fatalf("expected %s to be empty, got %d entries", dir, len(entries))
	}
}
