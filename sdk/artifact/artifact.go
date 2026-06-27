package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const HashSHA256 = "sha256"

var (
	ErrBlobNotFound          = errors.New("artifact blob not found")
	ErrBlobDigestMismatch    = errors.New("artifact blob digest mismatch")
	ErrBlobSizeMismatch      = errors.New("artifact blob size mismatch")
	ErrBlobTooLarge          = errors.New("artifact blob exceeds size limit")
	ErrInvalidBlobKey        = errors.New("invalid artifact blob key")
	ErrInvalidBlobDescriptor = errors.New("invalid artifact blob descriptor")
	ErrInvalidDigest         = errors.New("invalid artifact digest")
	ErrStoreReadOnly         = errors.New("artifact storage is read-only for new blobs")
)

type BlobDescriptor struct {
	Key       string
	Algorithm string
	Digest    string
	Size      int64
}

type PutOptions struct {
	ExpectedSHA256 string
	ExpectedSize   int64
	RequireSize    bool
	MaxBytes       int64
}

type StorageStats struct {
	BlobFiles       int64
	BlobBytes       int64
	FreeBytes       uint64
	FreeInodes      uint64
	FreeBytesKnown  bool
	FreeInodesKnown bool
	NewBlobWritable bool
}

type Store interface {
	Put(context.Context, io.Reader, PutOptions) (BlobDescriptor, error)
	Stat(context.Context, string) (BlobDescriptor, error)
	Open(context.Context, string) (BlobDescriptor, io.ReadCloser, error)
}

func BlobKeySHA256(digest string) string {
	return HashSHA256 + ":" + digest
}

func DescriptorForSHA256(digest string, size int64) BlobDescriptor {
	return BlobDescriptor{
		Key:       BlobKeySHA256(digest),
		Algorithm: HashSHA256,
		Digest:    digest,
		Size:      size,
	}
}

func ValidateBlobDescriptor(desc BlobDescriptor) error {
	key := strings.TrimSpace(desc.Key)
	algorithm := strings.TrimSpace(desc.Algorithm)
	digest := strings.TrimSpace(desc.Digest)

	if key == "" {
		return fmt.Errorf("%w: artifact blob key is required", ErrInvalidBlobDescriptor)
	}

	if algorithm != HashSHA256 {
		return fmt.Errorf("%w: artifact blob algorithm %q is not supported", ErrInvalidBlobDescriptor, algorithm)
	}

	keyDigest, err := ParseSHA256BlobKey(key)
	if err != nil {
		return err
	}

	normalizedDigest, err := NormalizeSHA256Digest(digest)
	if err != nil {
		return err
	}

	if keyDigest != normalizedDigest {
		return fmt.Errorf("%w: blob key digest %q does not match descriptor digest %q", ErrInvalidBlobDescriptor, keyDigest, normalizedDigest)
	}

	if desc.Size < 0 {
		return fmt.Errorf("%w: artifact blob size must be >= 0", ErrInvalidBlobDescriptor)
	}

	return nil
}

func ParseSHA256BlobKey(key string) (string, error) {
	prefix := HashSHA256 + ":"
	if !strings.HasPrefix(key, prefix) {
		return "", fmt.Errorf("%w: must use %s prefix", ErrInvalidBlobKey, prefix)
	}

	return NormalizeSHA256Digest(strings.TrimPrefix(key, prefix))
}

func NormalizeSHA256Digest(digest string) (string, error) {
	if len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("%w: sha256 digest must be %d lowercase hex characters", ErrInvalidDigest, sha256.Size*2)
	}

	if strings.ToLower(digest) != digest {
		return "", fmt.Errorf("%w: sha256 digest must be lowercase hex", ErrInvalidDigest)
	}

	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: sha256 digest must be lowercase hex: %v", ErrInvalidDigest, err)
	}

	return digest, nil
}
