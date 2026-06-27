package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"vectis/internal/platform"
	sdkartifact "vectis/sdk/artifact"
)

const (
	HashSHA256              = sdkartifact.HashSHA256
	artifactBlobFileSuffix  = ".blob"
	artifactStorageLockFile = "artifact.lock"
)

var (
	ErrBlobNotFound          = sdkartifact.ErrBlobNotFound
	ErrBlobDigestMismatch    = sdkartifact.ErrBlobDigestMismatch
	ErrBlobSizeMismatch      = sdkartifact.ErrBlobSizeMismatch
	ErrBlobTooLarge          = sdkartifact.ErrBlobTooLarge
	ErrInvalidBlobKey        = sdkartifact.ErrInvalidBlobKey
	ErrInvalidBlobDescriptor = sdkartifact.ErrInvalidBlobDescriptor
	ErrInvalidDigest         = sdkartifact.ErrInvalidDigest
	ErrStoreReadOnly         = sdkartifact.ErrStoreReadOnly
)

type BlobDescriptor = sdkartifact.BlobDescriptor

type StorageStats = sdkartifact.StorageStats

type PutOptions = sdkartifact.PutOptions

type LocalStoreOptions struct {
	NewBlobMinFreeBytes uint64
	statFS              filesystemStatFunc
}

type LocalStore struct {
	baseDir             string
	tmpDir              string
	newBlobMinFreeBytes uint64
	statFS              filesystemStatFunc
	lockFile            *os.File
}

type filesystemStats struct {
	freeBytes  uint64
	freeInodes uint64
}

type filesystemStatFunc func(path string) (filesystemStats, error)

func NewLocalStore(baseDir string) (*LocalStore, error) {
	return NewLocalStoreWithOptions(baseDir, LocalStoreOptions{})
}

func NewLocalStoreWithOptions(baseDir string, opts LocalStoreOptions) (*LocalStore, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("local artifact storage base dir is required")
	}

	tmpDir := filepath.Join(baseDir, "tmp")
	blobDir := filepath.Join(baseDir, "blobs", HashSHA256)
	for _, dir := range []string{baseDir, tmpDir, blobDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create artifact storage dir %s: %w", dir, err)
		}
	}

	lockFile, err := acquireArtifactStorageLock(baseDir)
	if err != nil {
		return nil, err
	}

	statFS := opts.statFS
	if statFS == nil {
		statFS = defaultFilesystemStats
	}

	return &LocalStore{
		baseDir:             baseDir,
		tmpDir:              tmpDir,
		newBlobMinFreeBytes: opts.NewBlobMinFreeBytes,
		statFS:              statFS,
		lockFile:            lockFile,
	}, nil
}

func acquireArtifactStorageLock(dir string) (*os.File, error) {
	lockPath := filepath.Join(dir, artifactStorageLockFile)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open artifact storage lock %s: %w", lockPath, err)
	}

	if err := platform.TryLockFileExclusive(f); err != nil {
		_ = f.Close()
		if platform.IsFileLockUnavailable(err) {
			return nil, fmt.Errorf("artifact storage directory %s is already in use by another artifact process; use a distinct storage directory for each active artifact shard: %w", dir, err)
		}

		return nil, fmt.Errorf("lock artifact storage directory %s: %w", dir, err)
	}

	return f, nil
}

func (s *LocalStore) Close() error {
	if s == nil || s.lockFile == nil {
		return nil
	}

	lockFile := s.lockFile
	s.lockFile = nil

	var result error
	if err := platform.UnlockFile(lockFile); err != nil {
		result = fmt.Errorf("unlock artifact storage directory %s: %w", s.baseDir, err)
	}

	if err := lockFile.Close(); err != nil && result == nil {
		result = fmt.Errorf("close artifact storage lock %s: %w", filepath.Join(s.baseDir, artifactStorageLockFile), err)
	}

	return result
}

func (s *LocalStore) Put(ctx context.Context, r io.Reader, opts PutOptions) (BlobDescriptor, error) {
	if r == nil {
		return BlobDescriptor{}, fmt.Errorf("artifact blob reader is required")
	}

	expectedDigest := ""
	if opts.ExpectedSHA256 != "" {
		var err error
		expectedDigest, err = normalizeSHA256Digest(opts.ExpectedSHA256)
		if err != nil {
			return BlobDescriptor{}, err
		}

		existing, err := s.verifySHA256Digest(ctx, expectedDigest)
		if err == nil {
			if err := validateExistingBlob(existing, opts); err != nil {
				return BlobDescriptor{}, err
			}

			return existing, nil
		}

		if !errors.Is(err, ErrBlobNotFound) {
			return BlobDescriptor{}, err
		}
	}

	if err := s.ensureCanCreateBlob(); err != nil {
		return BlobDescriptor{}, err
	}

	tmp, err := os.CreateTemp(s.tmpDir, "upload-*.part")
	if err != nil {
		return BlobDescriptor{}, fmt.Errorf("create artifact temp file: %w", err)
	}

	tmpPath := tmp.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	h := sha256.New()
	size, err := copyHashing(ctx, tmp, r, h, opts.MaxBytes)
	if closeErr := tmp.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close artifact temp file %s: %w", tmpPath, closeErr)
	}

	if err != nil {
		return BlobDescriptor{}, err
	}

	digest := hex.EncodeToString(h.Sum(nil))
	desc := descriptorForSHA256(digest, size)
	if expectedDigest != "" && digest != expectedDigest {
		return BlobDescriptor{}, fmt.Errorf("%w: got %s want %s", ErrBlobDigestMismatch, desc.Key, BlobKeySHA256(expectedDigest))
	}

	if opts.RequireSize && size != opts.ExpectedSize {
		return BlobDescriptor{}, fmt.Errorf("%w: got %d want %d", ErrBlobSizeMismatch, size, opts.ExpectedSize)
	}

	if err := syncPath(tmpPath); err != nil {
		return BlobDescriptor{}, fmt.Errorf("sync artifact temp file %s: %w", tmpPath, err)
	}

	finalPath := s.sha256Path(digest)
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return BlobDescriptor{}, fmt.Errorf("create artifact blob dir: %w", err)
	}

	if err := os.Link(tmpPath, finalPath); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return BlobDescriptor{}, fmt.Errorf("publish artifact blob %s: %w", desc.Key, err)
		}

		existing, statErr := s.verifySHA256Digest(ctx, digest)
		if statErr != nil {
			return BlobDescriptor{}, statErr
		}

		if existing.Size != size {
			return BlobDescriptor{}, fmt.Errorf("%w: existing %s has %d bytes, upload has %d", ErrBlobSizeMismatch, desc.Key, existing.Size, size)
		}
	} else {
		for _, dir := range blobPublishSyncDirs(finalPath) {
			if err := syncPath(dir); err != nil {
				return BlobDescriptor{}, fmt.Errorf("sync artifact blob dir %s: %w", dir, err)
			}
		}
	}

	if err := os.Remove(tmpPath); err != nil {
		return BlobDescriptor{}, fmt.Errorf("remove artifact temp file %s: %w", tmpPath, err)
	}

	cleanupTmp = false
	return desc, nil
}

func validateExistingBlob(desc BlobDescriptor, opts PutOptions) error {
	if opts.RequireSize && desc.Size != opts.ExpectedSize {
		return fmt.Errorf("%w: got %d want %d", ErrBlobSizeMismatch, desc.Size, opts.ExpectedSize)
	}

	if opts.MaxBytes > 0 && desc.Size > opts.MaxBytes {
		return fmt.Errorf("%w: got %d max %d", ErrBlobTooLarge, desc.Size, opts.MaxBytes)
	}

	return nil
}

func (s *LocalStore) Stat(ctx context.Context, key string) (BlobDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return BlobDescriptor{}, err
	}

	digest, err := parseSHA256BlobKey(key)
	if err != nil {
		return BlobDescriptor{}, err
	}

	return s.statSHA256Digest(ctx, digest)
}

func (s *LocalStore) Open(ctx context.Context, key string) (BlobDescriptor, io.ReadCloser, error) {
	desc, err := s.Stat(ctx, key)
	if err != nil {
		return BlobDescriptor{}, nil, err
	}

	f, err := os.Open(s.sha256Path(desc.Digest))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlobDescriptor{}, nil, ErrBlobNotFound
		}

		return BlobDescriptor{}, nil, fmt.Errorf("open artifact blob %s: %w", key, err)
	}

	return desc, f, nil
}

func (s *LocalStore) NewBlobWritable() bool {
	if s == nil {
		return false
	}

	if s.newBlobMinFreeBytes == 0 {
		return true
	}

	stats, err := s.statFS(s.baseDir)
	if err != nil {
		return false
	}

	return s.newBlobWritableForStats(stats)
}

func (s *LocalStore) StorageStats(ctx context.Context) (StorageStats, error) {
	if s == nil {
		return StorageStats{}, fmt.Errorf("artifact local store is nil")
	}

	var out StorageStats
	root := filepath.Join(s.baseDir, "blobs", HashSHA256)
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("walk artifact storage path %s: %w", path, err)
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}

		if !validSHA256BlobPath(root, path) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("inspect artifact blob %s: %w", path, err)
		}

		out.BlobFiles++
		out.BlobBytes += info.Size()
		return nil
	}); err != nil {
		return StorageStats{}, err
	}

	fsStats, err := s.statFS(s.baseDir)
	if err != nil {
		return StorageStats{}, fmt.Errorf("inspect artifact storage filesystem: %w", err)
	}

	out.FreeBytes = fsStats.freeBytes
	out.FreeInodes = fsStats.freeInodes
	out.FreeBytesKnown = true
	out.FreeInodesKnown = true
	out.NewBlobWritable = s.newBlobWritableForStats(fsStats)
	return out, nil
}

func (s *LocalStore) ensureCanCreateBlob() error {
	if s.newBlobMinFreeBytes == 0 {
		return nil
	}

	stats, err := s.statFS(s.baseDir)
	if err != nil {
		return fmt.Errorf("inspect artifact storage filesystem: %w", err)
	}

	if stats.freeInodes == 0 {
		return fmt.Errorf("%w: no free inodes in %s", ErrStoreReadOnly, s.baseDir)
	}

	if stats.freeBytes < s.newBlobMinFreeBytes {
		return fmt.Errorf("%w: %d bytes free below %d byte threshold in %s", ErrStoreReadOnly, stats.freeBytes, s.newBlobMinFreeBytes, s.baseDir)
	}

	return nil
}

func (s *LocalStore) newBlobWritableForStats(stats filesystemStats) bool {
	if s.newBlobMinFreeBytes == 0 {
		return true
	}

	return stats.freeInodes > 0 && stats.freeBytes >= s.newBlobMinFreeBytes
}

func (s *LocalStore) statSHA256Digest(ctx context.Context, digest string) (BlobDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return BlobDescriptor{}, err
	}

	path := s.sha256Path(digest)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlobDescriptor{}, ErrBlobNotFound
		}

		return BlobDescriptor{}, fmt.Errorf("stat artifact blob %s: %w", BlobKeySHA256(digest), err)
	}

	if info.IsDir() {
		return BlobDescriptor{}, fmt.Errorf("artifact blob path %s is a directory", path)
	}

	return descriptorForSHA256(digest, info.Size()), nil
}

func (s *LocalStore) verifySHA256Digest(ctx context.Context, digest string) (BlobDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return BlobDescriptor{}, err
	}

	path := s.sha256Path(digest)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BlobDescriptor{}, ErrBlobNotFound
		}

		return BlobDescriptor{}, fmt.Errorf("open artifact blob %s: %w", BlobKeySHA256(digest), err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(f)

	h := sha256.New()
	size, err := copyHashing(ctx, io.Discard, f, h, 0)
	if err != nil {
		return BlobDescriptor{}, fmt.Errorf("verify artifact blob %s: %w", BlobKeySHA256(digest), err)
	}

	gotDigest := hex.EncodeToString(h.Sum(nil))
	if gotDigest != digest {
		return BlobDescriptor{}, fmt.Errorf("%w: stored %s has digest %s", ErrBlobDigestMismatch, BlobKeySHA256(digest), BlobKeySHA256(gotDigest))
	}

	return descriptorForSHA256(digest, size), nil
}

func (s *LocalStore) sha256Path(digest string) string {
	return filepath.Join(s.baseDir, "blobs", HashSHA256, digest[:2], digest[2:4], digest+artifactBlobFileSuffix)
}

func blobPublishSyncDirs(finalPath string) []string {
	leafDir := filepath.Dir(finalPath)
	prefixDir := filepath.Dir(leafDir)
	hashDir := filepath.Dir(prefixDir)
	return []string{leafDir, prefixDir, hashDir}
}

func BlobKeySHA256(digest string) string {
	return sdkartifact.BlobKeySHA256(digest)
}

func descriptorForSHA256(digest string, size int64) BlobDescriptor {
	return sdkartifact.DescriptorForSHA256(digest, size)
}

func ValidateBlobDescriptor(desc BlobDescriptor) error {
	return sdkartifact.ValidateBlobDescriptor(desc)
}

func parseSHA256BlobKey(key string) (string, error) {
	return sdkartifact.ParseSHA256BlobKey(key)
}

func validSHA256BlobPath(root, path string) bool {
	name := filepath.Base(path)
	if !strings.HasSuffix(name, artifactBlobFileSuffix) {
		return false
	}

	digest := strings.TrimSuffix(name, artifactBlobFileSuffix)
	if _, err := normalizeSHA256Digest(digest); err != nil {
		return false
	}

	if filepath.Base(filepath.Dir(path)) != digest[2:4] {
		return false
	}

	if filepath.Base(filepath.Dir(filepath.Dir(path))) != digest[:2] {
		return false
	}

	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func normalizeSHA256Digest(digest string) (string, error) {
	return sdkartifact.NormalizeSHA256Digest(digest)
}

func copyHashing(ctx context.Context, w io.Writer, r io.Reader, h hash.Hash, maxBytes int64) (int64, error) {
	buf, releaseBuf := borrowArtifactBuffer(defaultArtifactChunkBytes)
	defer releaseBuf()

	var size int64
	mw := io.MultiWriter(w, h)

	for {
		if err := ctx.Err(); err != nil {
			return size, err
		}

		n, readErr := r.Read(buf)
		if n > 0 {
			nextSize := size + int64(n)
			if maxBytes > 0 && nextSize > maxBytes {
				return size, fmt.Errorf("%w: got more than %d bytes", ErrBlobTooLarge, maxBytes)
			}

			if _, err := mw.Write(buf[:n]); err != nil {
				return size, fmt.Errorf("write artifact temp file: %w", err)
			}

			size = nextSize
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return size, nil
			}

			return size, fmt.Errorf("read artifact blob: %w", readErr)
		}
	}
}

func defaultFilesystemStats(path string) (filesystemStats, error) {
	stats, err := platform.StatFileSystem(path)
	if err != nil {
		return filesystemStats{}, err
	}

	return filesystemStats{
		freeBytes:  stats.FreeBytes,
		freeInodes: stats.FreeInodes,
	}, nil
}

func syncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(f)

	return f.Sync()
}
