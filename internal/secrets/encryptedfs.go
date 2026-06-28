package secrets

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

const (
	EncryptedFSScheme          = "encryptedfs"
	EncryptedFSKeySize         = 32
	DefaultMaxSecretBytes      = 1 << 20
	encryptedFSEnvelopeVersion = 1
	encryptedFSAlgorithm       = "AES-256-GCM"
)

type EncryptedFSProvider struct {
	root           string
	key            []byte
	maxSecretBytes int64
}

type EncryptedFSOption func(*EncryptedFSProvider) error

func WithEncryptedFSKey(key []byte) EncryptedFSOption {
	return func(p *EncryptedFSProvider) error {
		key = append([]byte(nil), key...)
		if len(key) != EncryptedFSKeySize {
			return fmt.Errorf("secrets: encryptedfs key must be %d bytes", EncryptedFSKeySize)
		}

		p.key = key
		return nil
	}
}

func WithEncryptedFSKeyFile(path string) EncryptedFSOption {
	return func(p *EncryptedFSProvider) error {
		key, err := LoadEncryptedFSKeyFile(path)
		if err != nil {
			return err
		}

		p.key = key
		return nil
	}
}

func WithMaxSecretBytes(limit int64) EncryptedFSOption {
	return func(p *EncryptedFSProvider) error {
		if limit > 0 {
			p.maxSecretBytes = limit
		}

		return nil
	}
}

func NewEncryptedFSProvider(root string, opts ...EncryptedFSOption) (*EncryptedFSProvider, error) {
	abs, err := encryptedFSAbsoluteRoot(root)
	if err != nil {
		return nil, err
	}

	p := &EncryptedFSProvider{
		root:           abs,
		maxSecretBytes: DefaultMaxSecretBytes,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if err := opt(p); err != nil {
			return nil, err
		}
	}

	if len(p.key) != EncryptedFSKeySize {
		return nil, fmt.Errorf("secrets: encryptedfs key is required")
	}

	return p, nil
}

func (*EncryptedFSProvider) ProviderKind() string { return EncryptedFSScheme }

func EncryptedFSSecretFilePath(root, rawRef string) (string, error) {
	abs, err := encryptedFSAbsoluteRoot(root)
	if err != nil {
		return "", err
	}

	resolved, err := encryptedFSPathForRef(abs, rawRef)
	if err != nil {
		return "", err
	}

	return resolved.target, nil
}

func encryptedFSAbsoluteRoot(root string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", fmt.Errorf("secrets: encryptedfs root is required")
	}

	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("secrets: resolve encryptedfs root: %w", err)
	}

	return abs, nil
}

func LoadEncryptedFSKeyFile(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("secrets: encryptedfs key file is required")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("secrets: read encryptedfs key file: %w", err)
	}

	key, err := parseEncryptedFSKey(data)
	if err != nil {
		return nil, fmt.Errorf("secrets: encryptedfs key file %s: %w", path, err)
	}

	return key, nil
}

func EnsureEncryptedFSKeyFile(path string) ([]byte, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("secrets: encryptedfs key file is required")
	}

	key, err := LoadEncryptedFSKeyFile(path)
	if err == nil {
		return key, nil
	}

	if !errorsIsNotExist(err) {
		return nil, err
	}

	key, err = GenerateEncryptedFSKey()
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("secrets: create encryptedfs key directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errorsIsExist(err) {
			return LoadEncryptedFSKeyFile(path)
		}

		return nil, fmt.Errorf("secrets: create encryptedfs key file: %w", err)
	}

	encoded := base64.StdEncoding.EncodeToString(key)
	if _, err := f.WriteString(encoded + "\n"); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("secrets: write encryptedfs key file: %w", err)
	}

	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("secrets: close encryptedfs key file: %w", err)
	}

	return key, nil
}

func GenerateEncryptedFSKey() ([]byte, error) {
	key := make([]byte, EncryptedFSKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("secrets: generate encryptedfs key: %w", err)
	}

	return key, nil
}

func (p *EncryptedFSProvider) ValidateRef(_ context.Context, ref Reference) error {
	if p == nil {
		return fmt.Errorf("%w: encryptedfs provider is not configured", ErrNotFound)
	}

	if _, err := p.pathForRef(ref.Ref); err != nil {
		return err
	}

	return nil
}

func (p *EncryptedFSProvider) Resolve(ctx context.Context, req ResolveRequest) (Bundle, error) {
	if p == nil {
		return Bundle{}, fmt.Errorf("%w: encryptedfs provider is not configured", ErrNotFound)
	}

	files := make([]FileMaterial, 0, len(req.Secrets))
	for _, ref := range req.Secrets {
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}

		resolved, err := p.pathForRef(ref.Ref)
		if err != nil {
			return Bundle{}, err
		}

		data, err := p.readSecret(resolved)
		if err != nil {
			return Bundle{}, err
		}

		files = append(files, FileMaterial{
			ID:   ref.ID,
			Path: ref.Delivery.Path,
			Data: data,
			Mode: DefaultFileMode,
		})
	}

	return Bundle{Files: files}, nil
}

type encryptedFSResolvedRef struct {
	target string
	aad    []byte
}

func (p *EncryptedFSProvider) pathForRef(raw string) (encryptedFSResolvedRef, error) {
	return encryptedFSPathForRef(p.root, raw)
}

func encryptedFSPathForRef(root, raw string) (encryptedFSResolvedRef, error) {
	rel, err := encryptedFSRelativePath(raw)
	if err != nil {
		return encryptedFSResolvedRef{}, err
	}

	target := filepath.Join(root, filepath.FromSlash(rel))
	rootRel, err := filepath.Rel(root, target)
	if err != nil {
		return encryptedFSResolvedRef{}, fmt.Errorf("%w: resolve encryptedfs ref path: %w", ErrNotFound, err)
	}

	if rootRel == "." || rootRel == ".." || strings.HasPrefix(rootRel, ".."+string(filepath.Separator)) {
		return encryptedFSResolvedRef{}, fmt.Errorf("%w: encryptedfs ref path escapes the provider root", ErrNotFound)
	}

	return encryptedFSResolvedRef{
		target: target,
		aad:    encryptedFSAAD(rel),
	}, nil
}

func encryptedFSRelativePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: encryptedfs ref is required", ErrNotFound)
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: parse encryptedfs ref: %w", ErrNotFound, err)
	}

	if u.Scheme != EncryptedFSScheme {
		return "", fmt.Errorf("%w: unsupported secret provider scheme %q", ErrNotFound, u.Scheme)
	}

	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: encryptedfs ref must not include credentials, query, or fragment", ErrNotFound)
	}

	parts := make([]string, 0, 2)
	if u.Host != "" {
		parts = append(parts, u.Host)
	}

	if u.EscapedPath() != "" {
		decodedPath, err := url.PathUnescape(u.EscapedPath())
		if err != nil {
			return "", fmt.Errorf("%w: encryptedfs ref path: %w", ErrNotFound, err)
		}

		parts = append(parts, strings.TrimPrefix(decodedPath, "/"))
	}

	rel := strings.Join(parts, "/")
	rel = strings.TrimSpace(rel)
	if rel == "" {
		return "", fmt.Errorf("%w: encryptedfs ref path is required", ErrNotFound)
	}

	if filepath.IsAbs(rel) || strings.Contains(rel, `\`) {
		return "", fmt.Errorf("%w: encryptedfs ref path must be relative and slash-separated", ErrNotFound)
	}

	for _, part := range strings.Split(rel, "/") {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("%w: encryptedfs ref path must not contain empty, current-directory, or parent-directory segments", ErrNotFound)
		}
	}

	return rel, nil
}

func (p *EncryptedFSProvider) readSecret(ref encryptedFSResolvedRef) ([]byte, error) {
	resolvedRoot, err := filepath.EvalSymlinks(p.root)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs root is not readable: %w", ErrNotFound, err)
	}

	resolvedTarget, err := filepath.EvalSymlinks(ref.target)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs secret does not exist", ErrNotFound)
	}

	rel, err := filepath.Rel(resolvedRoot, resolvedTarget)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve encryptedfs secret path: %w", ErrNotFound, err)
	}

	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("%w: encryptedfs secret path escapes provider root", ErrDenied)
	}

	info, err := os.Stat(resolvedTarget)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs secret does not exist", ErrNotFound)
	}

	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: encryptedfs secret is not a regular file", ErrDenied)
	}

	f, err := os.Open(resolvedTarget)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs secret is not readable", ErrDenied)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(f)

	limit := p.maxEncryptedEnvelopeBytes()
	envelope, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, fmt.Errorf("secrets: read encryptedfs secret envelope: %w", err)
	}

	if int64(len(envelope)) > limit {
		return nil, fmt.Errorf("%w: encryptedfs secret envelope exceeds max size", ErrDenied)
	}

	data, err := DecryptEncryptedFSSecret(envelope, p.key, ref.aad)
	if err != nil {
		return nil, err
	}

	if int64(len(data)) > p.maxSecretBytes {
		return nil, fmt.Errorf("%w: encryptedfs secret exceeds max size", ErrDenied)
	}

	return data, nil
}

func (p *EncryptedFSProvider) maxEncryptedEnvelopeBytes() int64 {
	return (p.maxSecretBytes * 2) + 4096
}

type encryptedFSEnvelope struct {
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

func EncryptEncryptedFSSecret(rawRef string, plaintext, key []byte) ([]byte, error) {
	rel, err := encryptedFSRelativePath(rawRef)
	if err != nil {
		return nil, err
	}

	return encryptEncryptedFSSecretWithAAD(plaintext, key, encryptedFSAAD(rel))
}

func DecryptEncryptedFSSecret(envelopeBytes, key, aad []byte) ([]byte, error) {
	if len(key) != EncryptedFSKeySize {
		return nil, fmt.Errorf("%w: encryptedfs key must be %d bytes", ErrDenied, EncryptedFSKeySize)
	}

	decoder := json.NewDecoder(bytes.NewReader(envelopeBytes))
	decoder.DisallowUnknownFields()

	var envelope encryptedFSEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("%w: encryptedfs secret is not a valid encrypted envelope", ErrDenied)
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("%w: encryptedfs secret envelope has trailing data", ErrDenied)
	}

	if envelope.Version != encryptedFSEnvelopeVersion {
		return nil, fmt.Errorf("%w: unsupported encryptedfs envelope version %d", ErrDenied, envelope.Version)
	}

	if envelope.Algorithm != encryptedFSAlgorithm {
		return nil, fmt.Errorf("%w: unsupported encryptedfs algorithm %q", ErrDenied, envelope.Algorithm)
	}

	nonce, err := base64.StdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs envelope nonce is invalid", ErrDenied)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("%w: encryptedfs envelope ciphertext is invalid", ErrDenied)
	}

	aead, err := encryptedFSAEAD(key)
	if err != nil {
		return nil, err
	}

	if len(nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("%w: encryptedfs envelope nonce has invalid size", ErrDenied)
	}

	plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: decrypt encryptedfs secret", ErrDenied)
	}

	return plaintext, nil
}

func WriteEncryptedFSSecretFile(root, rawRef string, plaintext, key []byte) error {
	return writeEncryptedFSSecretFile(root, rawRef, plaintext, key, false)
}

func WriteEncryptedFSSecretFileExclusive(root, rawRef string, plaintext, key []byte) error {
	return writeEncryptedFSSecretFile(root, rawRef, plaintext, key, true)
}

func writeEncryptedFSSecretFile(root, rawRef string, plaintext, key []byte, exclusive bool) error {
	provider, err := NewEncryptedFSProvider(root, WithEncryptedFSKey(key))
	if err != nil {
		return err
	}

	resolved, err := provider.pathForRef(rawRef)
	if err != nil {
		return err
	}

	envelope, err := encryptEncryptedFSSecretWithAAD(plaintext, key, resolved.aad)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(resolved.target), 0o700); err != nil {
		return fmt.Errorf("secrets: create encryptedfs secret directory: %w", err)
	}

	flags := os.O_WRONLY | os.O_CREATE
	if exclusive {
		flags |= os.O_EXCL
	} else {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(resolved.target, flags, 0o600)
	if err != nil {
		return fmt.Errorf("secrets: create encryptedfs secret file: %w", err)
	}

	if _, err := f.Write(envelope); err != nil {
		_ = f.Close()
		return fmt.Errorf("secrets: write encryptedfs secret file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("secrets: close encryptedfs secret file: %w", err)
	}

	if err := os.Chmod(resolved.target, 0o600); err != nil {
		return fmt.Errorf("secrets: chmod encryptedfs secret file: %w", err)
	}

	return nil
}

func encryptEncryptedFSSecretWithAAD(plaintext, key, aad []byte) ([]byte, error) {
	if len(key) != EncryptedFSKeySize {
		return nil, fmt.Errorf("secrets: encryptedfs key must be %d bytes", EncryptedFSKeySize)
	}

	aead, err := encryptedFSAEAD(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secrets: generate encryptedfs nonce: %w", err)
	}

	envelope := encryptedFSEnvelope{
		Version:    encryptedFSEnvelopeVersion,
		Algorithm:  encryptedFSAlgorithm,
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(aead.Seal(nil, nonce, plaintext, aad)),
	}

	out, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("secrets: encode encryptedfs envelope: %w", err)
	}

	return append(out, '\n'), nil
}

func encryptedFSAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secrets: initialize encryptedfs cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secrets: initialize encryptedfs gcm: %w", err)
	}

	return aead, nil
}

func encryptedFSAAD(rel string) []byte {
	return []byte("vectis encryptedfs v1\n" + rel)
}

func parseEncryptedFSKey(data []byte) ([]byte, error) {
	if len(data) == EncryptedFSKeySize {
		return append([]byte(nil), data...), nil
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, fmt.Errorf("key is empty")
	}

	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == EncryptedFSKeySize {
		return decoded, nil
	}

	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(trimmed); err == nil && len(decoded) == EncryptedFSKeySize {
			return decoded, nil
		}
	}

	if len(trimmed) == EncryptedFSKeySize {
		return []byte(trimmed), nil
	}

	return nil, fmt.Errorf("key must decode to %d bytes", EncryptedFSKeySize)
}

func errorsIsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

func errorsIsExist(err error) bool {
	return errors.Is(err, os.ErrExist)
}
