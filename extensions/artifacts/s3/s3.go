package s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	sdkartifact "vectis/sdk/artifact"
)

const (
	defaultRegion         = "us-east-1"
	emptyPayloadSHA256Hex = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type StoreOptions struct {
	Endpoint        string
	Region          string
	Bucket          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	PathStyle       bool
	TempDir         string
	HTTPClient      *http.Client
	now             func() time.Time
}

type Store struct {
	endpoint        *url.URL
	region          string
	bucket          string
	prefix          string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	pathStyle       bool
	tempDir         string
	client          *http.Client
	now             func() time.Time
}

func NewStore(opts StoreOptions) (*Store, error) {
	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("artifact s3 endpoint is required")
	}

	parsedEndpoint, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse artifact s3 endpoint: %w", err)
	}

	if parsedEndpoint.Scheme != "http" && parsedEndpoint.Scheme != "https" {
		return nil, fmt.Errorf("artifact s3 endpoint must use http or https")
	}

	if parsedEndpoint.Host == "" {
		return nil, fmt.Errorf("artifact s3 endpoint host is required")
	}

	parsedEndpoint.Path = strings.TrimRight(parsedEndpoint.Path, "/")
	parsedEndpoint.RawQuery = ""
	parsedEndpoint.Fragment = ""

	bucket := strings.TrimSpace(opts.Bucket)
	if bucket == "" {
		return nil, fmt.Errorf("artifact s3 bucket is required")
	}

	region := strings.TrimSpace(opts.Region)
	if region == "" {
		region = defaultRegion
	}

	client := opts.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	now := opts.now
	if now == nil {
		now = time.Now
	}

	return &Store{
		endpoint:        parsedEndpoint,
		region:          region,
		bucket:          bucket,
		prefix:          cleanObjectPrefix(opts.Prefix),
		accessKeyID:     strings.TrimSpace(opts.AccessKeyID),
		secretAccessKey: opts.SecretAccessKey,
		sessionToken:    strings.TrimSpace(opts.SessionToken),
		pathStyle:       opts.PathStyle,
		tempDir:         strings.TrimSpace(opts.TempDir),
		client:          client,
		now:             now,
	}, nil
}

func (s *Store) Put(ctx context.Context, r io.Reader, opts sdkartifact.PutOptions) (sdkartifact.BlobDescriptor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if r == nil {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("artifact blob reader is required")
	}

	expectedDigest := ""
	if opts.ExpectedSHA256 != "" {
		var err error
		expectedDigest, err = sdkartifact.NormalizeSHA256Digest(opts.ExpectedSHA256)

		if err != nil {
			return sdkartifact.BlobDescriptor{}, err
		}

		existing, err := s.Stat(ctx, sdkartifact.BlobKeySHA256(expectedDigest))
		if err == nil {
			if err := validateExistingBlob(existing, opts); err != nil {
				return sdkartifact.BlobDescriptor{}, err
			}

			return existing, nil
		}

		if !errors.Is(err, sdkartifact.ErrBlobNotFound) {
			return sdkartifact.BlobDescriptor{}, err
		}
	}

	tmp, err := os.CreateTemp(s.tempDir, "vectis-s3-artifact-*.part")
	if err != nil {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("create artifact s3 temp file: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	defer tmp.Close()

	h := sha256.New()
	size, err := copyHashing(ctx, tmp, r, h, opts.MaxBytes)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, err
	}

	digest := hex.EncodeToString(h.Sum(nil))
	desc := sdkartifact.DescriptorForSHA256(digest, size)
	if expectedDigest != "" && digest != expectedDigest {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("%w: got %s want %s", sdkartifact.ErrBlobDigestMismatch, desc.Key, sdkartifact.BlobKeySHA256(expectedDigest))
	}

	if opts.RequireSize && size != opts.ExpectedSize {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("%w: got %d want %d", sdkartifact.ErrBlobSizeMismatch, size, opts.ExpectedSize)
	}

	existing, err := s.Stat(ctx, desc.Key)
	if err == nil {
		if err := validateExistingBlob(existing, opts); err != nil {
			return sdkartifact.BlobDescriptor{}, err
		}

		return existing, nil
	}

	if err != nil && !errors.Is(err, sdkartifact.ErrBlobNotFound) {
		return sdkartifact.BlobDescriptor{}, err
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("rewind artifact s3 temp file: %w", err)
	}

	req, err := s.newRequestWithHeaders(ctx, http.MethodPut, s.objectKey(desc.Digest), nil, tmp, size, digest, map[string]string{
		"Content-Type":                "application/octet-stream",
		"x-amz-meta-vectis-algorithm": desc.Algorithm,
		"x-amz-meta-vectis-digest":    desc.Digest,
	})

	if err != nil {
		return sdkartifact.BlobDescriptor{}, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("put artifact s3 blob %s: %w", desc.Key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return sdkartifact.BlobDescriptor{}, s.responseError(resp, "put", desc.Key)
	}

	return desc, nil
}

func validateExistingBlob(desc sdkartifact.BlobDescriptor, opts sdkartifact.PutOptions) error {
	if opts.RequireSize && desc.Size != opts.ExpectedSize {
		return fmt.Errorf("%w: got %d want %d", sdkartifact.ErrBlobSizeMismatch, desc.Size, opts.ExpectedSize)
	}

	if opts.MaxBytes > 0 && desc.Size > opts.MaxBytes {
		return fmt.Errorf("%w: got %d max %d", sdkartifact.ErrBlobTooLarge, desc.Size, opts.MaxBytes)
	}

	return nil
}

func (s *Store) Stat(ctx context.Context, key string) (sdkartifact.BlobDescriptor, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return sdkartifact.BlobDescriptor{}, err
	}

	digest, err := sdkartifact.ParseSHA256BlobKey(key)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, err
	}

	req, err := s.newRequest(ctx, http.MethodHead, s.objectKey(digest), nil, nil, 0, emptyPayloadSHA256Hex)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, fmt.Errorf("stat artifact s3 blob %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return sdkartifact.BlobDescriptor{}, sdkartifact.ErrBlobNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return sdkartifact.BlobDescriptor{}, s.responseError(resp, "stat", key)
	}

	return sdkartifact.DescriptorForSHA256(digest, resp.ContentLength), nil
}

func (s *Store) Open(ctx context.Context, key string) (sdkartifact.BlobDescriptor, io.ReadCloser, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return sdkartifact.BlobDescriptor{}, nil, err
	}

	digest, err := sdkartifact.ParseSHA256BlobKey(key)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, nil, err
	}

	req, err := s.newRequest(ctx, http.MethodGet, s.objectKey(digest), nil, nil, 0, emptyPayloadSHA256Hex)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return sdkartifact.BlobDescriptor{}, nil, fmt.Errorf("open artifact s3 blob %s: %w", key, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return sdkartifact.BlobDescriptor{}, nil, sdkartifact.ErrBlobNotFound
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := s.responseError(resp, "open", key)
		resp.Body.Close()
		return sdkartifact.BlobDescriptor{}, nil, err
	}

	return sdkartifact.DescriptorForSHA256(digest, resp.ContentLength), resp.Body, nil
}

func (s *Store) StorageStats(ctx context.Context) (sdkartifact.StorageStats, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var out sdkartifact.StorageStats
	query := url.Values{
		"list-type": []string{"2"},
		"prefix":    []string{s.statsPrefix()},
	}

	for {
		req, err := s.newRequest(ctx, http.MethodGet, "", query, nil, 0, emptyPayloadSHA256Hex)
		if err != nil {
			return sdkartifact.StorageStats{}, err
		}

		resp, err := s.client.Do(req)
		if err != nil {
			return sdkartifact.StorageStats{}, fmt.Errorf("list artifact s3 storage stats: %w", err)
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err := s.responseError(resp, "list", s.statsPrefix())
			resp.Body.Close()
			return sdkartifact.StorageStats{}, err
		}

		var result listBucketResult
		if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
			resp.Body.Close()
			return sdkartifact.StorageStats{}, fmt.Errorf("parse artifact s3 list result: %w", err)
		}

		resp.Body.Close()
		for _, object := range result.Contents {
			if strings.HasSuffix(object.Key, ".blob") {
				out.BlobFiles++
				out.BlobBytes += object.Size
			}
		}

		if !result.IsTruncated || strings.TrimSpace(result.NextContinuationToken) == "" {
			out.NewBlobWritable = true
			return out, nil
		}

		query.Set("continuation-token", result.NextContinuationToken)
	}
}

type listBucketResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
	} `xml:"Contents"`
}

func (s *Store) newRequest(ctx context.Context, method, objectKey string, query url.Values, body io.Reader, contentLength int64, payloadHash string) (*http.Request, error) {
	return s.newRequestWithHeaders(ctx, method, objectKey, query, body, contentLength, payloadHash, nil)
}

func (s *Store) newRequestWithHeaders(ctx context.Context, method, objectKey string, query url.Values, body io.Reader, contentLength int64, payloadHash string, headers map[string]string) (*http.Request, error) {
	u := *s.endpoint
	if s.pathStyle {
		u.Path = joinEscapedPath(u.EscapedPath(), s.bucket, objectKey)
	} else {
		u.Host = s.bucket + "." + u.Host
		u.Path = joinEscapedPath(u.EscapedPath(), "", objectKey)
	}

	if query != nil {
		u.RawQuery = query.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}

	if contentLength > 0 {
		req.ContentLength = contentLength
	}

	if payloadHash == "" {
		payloadHash = emptyPayloadSHA256Hex
	}

	req.Header.Set("x-amz-content-sha256", payloadHash)
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	if err := s.sign(req, payloadHash); err != nil {
		return nil, err
	}

	return req, nil
}

func joinEscapedPath(basePath, bucket, objectKey string) string {
	parts := make([]string, 0, 3)
	if strings.Trim(basePath, "/") != "" {
		parts = append(parts, strings.Split(strings.Trim(basePath, "/"), "/")...)
	}

	if bucket != "" {
		parts = append(parts, url.PathEscape(bucket))
	}

	for _, part := range strings.Split(objectKey, "/") {
		if part != "" {
			parts = append(parts, url.PathEscape(part))
		}
	}

	if len(parts) == 0 {
		return "/"
	}

	return "/" + strings.Join(parts, "/")
}

func (s *Store) sign(req *http.Request, payloadHash string) error {
	if s.accessKeyID == "" && s.secretAccessKey == "" {
		return nil
	}

	if s.accessKeyID == "" || s.secretAccessKey == "" {
		return fmt.Errorf("artifact s3 access key id and secret access key must be configured together")
	}

	now := s.now().UTC()
	date := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if s.sessionToken != "" {
		req.Header.Set("x-amz-security-token", s.sessionToken)
	}

	signedHeaders, canonicalHeaders := canonicalSignedHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQuery(req.URL),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{date, s.region, "s3", "aws4_request"}, "/")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")

	signingKey := sigV4SigningKey(s.secretAccessKey, date, s.region, "s3")
	signature := hmacSHA256Hex(signingKey, stringToSign)
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s", s.accessKeyID, scope, signedHeaders, signature))
	return nil
}

func canonicalSignedHeaders(req *http.Request) (string, string) {
	names := []string{"host"}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if lower == "host" || strings.HasPrefix(lower, "x-amz-") {
			names = append(names, lower)
		}
	}

	sort.Strings(names)
	names = compactSortedStrings(names)

	var headers strings.Builder
	for _, name := range names {
		value := ""
		if name == "host" {
			value = req.URL.Host
		} else {
			value = strings.Join(headerValues(req.Header, name), ",")
		}

		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(canonicalHeaderValue(value))
		headers.WriteByte('\n')
	}

	return strings.Join(names, ";"), headers.String()
}

func headerValues(h http.Header, name string) []string {
	if values := h.Values(http.CanonicalHeaderKey(name)); len(values) > 0 {
		return values
	}

	for key, values := range h {
		if strings.EqualFold(key, name) {
			return values
		}
	}

	return nil
}

func compactSortedStrings(in []string) []string {
	out := in[:0]
	var prev string
	for _, value := range in {
		if value == prev {
			continue
		}

		out = append(out, value)
		prev = value
	}

	return out
}

func canonicalHeaderValue(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func canonicalURI(u *url.URL) string {
	uri := u.EscapedPath()
	if uri == "" {
		return "/"
	}

	return uri
}

func canonicalQuery(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}

	values, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return u.RawQuery
	}

	return values.Encode()
}

func sigV4SigningKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func hmacSHA256Hex(key []byte, data string) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

func (s *Store) responseError(resp *http.Response, op, key string) error {
	if resp.StatusCode == http.StatusNotFound {
		return sdkartifact.ErrBlobNotFound
	}

	var body bytes.Buffer
	if resp.Body != nil {
		_, _ = io.CopyN(&body, resp.Body, 4096)
	}

	detail := strings.TrimSpace(body.String())
	if detail != "" {
		return fmt.Errorf("artifact s3 %s %s failed: status=%d: %s", op, key, resp.StatusCode, detail)
	}

	return fmt.Errorf("artifact s3 %s %s failed: status=%d", op, key, resp.StatusCode)
}

func (s *Store) objectKey(digest string) string {
	return joinObjectKey(s.prefix, "blobs", sdkartifact.HashSHA256, digest[:2], digest[2:4], digest+".blob")
}

func (s *Store) statsPrefix() string {
	return joinObjectKey(s.prefix, "blobs", sdkartifact.HashSHA256) + "/"
}

func joinObjectKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			clean = append(clean, part)
		}
	}

	return strings.Join(clean, "/")
}

func cleanObjectPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return ""
	}

	return path.Clean(prefix)
}

func copyHashing(ctx context.Context, w io.Writer, r io.Reader, h hash.Hash, maxBytes int64) (int64, error) {
	buf := make([]byte, 128*1024)
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
				return size, fmt.Errorf("%w: got more than %d bytes", sdkartifact.ErrBlobTooLarge, maxBytes)
			}

			if _, err := mw.Write(buf[:n]); err != nil {
				return size, fmt.Errorf("write artifact s3 temp file: %w", err)
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
