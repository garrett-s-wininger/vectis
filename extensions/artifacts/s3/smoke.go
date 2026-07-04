package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	sdkartifact "vectis/sdk/artifact"
)

const (
	DefaultSmokeRegion       = defaultRegion
	DefaultSmokeBucket       = "vectis-artifacts"
	DefaultSmokePrefix       = "smoke"
	DefaultSmokePayload      = "vectis s3 smoke\n"
	DefaultSmokeTimeout      = 30 * time.Second
	DefaultSmokeCreateBucket = false
)

type SmokeOptions struct {
	Endpoint            string
	Region              string
	Bucket              string
	Prefix              string
	AccessKeyID         string
	SecretAccessKey     string
	SecretAccessKeyFile string
	SessionToken        string
	PathStyle           bool
	CreateBucket        bool
	Payload             string
	Timeout             time.Duration
	Stdout              io.Writer
}

type SmokeResult struct {
	Status   string                   `json:"status"`
	Endpoint string                   `json:"endpoint"`
	Bucket   string                   `json:"bucket"`
	Prefix   string                   `json:"prefix"`
	BlobKey  string                   `json:"blob_key"`
	Digest   string                   `json:"digest"`
	Bytes    int64                    `json:"bytes"`
	Created  bool                     `json:"bucket_create_attempted"`
	Stats    sdkartifact.StorageStats `json:"stats"`
}

func RunSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeSmokeOptions(opts)
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	var lastErr error
	for {
		result, err := runSmokeOnce(ctx, opts)
		if err == nil {
			return result, nil
		}

		lastErr = err
		select {
		case <-ctx.Done():
			return SmokeResult{}, fmt.Errorf("s3 smoke did not succeed within %s: %w", opts.Timeout, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(opts.Stdout, "Waiting for S3 endpoint %s: %v\n", opts.Endpoint, err)
		}
	}
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.Endpoint = strings.TrimSpace(opts.Endpoint)
	opts.Region = strings.TrimSpace(opts.Region)
	if opts.Region == "" {
		opts.Region = DefaultSmokeRegion
	}

	opts.Bucket = strings.TrimSpace(opts.Bucket)
	if opts.Bucket == "" {
		opts.Bucket = DefaultSmokeBucket
	}

	opts.Prefix = cleanObjectPrefix(opts.Prefix)
	if opts.Prefix == "" {
		opts.Prefix = joinObjectKey(DefaultSmokePrefix, fmt.Sprintf("%d", time.Now().UTC().UnixNano()))
	}

	opts.AccessKeyID = strings.TrimSpace(opts.AccessKeyID)
	opts.SecretAccessKey = strings.TrimSpace(opts.SecretAccessKey)
	opts.SecretAccessKeyFile = strings.TrimSpace(opts.SecretAccessKeyFile)
	opts.SessionToken = strings.TrimSpace(opts.SessionToken)
	if opts.Payload == "" {
		opts.Payload = DefaultSmokePayload
	}

	if opts.Timeout == 0 {
		opts.Timeout = DefaultSmokeTimeout
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.Endpoint == "" {
		return fmt.Errorf("s3 smoke endpoint is required")
	}

	if opts.Bucket == "" {
		return fmt.Errorf("s3 smoke bucket is required")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("s3 smoke timeout must be > 0")
	}

	return nil
}

func runSmokeOnce(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	secret, err := smokeSecretAccessKey(opts)
	if err != nil {
		return SmokeResult{}, err
	}

	store, err := NewStore(StoreOptions{
		Endpoint:        opts.Endpoint,
		Region:          opts.Region,
		Bucket:          opts.Bucket,
		Prefix:          opts.Prefix,
		AccessKeyID:     opts.AccessKeyID,
		SecretAccessKey: secret,
		SessionToken:    opts.SessionToken,
		PathStyle:       opts.PathStyle,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	if opts.CreateBucket {
		if err := ensureSmokeBucket(ctx, store); err != nil {
			return SmokeResult{}, err
		}
	}

	payload := []byte(opts.Payload)
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])
	desc, err := store.Put(ctx, strings.NewReader(opts.Payload), sdkartifact.PutOptions{
		ExpectedSHA256: digest,
		ExpectedSize:   int64(len(payload)),
		RequireSize:    true,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	stat, err := store.Stat(ctx, desc.Key)
	if err != nil {
		return SmokeResult{}, err
	}

	if stat != desc {
		return SmokeResult{}, fmt.Errorf("s3 smoke stat descriptor = %+v, want %+v", stat, desc)
	}

	openDesc, rc, err := store.Open(ctx, desc.Key)
	if err != nil {
		return SmokeResult{}, err
	}
	defer rc.Close()

	body, err := io.ReadAll(rc)
	if err != nil {
		return SmokeResult{}, fmt.Errorf("read s3 smoke blob: %w", err)
	}

	if openDesc != desc {
		return SmokeResult{}, fmt.Errorf("s3 smoke open descriptor = %+v, want %+v", openDesc, desc)
	}

	if string(body) != opts.Payload {
		return SmokeResult{}, fmt.Errorf("s3 smoke blob body mismatch")
	}

	stats, err := store.StorageStats(ctx)
	if err != nil {
		return SmokeResult{}, err
	}

	if !stats.NewBlobWritable || stats.BlobFiles < 1 || stats.BlobBytes < int64(len(payload)) {
		return SmokeResult{}, fmt.Errorf("s3 smoke stats = %+v, want writable with at least one blob", stats)
	}

	return SmokeResult{
		Status:   "ok",
		Endpoint: opts.Endpoint,
		Bucket:   opts.Bucket,
		Prefix:   opts.Prefix,
		BlobKey:  desc.Key,
		Digest:   desc.Digest,
		Bytes:    desc.Size,
		Created:  opts.CreateBucket,
		Stats:    stats,
	}, nil
}

func smokeSecretAccessKey(opts SmokeOptions) (string, error) {
	if opts.SecretAccessKeyFile == "" {
		return opts.SecretAccessKey, nil
	}

	data, err := os.ReadFile(opts.SecretAccessKeyFile)
	if err != nil {
		return "", fmt.Errorf("read s3 smoke secret access key file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func ensureSmokeBucket(ctx context.Context, store *Store) error {
	req, err := store.newRequest(ctx, http.MethodPut, "", nil, nil, 0, emptyPayloadSHA256Hex)
	if err != nil {
		return err
	}

	resp, err := store.client.Do(req)
	if err != nil {
		return fmt.Errorf("create s3 smoke bucket %s: %w", store.bucket, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := store.responseError(resp, "create bucket", store.bucket)
		if errors.Is(err, sdkartifact.ErrBlobNotFound) {
			return fmt.Errorf("create s3 smoke bucket %s failed: status=%d", store.bucket, resp.StatusCode)
		}

		return err
	}

	return nil
}
