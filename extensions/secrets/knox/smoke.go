package knox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	sdksecrets "vectis/sdk/secrets"
)

const (
	DefaultSmokeID      = "smoke-token"
	DefaultSmokeRef     = "knox://team/smoke_token"
	DefaultSmokePath    = "smoke/token"
	DefaultSmokeTimeout = 30 * time.Second
)

type SmokeOptions struct {
	URL                string
	AuthToken          string
	AuthTokenFile      string
	InsecureSkipVerify bool
	CAFile             string
	ClientCertFile     string
	ClientKeyFile      string
	Ref                string
	ID                 string
	Path               string
	ExpectedData       string
	ExpectedSHA256     string
	WrongAuthToken     string
	MissingRef         string
	Timeout            time.Duration
	Stdout             io.Writer
}

type SmokeResult struct {
	Status           string `json:"status"`
	URL              string `json:"url"`
	Ref              string `json:"ref"`
	ID               string `json:"id"`
	Path             string `json:"path"`
	SHA256           string `json:"sha256"`
	Bytes            int    `json:"bytes"`
	WrongTokenDenied bool   `json:"wrong_token_denied"`
	MissingRefDenied bool   `json:"missing_ref_denied"`
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
			return SmokeResult{}, fmt.Errorf("knox smoke did not succeed within %s: %w", opts.Timeout, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(opts.Stdout, "Waiting for Knox endpoint %s: %v\n", opts.URL, err)
		}
	}
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.URL = strings.TrimSpace(opts.URL)
	opts.AuthToken = strings.TrimSpace(opts.AuthToken)
	opts.AuthTokenFile = strings.TrimSpace(opts.AuthTokenFile)
	opts.CAFile = strings.TrimSpace(opts.CAFile)
	opts.ClientCertFile = strings.TrimSpace(opts.ClientCertFile)
	opts.ClientKeyFile = strings.TrimSpace(opts.ClientKeyFile)
	opts.Ref = strings.TrimSpace(opts.Ref)
	if opts.Ref == "" {
		opts.Ref = DefaultSmokeRef
	}

	opts.ID = strings.TrimSpace(opts.ID)
	if opts.ID == "" {
		opts.ID = DefaultSmokeID
	}

	opts.Path = strings.TrimSpace(opts.Path)
	if opts.Path == "" {
		opts.Path = DefaultSmokePath
	}

	opts.ExpectedSHA256 = strings.ToLower(strings.TrimSpace(opts.ExpectedSHA256))
	if opts.ExpectedSHA256 == "" && opts.ExpectedData != "" {
		sum := sha256.Sum256([]byte(opts.ExpectedData))
		opts.ExpectedSHA256 = hex.EncodeToString(sum[:])
	}

	opts.WrongAuthToken = strings.TrimSpace(opts.WrongAuthToken)
	opts.MissingRef = strings.TrimSpace(opts.MissingRef)

	if opts.Timeout == 0 {
		opts.Timeout = DefaultSmokeTimeout
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.URL == "" {
		return fmt.Errorf("knox smoke url is required")
	}

	if opts.AuthToken == "" && opts.AuthTokenFile == "" {
		return fmt.Errorf("knox smoke auth token or auth token file is required")
	}

	if opts.Ref == "" {
		return fmt.Errorf("knox smoke ref is required")
	}

	if opts.ExpectedSHA256 == "" {
		return fmt.Errorf("knox smoke expected sha256 or expected data is required")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("knox smoke timeout must be > 0")
	}

	return nil
}

func runSmokeOnce(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	provider, err := NewKnoxProvider(
		opts.URL,
		WithKnoxAuthToken(opts.AuthToken),
		WithKnoxAuthTokenFile(opts.AuthTokenFile),
		WithKnoxInsecureSkipVerify(opts.InsecureSkipVerify),
		WithKnoxCAFile(opts.CAFile),
		WithKnoxClientCertificateFiles(opts.ClientCertFile, opts.ClientKeyFile),
	)

	if err != nil {
		return SmokeResult{}, err
	}

	bundle, err := provider.Resolve(ctx, ResolveRequest{
		Secrets: []Reference{{
			ID:  opts.ID,
			Ref: opts.Ref,
			Delivery: sdksecrets.Delivery{
				Type: sdksecrets.DeliveryTypeFile,
				Path: opts.Path,
			},
		}},
	})

	if err != nil {
		return SmokeResult{}, err
	}

	if len(bundle.Files) != 1 {
		return SmokeResult{}, fmt.Errorf("knox smoke resolved %d files, want 1", len(bundle.Files))
	}

	file := bundle.Files[0]
	if file.ID != opts.ID || file.Path != opts.Path {
		return SmokeResult{}, fmt.Errorf("knox smoke file = id=%q path=%q, want id=%q path=%q", file.ID, file.Path, opts.ID, opts.Path)
	}

	sum := sha256.Sum256(file.Data)
	digest := hex.EncodeToString(sum[:])
	if digest != opts.ExpectedSHA256 {
		return SmokeResult{}, fmt.Errorf("knox smoke sha256 = %s, want %s", digest, opts.ExpectedSHA256)
	}

	wrongTokenDenied := false
	if opts.WrongAuthToken != "" {
		wrongProvider, err := NewKnoxProvider(
			opts.URL,
			WithKnoxAuthToken(opts.WrongAuthToken),
			WithKnoxInsecureSkipVerify(opts.InsecureSkipVerify),
			WithKnoxCAFile(opts.CAFile),
			WithKnoxClientCertificateFiles(opts.ClientCertFile, opts.ClientKeyFile),
		)

		if err != nil {
			return SmokeResult{}, err
		}

		_, err = wrongProvider.Resolve(ctx, ResolveRequest{Secrets: []Reference{{
			ID:       opts.ID,
			Ref:      opts.Ref,
			Delivery: sdksecrets.Delivery{Type: sdksecrets.DeliveryTypeFile, Path: opts.Path},
		}}})

		if !errors.Is(err, ErrDenied) {
			return SmokeResult{}, fmt.Errorf("knox smoke wrong token error = %v, want denied", err)
		}

		wrongTokenDenied = true
	}

	missingRefDenied := false
	if opts.MissingRef != "" {
		_, err = provider.Resolve(ctx, ResolveRequest{Secrets: []Reference{{
			ID:       "missing",
			Ref:      opts.MissingRef,
			Delivery: sdksecrets.Delivery{Type: sdksecrets.DeliveryTypeFile, Path: "missing"},
		}}})

		if !errors.Is(err, ErrNotFound) {
			return SmokeResult{}, fmt.Errorf("knox smoke missing ref error = %v, want not found", err)
		}

		missingRefDenied = true
	}

	return SmokeResult{
		Status:           "ok",
		URL:              opts.URL,
		Ref:              opts.Ref,
		ID:               file.ID,
		Path:             file.Path,
		SHA256:           digest,
		Bytes:            len(file.Data),
		WrongTokenDenied: wrongTokenDenied,
		MissingRefDenied: missingRefDenied,
	}, nil
}
