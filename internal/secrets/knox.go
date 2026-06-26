package secrets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	KnoxScheme               = "knox"
	DefaultKnoxUserAgent     = "Vectis-Knox-Provider/1"
	DefaultKnoxTimeout       = 10 * time.Second
	knoxResponseCodeOK       = 0
	knoxResponseCodeNoKey    = 4
	knoxResponseCodeNoAuth   = 5
	knoxResponseCodeDenied   = 6
	knoxResponseCodeNotFound = 8
)

var knoxKeyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_:]+$`)

type knoxHTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

type KnoxProvider struct {
	baseURL        *url.URL
	authToken      string
	client         knoxHTTPClient
	maxSecretBytes int64
	userAgent      string
}

type KnoxOption func(*KnoxProvider) error

func WithKnoxAuthToken(token string) KnoxOption {
	return func(p *KnoxProvider) error {
		p.authToken = strings.TrimSpace(token)
		return nil
	}
}

func WithKnoxAuthTokenFile(path string) KnoxOption {
	return func(p *KnoxProvider) error {
		path = strings.TrimSpace(path)
		if path == "" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("secrets: read knox auth token file: %w", err)
		}

		p.authToken = strings.TrimRight(string(data), "\r\n")
		return nil
	}
}

func WithKnoxHTTPClient(client knoxHTTPClient) KnoxOption {
	return func(p *KnoxProvider) error {
		if client == nil {
			return fmt.Errorf("secrets: knox HTTP client is nil")
		}

		p.client = client
		return nil
	}
}

func WithKnoxInsecureSkipVerify(enabled bool) KnoxOption {
	return func(p *KnoxProvider) error {
		if !enabled {
			return nil
		}

		p.client = &http.Client{
			Timeout: DefaultKnoxTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // Explicit local-dev compatibility option.
			},
		}
		return nil
	}
}

func WithKnoxMaxSecretBytes(limit int64) KnoxOption {
	return func(p *KnoxProvider) error {
		if limit > 0 {
			p.maxSecretBytes = limit
		}

		return nil
	}
}

func NewKnoxProvider(rawBaseURL string, opts ...KnoxOption) (*KnoxProvider, error) {
	baseURL, err := parseKnoxBaseURL(rawBaseURL)
	if err != nil {
		return nil, err
	}

	p := &KnoxProvider{
		baseURL:        baseURL,
		client:         &http.Client{Timeout: DefaultKnoxTimeout},
		maxSecretBytes: DefaultMaxSecretBytes,
		userAgent:      DefaultKnoxUserAgent,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if err := opt(p); err != nil {
			return nil, err
		}
	}

	if strings.TrimSpace(p.authToken) == "" {
		return nil, fmt.Errorf("secrets: knox auth token is required")
	}

	return p, nil
}

func (*KnoxProvider) ProviderKind() string { return KnoxScheme }

func (p *KnoxProvider) ValidateRef(_ context.Context, ref Reference) error {
	if p == nil {
		return fmt.Errorf("%w: knox provider is not configured", ErrNotFound)
	}

	if _, err := knoxKeyIDFromRef(ref.Ref); err != nil {
		return err
	}

	return nil
}

func (p *KnoxProvider) Resolve(ctx context.Context, req ResolveRequest) (Bundle, error) {
	if p == nil {
		return Bundle{}, fmt.Errorf("%w: knox provider is not configured", ErrNotFound)
	}

	files := make([]FileMaterial, 0, len(req.Secrets))
	for _, ref := range req.Secrets {
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}

		keyID, err := knoxKeyIDFromRef(ref.Ref)
		if err != nil {
			return Bundle{}, err
		}

		data, err := p.fetchPrimary(ctx, keyID)
		if err != nil {
			return Bundle{}, err
		}

		if p.maxSecretBytes > 0 && int64(len(data)) > p.maxSecretBytes {
			return Bundle{}, fmt.Errorf("%w: knox secret %q exceeds size limit", ErrDenied, keyID)
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

func parseKnoxBaseURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("secrets: knox url is required")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("secrets: parse knox url: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("secrets: knox url must use http or https")
	}

	if strings.TrimSpace(u.Host) == "" {
		return nil, fmt.Errorf("secrets: knox url host is required")
	}

	if u.RawQuery != "" || u.Fragment != "" || u.User != nil {
		return nil, fmt.Errorf("secrets: knox url must not include credentials, query, or fragment")
	}

	clean := *u
	clean.Path = strings.TrimRight(clean.Path, "/")
	clean.RawPath = ""
	return &clean, nil
}

func knoxKeyIDFromRef(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("%w: knox ref is required", ErrNotFound)
	}

	keyID, err := parseKnoxRefKeyID(raw)
	if err != nil {
		return "", err
	}

	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return "", fmt.Errorf("%w: knox key id is required", ErrNotFound)
	}

	if strings.Contains(keyID, "/") || !knoxKeyIDPattern.MatchString(keyID) {
		return "", fmt.Errorf("%w: knox key id must contain only letters, digits, underscores, and colons", ErrNotFound)
	}

	return keyID, nil
}

func parseKnoxRefKeyID(raw string) (string, error) {
	lower := strings.ToLower(raw)
	switch {
	case strings.HasPrefix(lower, KnoxScheme+"://"):
		rest := raw[len(KnoxScheme+"://"):]
		if strings.ContainsAny(rest, "?#") || strings.Contains(rest, "@") {
			return "", fmt.Errorf("%w: knox ref must not include credentials, query, or fragment", ErrNotFound)
		}

		rest, err := url.PathUnescape(rest)
		if err != nil {
			return "", fmt.Errorf("%w: knox ref key id: %v", ErrNotFound, err)
		}

		rest = strings.TrimPrefix(rest, "/")
		parts := strings.Split(rest, "/")
		switch len(parts) {
		case 1:
			return parts[0], nil
		case 2:
			if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				return "", fmt.Errorf("%w: knox key id is required", ErrNotFound)
			}

			return parts[0] + ":" + parts[1], nil
		default:
			return "", fmt.Errorf("%w: knox ref key id must not contain path separators", ErrNotFound)
		}
	case strings.HasPrefix(lower, KnoxScheme+":"):
		rest := raw[len(KnoxScheme+":"):]
		if strings.ContainsAny(rest, "?#") || strings.Contains(rest, "@") {
			return "", fmt.Errorf("%w: knox ref must not include credentials, query, or fragment", ErrNotFound)
		}

		keyID, err := url.PathUnescape(rest)
		if err != nil {
			return "", fmt.Errorf("%w: knox ref key id: %v", ErrNotFound, err)
		}

		return keyID, nil
	default:
		u, err := url.Parse(raw)
		if err != nil {
			return "", fmt.Errorf("%w: parse knox ref: %v", ErrNotFound, err)
		}

		scheme := normalizeProviderScheme(u.Scheme)
		if scheme == "" {
			scheme = providerKindUnknown
		}

		return "", fmt.Errorf("%w: unsupported secret provider scheme %q", ErrNotFound, scheme)
	}
}

func (p *KnoxProvider) fetchPrimary(ctx context.Context, keyID string) ([]byte, error) {
	reqURL := p.keyURL(keyID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("secrets: create knox request: %w", err)
	}

	req.Header.Set("Authorization", p.authToken)
	req.Header.Set("User-Agent", p.userAgent)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("secrets: knox request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("%w: knox authorization failed", ErrDenied)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("%w: knox key %q was not found", ErrNotFound, keyID)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("secrets: knox request failed with HTTP %d", resp.StatusCode)
	}

	key, err := decodeKnoxKeyResponse(resp.Body, keyID)
	if err != nil {
		return nil, err
	}

	for _, version := range key.Versions {
		if version.Status == "Primary" {
			return append([]byte(nil), version.Data...), nil
		}
	}

	return nil, fmt.Errorf("%w: knox key %q has no primary version", ErrNotFound, keyID)
}

func (p *KnoxProvider) keyURL(keyID string) string {
	u := *p.baseURL
	prefix := strings.TrimRight(u.Path, "/")
	u.Path = prefix + "/v0/keys/" + url.PathEscape(keyID) + "/"
	u.RawPath = ""
	return u.String()
}

type knoxAPIResponse struct {
	Status  string          `json:"status"`
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type knoxKey struct {
	ID       string           `json:"id"`
	Versions []knoxKeyVersion `json:"versions"`
}

type knoxKeyVersion struct {
	ID     uint64 `json:"id"`
	Data   []byte `json:"data"`
	Status string `json:"status"`
}

func decodeKnoxKeyResponse(r io.Reader, keyID string) (knoxKey, error) {
	body, err := io.ReadAll(io.LimitReader(r, DefaultMaxSecretBytes*2))
	if err != nil {
		return knoxKey{}, fmt.Errorf("secrets: read knox response: %w", err)
	}

	var envelope knoxAPIResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		return knoxKey{}, fmt.Errorf("secrets: decode knox response: %w", err)
	}

	if len(envelope.Data) == 0 {
		var bareKey knoxKey
		if err := json.Unmarshal(body, &bareKey); err != nil {
			return knoxKey{}, fmt.Errorf("secrets: decode knox key: %w", err)
		}

		return validateKnoxKey(bareKey, keyID)
	}

	if envelope.Status != "ok" || envelope.Code != knoxResponseCodeOK {
		return knoxKey{}, knoxError(envelope.Code, envelope.Message)
	}

	var key knoxKey
	if err := json.Unmarshal(envelope.Data, &key); err != nil {
		return knoxKey{}, fmt.Errorf("secrets: decode knox key: %w", err)
	}

	return validateKnoxKey(key, keyID)
}

func validateKnoxKey(key knoxKey, requestedKeyID string) (knoxKey, error) {
	if strings.TrimSpace(key.ID) == "" {
		return knoxKey{}, fmt.Errorf("%w: knox response missing key id", ErrNotFound)
	}

	if key.ID != requestedKeyID {
		return knoxKey{}, fmt.Errorf("%w: knox response key id %q did not match %q", ErrDenied, key.ID, requestedKeyID)
	}

	if len(key.Versions) == 0 {
		return knoxKey{}, fmt.Errorf("%w: knox key %q has no versions", ErrNotFound, requestedKeyID)
	}

	return key, nil
}

func knoxError(code int, message string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "knox request was rejected"
	}

	switch code {
	case knoxResponseCodeNoKey, knoxResponseCodeNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, message)
	case knoxResponseCodeNoAuth, knoxResponseCodeDenied:
		return fmt.Errorf("%w: %s", ErrDenied, message)
	default:
		return fmt.Errorf("secrets: knox error %d: %s", code, message)
	}
}
