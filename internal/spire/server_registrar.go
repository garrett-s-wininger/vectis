package spire

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	spiretypes "github.com/spiffe/spire-api-sdk/proto/spire/api/types"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const spireServerRegistrationHintPrefix = "vectis:"

type SPIREEntryClient interface {
	BatchCreateEntry(ctx context.Context, in *entryv1.BatchCreateEntryRequest, opts ...grpc.CallOption) (*entryv1.BatchCreateEntryResponse, error)
	BatchUpdateEntry(ctx context.Context, in *entryv1.BatchUpdateEntryRequest, opts ...grpc.CallOption) (*entryv1.BatchUpdateEntryResponse, error)
	BatchDeleteEntry(ctx context.Context, in *entryv1.BatchDeleteEntryRequest, opts ...grpc.CallOption) (*entryv1.BatchDeleteEntryResponse, error)
}

type SPIREServerRegistrar struct {
	client      SPIREEntryClient
	x509SVIDTTL time.Duration
}

type SPIREServerRegistrarOption func(*SPIREServerRegistrar)

func WithSPIREServerX509SVIDTTL(ttl time.Duration) SPIREServerRegistrarOption {
	return func(r *SPIREServerRegistrar) {
		r.x509SVIDTTL = ttl
	}
}

func NewSPIREServerRegistrar(client SPIREEntryClient, opts ...SPIREServerRegistrarOption) (*SPIREServerRegistrar, error) {
	if client == nil {
		return nil, fmt.Errorf("spire: server entry client is required")
	}

	r := &SPIREServerRegistrar{client: client}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}

	if _, err := durationSeconds("SPIRE Server X.509-SVID TTL", r.x509SVIDTTL); err != nil {
		return nil, err
	}

	return r, nil
}

func DialSPIREServerRegistrar(address string, opts ...SPIREServerRegistrarOption) (*SPIREServerRegistrar, func(), error) {
	if err := ValidateServerAPIAddress(address); err != nil {
		return nil, nil, err
	}

	conn, err := grpc.NewClient(
		strings.TrimSpace(address),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)

	if err != nil {
		return nil, nil, fmt.Errorf("spire: dial server API: %w", err)
	}

	registrar, err := NewSPIREServerRegistrar(entryv1.NewEntryClient(conn), opts...)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}

	return registrar, func() { _ = conn.Close() }, nil
}

func ValidateServerAPIAddress(address string) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("spire: server API address is required")
	}

	u, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("spire: parse server API address %q: %w", address, err)
	}

	if u.Scheme != "unix" {
		return fmt.Errorf("spire: server API address %q must use unix:// scheme", address)
	}

	if strings.TrimSpace(u.Host) != "" || strings.TrimSpace(u.Path) == "" || !strings.HasPrefix(u.Path, "/") {
		return fmt.Errorf("spire: server API unix address %q must be in unix:///path/to/socket form", address)
	}

	if u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.Opaque != "" {
		return fmt.Errorf("spire: server API address %q must not include userinfo, query, fragment, or opaque data", address)
	}

	return nil
}

func ParseSelector(raw string) (Selector, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Selector{}, fmt.Errorf("spire: registration selector is required")
	}

	typ, value, ok := strings.Cut(raw, ":")
	if !ok {
		return Selector{}, fmt.Errorf("spire: registration selector %q must be in type:value form", raw)
	}

	return normalizeSelector(Selector{Type: typ, Value: value})
}

func (r *SPIREServerRegistrar) EnsureRegistration(ctx context.Context, intent RegistrationIntent) (RegistrationResult, error) {
	if r == nil || r.client == nil {
		return RegistrationResult{}, fmt.Errorf("spire: server registrar is not configured")
	}

	if err := intent.Validate(time.Now().UTC()); err != nil {
		return RegistrationResult{}, err
	}

	entry, err := r.intentEntry(intent)
	if err != nil {
		return RegistrationResult{}, err
	}

	resp, err := r.client.BatchCreateEntry(ctx, &entryv1.BatchCreateEntryRequest{
		Entries:    []*spiretypes.Entry{entry},
		OutputMask: registrationOutputMask(),
	})

	if err != nil {
		return RegistrationResult{}, fmt.Errorf("spire: create registration entry: %w", err)
	}

	results := resp.GetResults()
	if len(results) != 1 {
		return RegistrationResult{}, fmt.Errorf("spire: create registration entry returned %d results, want 1", len(results))
	}

	result := results[0]
	switch code := statusCode(result.GetStatus()); code {
	case codes.OK:
		entry := result.GetEntry()
		if entry == nil || entry.GetId() == "" {
			return RegistrationResult{}, fmt.Errorf("spire: create registration entry succeeded without entry ID")
		}

		return RegistrationResult{
			Handle:  handleFromEntry(intent, entry, true),
			Created: true,
		}, nil
	case codes.AlreadyExists:
		entry := result.GetEntry()
		if entry == nil || entry.GetId() == "" {
			return RegistrationResult{}, fmt.Errorf("spire: existing registration entry did not include entry ID")
		}

		if entry.GetHint() != registrationHint(intent) {
			return RegistrationResult{
				Handle:  handleFromEntry(intent, entry, false),
				Created: false,
			}, nil
		}

		handle, err := r.updateManagedEntry(ctx, intent, entry.GetId())
		if err != nil {
			return RegistrationResult{}, err
		}

		return RegistrationResult{Handle: handle, Created: false}, nil
	default:
		return RegistrationResult{}, fmt.Errorf("spire: create registration entry failed: %s", statusSummary(result.GetStatus(), code))
	}
}

func (r *SPIREServerRegistrar) ReleaseRegistration(ctx context.Context, handle RegistrationHandle) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("spire: server registrar is not configured")
	}

	if !handle.Managed {
		return nil
	}

	if strings.TrimSpace(handle.EntryID) == "" {
		return fmt.Errorf("spire: managed registration entry ID is required for release")
	}

	resp, err := r.client.BatchDeleteEntry(ctx, &entryv1.BatchDeleteEntryRequest{
		Ids: []string{handle.EntryID},
	})

	if err != nil {
		return fmt.Errorf("spire: delete registration entry: %w", err)
	}

	results := resp.GetResults()
	if len(results) != 1 {
		return fmt.Errorf("spire: delete registration entry returned %d results, want 1", len(results))
	}

	result := results[0]
	switch code := statusCode(result.GetStatus()); code {
	case codes.OK, codes.NotFound:
		return nil
	default:
		return fmt.Errorf("spire: delete registration entry failed: %s", statusSummary(result.GetStatus(), code))
	}
}

func (r *SPIREServerRegistrar) updateManagedEntry(ctx context.Context, intent RegistrationIntent, entryID string) (RegistrationHandle, error) {
	entry, err := r.intentEntry(intent)
	if err != nil {
		return RegistrationHandle{}, err
	}

	entry.Id = entryID
	resp, err := r.client.BatchUpdateEntry(ctx, &entryv1.BatchUpdateEntryRequest{
		Entries:    []*spiretypes.Entry{entry},
		InputMask:  registrationUpdateMask(entry),
		OutputMask: registrationOutputMask(),
	})

	if err != nil {
		return RegistrationHandle{}, fmt.Errorf("spire: update registration entry: %w", err)
	}

	results := resp.GetResults()
	if len(results) != 1 {
		return RegistrationHandle{}, fmt.Errorf("spire: update registration entry returned %d results, want 1", len(results))
	}

	result := results[0]
	if code := statusCode(result.GetStatus()); code != codes.OK {
		return RegistrationHandle{}, fmt.Errorf("spire: update registration entry failed: %s", statusSummary(result.GetStatus(), code))
	}

	updated := result.GetEntry()
	if updated == nil || updated.GetId() == "" {
		return RegistrationHandle{}, fmt.Errorf("spire: update registration entry succeeded without entry ID")
	}

	return handleFromEntry(intent, updated, true), nil
}

func (r *SPIREServerRegistrar) intentEntry(intent RegistrationIntent) (*spiretypes.Entry, error) {
	spiffeID, err := apiSPIFFEID(intent.SPIFFEID)
	if err != nil {
		return nil, err
	}

	parentID, err := apiSPIFFEID(intent.ParentSPIFFEID)
	if err != nil {
		return nil, err
	}

	selectors, err := apiSelectors(intent.Selectors)
	if err != nil {
		return nil, err
	}

	x509SVIDTTL, err := durationSeconds("SPIRE Server X.509-SVID TTL", r.x509SVIDTTL)
	if err != nil {
		return nil, err
	}

	return &spiretypes.Entry{
		SpiffeId:    spiffeID,
		ParentId:    parentID,
		Selectors:   selectors,
		ExpiresAt:   intent.ExpiresAt.Unix(),
		X509SvidTtl: x509SVIDTTL,
		Hint:        registrationHint(intent),
	}, nil
}

func apiSPIFFEID(raw string) (*spiretypes.SPIFFEID, error) {
	normalized, err := normalizeRegistrationSPIFFEID("SPIFFE ID", raw)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("spire: parse SPIFFE ID %q: %w", normalized, err)
	}

	path := u.EscapedPath()
	if path == "" {
		path = u.Path
	}

	return &spiretypes.SPIFFEID{
		TrustDomain: u.Host,
		Path:        path,
	}, nil
}

func apiSelectors(selectors []Selector) ([]*spiretypes.Selector, error) {
	normalized, err := NormalizeSelectors(selectors)
	if err != nil {
		return nil, err
	}

	out := make([]*spiretypes.Selector, 0, len(normalized))
	for _, selector := range normalized {
		out = append(out, &spiretypes.Selector{
			Type:  selector.Type,
			Value: selector.Value,
		})
	}

	return out, nil
}

func handleFromEntry(intent RegistrationIntent, entry *spiretypes.Entry, managed bool) RegistrationHandle {
	expiresAt := intent.ExpiresAt
	if entry != nil && entry.GetExpiresAt() > 0 {
		expiresAt = time.Unix(entry.GetExpiresAt(), 0).UTC()
	}

	return RegistrationHandle{
		EntryID:   entry.GetId(),
		Key:       intent.Key,
		SPIFFEID:  intent.SPIFFEID,
		ExpiresAt: expiresAt,
		Managed:   managed,
	}
}

func registrationHint(intent RegistrationIntent) string {
	return spireServerRegistrationHintPrefix + intent.Key
}

func registrationOutputMask() *spiretypes.EntryMask {
	return &spiretypes.EntryMask{
		SpiffeId:       true,
		ParentId:       true,
		Selectors:      true,
		X509SvidTtl:    true,
		ExpiresAt:      true,
		Hint:           true,
		RevisionNumber: true,
	}
}

func registrationUpdateMask(entry *spiretypes.Entry) *spiretypes.EntryMask {
	return &spiretypes.EntryMask{
		ExpiresAt:   true,
		Hint:        true,
		X509SvidTtl: entry.GetX509SvidTtl() > 0,
	}
}

func statusCode(status *spiretypes.Status) codes.Code {
	if status == nil {
		return codes.Unknown
	}

	return codes.Code(status.GetCode())
}

func statusSummary(status *spiretypes.Status, code codes.Code) string {
	if status == nil {
		return code.String()
	}

	msg := strings.TrimSpace(status.GetMessage())
	if msg == "" {
		return code.String()
	}

	return fmt.Sprintf("%s: %s", code.String(), msg)
}

func durationSeconds(label string, d time.Duration) (int32, error) {
	if d < 0 {
		return 0, fmt.Errorf("spire: %s must not be negative", label)
	}

	if d == 0 {
		return 0, nil
	}

	if d < time.Second {
		return 0, fmt.Errorf("spire: %s must be at least 1s when set", label)
	}

	seconds := d / time.Second
	if seconds > 1<<31-1 {
		return 0, fmt.Errorf("spire: %s must not exceed %ds", label, int64(1<<31-1))
	}

	return int32(seconds), nil
}
