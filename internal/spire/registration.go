package spire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"vectis/internal/serviceidentity"
	"vectis/internal/workloadidentity"
)

const registrationKeyVersion = "vectis-spire-registration-v1"

type Selector struct {
	Type  string
	Value string
}

func (s Selector) String() string {
	return strings.TrimSpace(s.Type) + ":" + strings.TrimSpace(s.Value)
}

type ExecutionRegistrationMetadata struct {
	CellID            string
	NamespacePath     string
	JobID             string
	RunID             string
	RunIndex          int
	SegmentID         string
	ExecutionID       string
	Attempt           int
	DefinitionVersion int
	DefinitionHash    string
}

type RegistrationIntent struct {
	Key            string
	SPIFFEID       string
	ParentSPIFFEID string
	Selectors      []Selector
	ExpiresAt      time.Time
	Metadata       ExecutionRegistrationMetadata
}

type ExecutionRegistrationOptions struct {
	ParentSPIFFEID string
	Selectors      []Selector
	ExpiresAt      time.Time
	Now            time.Time
	MinTTL         time.Duration
	MaxTTL         time.Duration
}

type RegistrationHandle struct {
	EntryID   string
	Key       string
	SPIFFEID  string
	ExpiresAt time.Time
	Managed   bool
}

type RegistrationResult struct {
	Handle  RegistrationHandle
	Created bool
}

type Registrar interface {
	EnsureRegistration(ctx context.Context, intent RegistrationIntent) (RegistrationResult, error)
	ReleaseRegistration(ctx context.Context, handle RegistrationHandle) error
}

func NewExecutionRegistrationIntent(spiffeID string, execution workloadidentity.Execution, opts ExecutionRegistrationOptions) (RegistrationIntent, error) {
	normalizedSPIFFEID, err := normalizeRegistrationSPIFFEID("execution SPIFFE ID", spiffeID)
	if err != nil {
		return RegistrationIntent{}, err
	}

	parentSPIFFEID, err := normalizeRegistrationSPIFFEID("parent SPIFFE ID", opts.ParentSPIFFEID)
	if err != nil {
		return RegistrationIntent{}, err
	}

	selectors, err := NormalizeSelectors(opts.Selectors)
	if err != nil {
		return RegistrationIntent{}, err
	}

	expiresAt, err := validateRegistrationExpiry(opts)
	if err != nil {
		return RegistrationIntent{}, err
	}

	key, err := RegistrationKey(normalizedSPIFFEID, parentSPIFFEID, selectors)
	if err != nil {
		return RegistrationIntent{}, err
	}

	return RegistrationIntent{
		Key:            key,
		SPIFFEID:       normalizedSPIFFEID,
		ParentSPIFFEID: parentSPIFFEID,
		Selectors:      selectors,
		ExpiresAt:      expiresAt,
		Metadata:       executionRegistrationMetadata(execution),
	}, nil
}

func (i RegistrationIntent) Validate(now time.Time) error {
	normalizedSPIFFEID, err := normalizeRegistrationSPIFFEID("execution SPIFFE ID", i.SPIFFEID)
	if err != nil {
		return err
	}
	if normalizedSPIFFEID != i.SPIFFEID {
		return fmt.Errorf("spire: registration intent execution SPIFFE ID is not normalized")
	}

	parentSPIFFEID, err := normalizeRegistrationSPIFFEID("parent SPIFFE ID", i.ParentSPIFFEID)
	if err != nil {
		return err
	}
	if parentSPIFFEID != i.ParentSPIFFEID {
		return fmt.Errorf("spire: registration intent parent SPIFFE ID is not normalized")
	}

	selectors, err := NormalizeSelectors(i.Selectors)
	if err != nil {
		return err
	}
	if !sameSelectors(selectors, i.Selectors) {
		return fmt.Errorf("spire: registration intent selectors are not normalized")
	}

	if i.Key == "" {
		return fmt.Errorf("spire: registration intent key is required")
	}
	expectedKey, err := RegistrationKey(i.SPIFFEID, i.ParentSPIFFEID, i.Selectors)
	if err != nil {
		return err
	}
	if i.Key != expectedKey {
		return fmt.Errorf("spire: registration intent key does not match identity, parent, and selectors")
	}

	if now.IsZero() {
		now = time.Now()
	}
	if !i.ExpiresAt.After(now) {
		return fmt.Errorf("spire: registration intent expiry must be in the future")
	}

	return nil
}

func NormalizeSelectors(selectors []Selector) ([]Selector, error) {
	if len(selectors) == 0 {
		return nil, fmt.Errorf("spire: at least one trusted registration selector is required")
	}

	out := make([]Selector, 0, len(selectors))
	seen := make(map[string]struct{}, len(selectors))
	for _, selector := range selectors {
		normalized, err := normalizeSelector(selector)
		if err != nil {
			return nil, err
		}

		key := normalized.Type + "\x00" + normalized.Value
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("spire: duplicate registration selector %q", normalized.String())
		}
		seen[key] = struct{}{}
		out = append(out, normalized)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Type == out[j].Type {
			return out[i].Value < out[j].Value
		}
		return out[i].Type < out[j].Type
	})

	return out, nil
}

func RegistrationKey(spiffeID, parentSPIFFEID string, selectors []Selector) (string, error) {
	spiffeID, err := normalizeRegistrationSPIFFEID("execution SPIFFE ID", spiffeID)
	if err != nil {
		return "", err
	}

	parentSPIFFEID, err = normalizeRegistrationSPIFFEID("parent SPIFFE ID", parentSPIFFEID)
	if err != nil {
		return "", err
	}

	selectors, err = NormalizeSelectors(selectors)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	writeRegistrationKeyPart(h, registrationKeyVersion)
	writeRegistrationKeyPart(h, spiffeID)
	writeRegistrationKeyPart(h, parentSPIFFEID)
	for _, selector := range selectors {
		writeRegistrationKeyPart(h, selector.String())
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func normalizeRegistrationSPIFFEID(label, id string) (string, error) {
	normalized, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{id})
	if err != nil {
		return "", fmt.Errorf("spire: invalid %s: %w", label, err)
	}

	return normalized[0], nil
}

func normalizeSelector(selector Selector) (Selector, error) {
	typ := strings.TrimSpace(selector.Type)
	if typ == "" {
		return Selector{}, fmt.Errorf("spire: registration selector type is required")
	}

	if strings.ContainsRune(typ, ':') {
		return Selector{}, fmt.Errorf("spire: registration selector type %q must not contain ':'", typ)
	}

	if strings.IndexFunc(typ, unicode.IsSpace) >= 0 {
		return Selector{}, fmt.Errorf("spire: registration selector type %q must not contain whitespace", typ)
	}

	value := strings.TrimSpace(selector.Value)
	if value == "" {
		return Selector{}, fmt.Errorf("spire: registration selector value is required for type %q", typ)
	}

	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return Selector{}, fmt.Errorf("spire: registration selector value for type %q must not contain control characters", typ)
	}

	return Selector{Type: typ, Value: value}, nil
}

func validateRegistrationExpiry(opts ExecutionRegistrationOptions) (time.Time, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}

	now = now.UTC()
	expiresAt := opts.ExpiresAt
	if expiresAt.IsZero() {
		return time.Time{}, fmt.Errorf("spire: registration expiry is required")
	}

	expiresAt = expiresAt.UTC()
	if opts.MinTTL < 0 {
		return time.Time{}, fmt.Errorf("spire: registration minimum TTL must not be negative")
	}

	if opts.MaxTTL < 0 {
		return time.Time{}, fmt.Errorf("spire: registration maximum TTL must not be negative")
	}

	if opts.MinTTL > 0 && opts.MaxTTL > 0 && opts.MinTTL > opts.MaxTTL {
		return time.Time{}, fmt.Errorf("spire: registration minimum TTL must not exceed maximum TTL")
	}

	ttl := expiresAt.Sub(now)
	if ttl <= 0 {
		return time.Time{}, fmt.Errorf("spire: registration expiry must be in the future")
	}

	if opts.MinTTL > 0 && ttl < opts.MinTTL {
		return time.Time{}, fmt.Errorf("spire: registration TTL %s is below minimum %s", ttl, opts.MinTTL)
	}

	if opts.MaxTTL > 0 && ttl > opts.MaxTTL {
		return time.Time{}, fmt.Errorf("spire: registration TTL %s exceeds maximum %s", ttl, opts.MaxTTL)
	}

	return expiresAt, nil
}

func executionRegistrationMetadata(execution workloadidentity.Execution) ExecutionRegistrationMetadata {
	namespacePath := strings.TrimSpace(execution.NamespacePath)
	if namespacePath == "" {
		namespacePath = "/"
	}

	return ExecutionRegistrationMetadata{
		CellID:            strings.TrimSpace(execution.CellID),
		NamespacePath:     namespacePath,
		JobID:             strings.TrimSpace(execution.JobID),
		RunID:             strings.TrimSpace(execution.RunID),
		RunIndex:          execution.RunIndex,
		SegmentID:         strings.TrimSpace(execution.SegmentID),
		ExecutionID:       strings.TrimSpace(execution.ExecutionID),
		Attempt:           execution.Attempt,
		DefinitionVersion: execution.DefinitionVersion,
		DefinitionHash:    strings.TrimSpace(execution.DefinitionHash),
	}
}

func sameSelectors(a, b []Selector) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func writeRegistrationKeyPart(h interface{ Write([]byte) (int, error) }, value string) {
	_, _ = h.Write([]byte(strconv.Itoa(len(value))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(value))
	_, _ = h.Write([]byte{'\n'})
}
