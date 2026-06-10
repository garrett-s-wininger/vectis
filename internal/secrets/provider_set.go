package secrets

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

const (
	providerKindMulti   = "multi"
	providerKindUnknown = "unknown"
)

type ProviderSet struct {
	providers map[string]Provider
}

func NewProviderSet() *ProviderSet {
	return &ProviderSet{
		providers: make(map[string]Provider),
	}
}

func (s *ProviderSet) Register(scheme string, provider Provider) error {
	if s == nil {
		return fmt.Errorf("secrets: provider set is nil")
	}

	scheme = normalizeProviderScheme(scheme)
	if scheme == "" {
		return fmt.Errorf("secrets: provider scheme is required")
	}

	if provider == nil {
		return fmt.Errorf("secrets: provider for scheme %q is nil", scheme)
	}

	if s.providers == nil {
		s.providers = make(map[string]Provider)
	}

	if _, ok := s.providers[scheme]; ok {
		return fmt.Errorf("secrets: provider for scheme %q is already registered", scheme)
	}

	s.providers[scheme] = provider
	return nil
}

func (s *ProviderSet) ProviderKind() string {
	return providerKindMulti
}

func (s *ProviderSet) ProviderKindForRefs(refs []Reference) string {
	if s == nil || len(refs) == 0 {
		return providerKindMulti
	}

	kind := ""
	for _, ref := range refs {
		scheme, err := providerSchemeForRef(ref.Ref)
		if err != nil {
			scheme = providerKindUnknown
		} else if _, ok := s.providers[scheme]; !ok {
			scheme = providerKindUnknown
		}

		if kind == "" {
			kind = scheme
			continue
		}

		if kind != scheme {
			return "mixed"
		}
	}

	if kind == "" {
		return providerKindUnknown
	}

	return kind
}

func (s *ProviderSet) ValidateRef(ctx context.Context, ref Reference) error {
	provider, err := s.providerForRef(ref.Ref)
	if err != nil {
		return err
	}

	return provider.ValidateRef(ctx, ref)
}

func (s *ProviderSet) Resolve(ctx context.Context, req ResolveRequest) (Bundle, error) {
	if s == nil {
		return Bundle{}, fmt.Errorf("%w: secret provider set is not configured", ErrNotFound)
	}

	files := make([]FileMaterial, 0, len(req.Secrets))
	for _, ref := range req.Secrets {
		if err := ctx.Err(); err != nil {
			return Bundle{}, err
		}

		provider, err := s.providerForRef(ref.Ref)
		if err != nil {
			return Bundle{}, err
		}

		subReq := req
		subReq.Secrets = []Reference{ref}
		bundle, err := provider.Resolve(ctx, subReq)
		if err != nil {
			return Bundle{}, err
		}

		files = append(files, bundle.Files...)
	}

	return Bundle{Files: files}, nil
}

func (s *ProviderSet) providerForRef(rawRef string) (Provider, error) {
	if s == nil || len(s.providers) == 0 {
		return nil, fmt.Errorf("%w: secret provider set is not configured", ErrNotFound)
	}

	scheme, err := providerSchemeForRef(rawRef)
	if err != nil {
		return nil, err
	}

	provider, ok := s.providers[scheme]
	if !ok {
		return nil, fmt.Errorf("%w: no secret provider registered for scheme %q", ErrNotFound, scheme)
	}

	return provider, nil
}

func providerSchemeForRef(rawRef string) (string, error) {
	rawRef = strings.TrimSpace(rawRef)
	if rawRef == "" {
		return "", fmt.Errorf("%w: secret ref is required", ErrNotFound)
	}

	u, err := url.Parse(rawRef)
	if err != nil {
		return "", fmt.Errorf("%w: parse secret ref: %v", ErrNotFound, err)
	}

	scheme := normalizeProviderScheme(u.Scheme)
	if scheme == "" {
		return "", fmt.Errorf("%w: secret ref scheme is required", ErrNotFound)
	}

	return scheme, nil
}

func normalizeProviderScheme(scheme string) string {
	return strings.ToLower(strings.TrimSpace(scheme))
}
