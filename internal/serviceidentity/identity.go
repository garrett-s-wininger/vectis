package serviceidentity

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrNoPeerCertificate = errors.New("service identity: no peer certificate")
	ErrNoPeerIdentity    = errors.New("service identity: peer certificate has no URI SAN")
	ErrIdentityDenied    = errors.New("service identity: identity is not allowed")
)

// NormalizeSPIFFEAllowlist validates and normalizes exact-match SPIFFE URI SANs.
func NormalizeSPIFFEAllowlist(raw []string) ([]string, error) {
	out := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, identity := range raw {
		normalized, err := normalizeSPIFFEID(identity)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[normalized]; ok {
			continue
		}

		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	return out, nil
}

func normalizeSPIFFEID(identity string) (string, error) {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return "", fmt.Errorf("service identity: empty SPIFFE ID")
	}

	u, err := url.Parse(identity)
	if err != nil {
		return "", fmt.Errorf("service identity: parse %q: %w", identity, err)
	}

	if u.Scheme != "spiffe" {
		return "", fmt.Errorf("service identity: %q must use spiffe:// URI scheme", identity)
	}

	if strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("service identity: %q must include a SPIFFE trust domain", identity)
	}

	if strings.TrimSpace(u.Path) == "" || u.Path == "/" {
		return "", fmt.Errorf("service identity: %q must include a workload path", identity)
	}

	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("service identity: %q must not include userinfo, query, fragment, or opaque data", identity)
	}

	return u.String(), nil
}

func PeerURISANs(peerCertificates []*x509.Certificate) []string {
	if len(peerCertificates) == 0 || peerCertificates[0] == nil {
		return nil
	}

	leaf := peerCertificates[0]
	out := make([]string, 0, len(leaf.URIs))
	for _, u := range leaf.URIs {
		if u == nil {
			continue
		}

		out = append(out, u.String())
	}

	return out
}

// AuthorizePeerCertificate accepts a peer when its leaf URI SAN exactly matches
// a configured SPIFFE identity. Empty allowlists leave authorization disabled.
func AuthorizePeerCertificate(peerCertificates []*x509.Certificate, allowed []string) (string, error) {
	allowed, err := NormalizeSPIFFEAllowlist(allowed)
	if err != nil {
		return "", err
	}

	if len(allowed) == 0 {
		return "", nil
	}

	if len(peerCertificates) == 0 || peerCertificates[0] == nil {
		return "", ErrNoPeerCertificate
	}

	uris := PeerURISANs(peerCertificates)
	if len(uris) == 0 {
		return "", ErrNoPeerIdentity
	}

	allowedSet := make(map[string]struct{}, len(allowed))
	for _, identity := range allowed {
		allowedSet[identity] = struct{}{}
	}

	for _, identity := range uris {
		if _, ok := allowedSet[identity]; ok {
			return identity, nil
		}
	}

	return "", ErrIdentityDenied
}
