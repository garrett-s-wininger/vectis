package spire

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"vectis/internal/serviceidentity"

	"github.com/spiffe/go-spiffe/v2/workloadapi"
)

var (
	ErrX509SVIDSourceRequired  = errors.New("spire: X.509-SVID source is required")
	ErrExpectedSPIFFEIDInvalid = errors.New("spire: expected execution SPIFFE ID is invalid")
	ErrNoMatchingX509SVID      = errors.New("spire: no X.509-SVID for expected execution identity")
)

type X509SVID struct {
	SPIFFEID string
}

type X509SVIDSource interface {
	FetchX509SVIDs(ctx context.Context) ([]X509SVID, error)
}

type WorkloadAPISource struct {
	address string
}

func ValidateWorkloadAPIAddress(address string) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("spire: workload API address is required")
	}

	if err := workloadapi.ValidateAddress(address); err != nil {
		return fmt.Errorf("spire: invalid workload API address %q: %w", address, err)
	}

	return nil
}

func NewWorkloadAPISource(address string) (*WorkloadAPISource, error) {
	address = strings.TrimSpace(address)
	if err := ValidateWorkloadAPIAddress(address); err != nil {
		return nil, err
	}

	return &WorkloadAPISource{address: address}, nil
}

func (s *WorkloadAPISource) FetchX509SVIDs(ctx context.Context) ([]X509SVID, error) {
	if s == nil {
		return nil, fmt.Errorf("spire: workload API source is required")
	}

	svids, err := workloadapi.FetchX509SVIDs(ctx, workloadapi.WithAddr(s.address))
	if err != nil {
		return nil, fmt.Errorf("spire: fetch X.509-SVIDs: %w", err)
	}

	out := make([]X509SVID, 0, len(svids))
	for _, svid := range svids {
		if svid == nil {
			continue
		}

		out = append(out, X509SVID{SPIFFEID: svid.ID.String()})
	}

	return out, nil
}

func RequireX509SVID(ctx context.Context, source X509SVIDSource, expectedSPIFFEID string) error {
	if source == nil {
		return ErrX509SVIDSourceRequired
	}

	normalized, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{expectedSPIFFEID})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrExpectedSPIFFEIDInvalid, err)
	}
	expectedSPIFFEID = normalized[0]

	svids, err := source.FetchX509SVIDs(ctx)
	if err != nil {
		return err
	}

	for _, svid := range svids {
		normalized, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{svid.SPIFFEID})
		if err != nil {
			continue
		}

		if normalized[0] == expectedSPIFFEID {
			return nil
		}
	}

	return fmt.Errorf("%w: %s", ErrNoMatchingX509SVID, expectedSPIFFEID)
}
