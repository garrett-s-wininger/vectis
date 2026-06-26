// Package secrets defines the extension-facing contract for Vectis secret
// providers.
package secrets

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
)

var (
	ErrNotFound = errors.New("secrets: not found")
	ErrDenied   = errors.New("secrets: denied")
)

const (
	DeliveryTypeFile DeliveryType = "file"

	DefaultFileMode       fs.FileMode = 0o400
	DefaultMaxSecretBytes int64       = 1 << 20

	ProviderKindMulti   = "multi"
	ProviderKindMixed   = "mixed"
	ProviderKindUnknown = "unknown"
)

type DeliveryType string

type Reference struct {
	ID       string
	Ref      string
	Delivery Delivery
	TaskKeys []string
}

type Delivery struct {
	Type DeliveryType
	Path string
}

type ResolveRequest struct {
	RunID               string
	ExecutionID         string
	ExecutionClaimToken string
	PeerSPIFFEID        string
	Workload            *WorkloadIdentity
	Scope               ExecutionScope
	Secrets             []Reference
}

type WorkloadIdentity struct {
	SPIFFEID      string
	TrustDomain   string
	NamespacePath string
	CellID        string
	JobID         string
	RunID         string
	ExecutionID   string
	X509SVID      *X509SVID
}

type X509SVID struct {
	SPIFFEID     string
	Certificates []*x509.Certificate
	PrivateKey   crypto.Signer
}

func (s X509SVID) TLSCertificate() (*tls.Certificate, error) {
	if s.SPIFFEID == "" {
		return nil, fmt.Errorf("secrets: X.509-SVID SPIFFE ID is required")
	}

	if len(s.Certificates) == 0 || s.Certificates[0] == nil {
		return nil, fmt.Errorf("secrets: X.509-SVID certificate chain is required")
	}

	if s.PrivateKey == nil {
		return nil, fmt.Errorf("secrets: X.509-SVID private key is required")
	}

	cert := &tls.Certificate{
		Certificate: make([][]byte, 0, len(s.Certificates)),
		PrivateKey:  s.PrivateKey,
		Leaf:        s.Certificates[0],
	}

	for _, c := range s.Certificates {
		if c == nil {
			return nil, fmt.Errorf("secrets: X.509-SVID certificate chain contains nil certificate")
		}

		cert.Certificate = append(cert.Certificate, append([]byte(nil), c.Raw...))
	}

	return cert, nil
}

type ExecutionScope struct {
	SPIFFEID          string
	TrustDomain       string
	NamespacePath     string
	CellID            string
	JobID             string
	RunID             string
	RunIndex          int
	TaskID            string
	TaskKey           string
	SegmentID         string
	ExecutionID       string
	Attempt           int
	DefinitionVersion int
	DefinitionHash    string
}

type FileMaterial struct {
	ID   string
	Path string
	Data []byte
	Mode fs.FileMode
}

type Bundle struct {
	Files []FileMaterial
}

type Provider interface {
	ValidateRef(ctx context.Context, ref Reference) error
	Resolve(ctx context.Context, req ResolveRequest) (Bundle, error)
}

type KindedProvider interface {
	ProviderKind() string
}

type RequestKindedProvider interface {
	ProviderKindForRefs(refs []Reference) string
}

func (r ResolveRequest) String() string {
	return fmt.Sprintf("run=%s execution=%s secrets=%d peer=%s", r.RunID, r.ExecutionID, len(r.Secrets), r.PeerSPIFFEID)
}
