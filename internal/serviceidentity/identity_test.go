package serviceidentity

import (
	"crypto/x509"
	"errors"
	"net/url"
	"reflect"
	"testing"
)

func TestNormalizeSPIFFEAllowlist(t *testing.T) {
	got, err := NormalizeSPIFFEAllowlist([]string{
		" spiffe://vectis.internal/service/api ",
		"spiffe://vectis.internal/service/api",
		"spiffe://vectis.internal/service/queue",
	})
	if err != nil {
		t.Fatalf("NormalizeSPIFFEAllowlist: %v", err)
	}

	want := []string{
		"spiffe://vectis.internal/service/api",
		"spiffe://vectis.internal/service/queue",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeSPIFFEAllowlist = %v, want %v", got, want)
	}
}

func TestNormalizeSPIFFEAllowlistRejectsNonSPIFFE(t *testing.T) {
	if _, err := NormalizeSPIFFEAllowlist([]string{"https://vectis.internal/service/api"}); err == nil {
		t.Fatal("NormalizeSPIFFEAllowlist accepted non-SPIFFE URI")
	}
}

func TestNormalizeSPIFFEAllowlistRejectsSPIFFEWithoutWorkloadPath(t *testing.T) {
	if _, err := NormalizeSPIFFEAllowlist([]string{"spiffe://vectis.internal"}); err == nil {
		t.Fatal("NormalizeSPIFFEAllowlist accepted SPIFFE ID without workload path")
	}
}

func TestAuthorizePeerCertificateAllowsExactLeafURISAN(t *testing.T) {
	cert := certificateWithURIs(t, "spiffe://vectis.internal/service/api")

	identity, err := AuthorizePeerCertificate([]*x509.Certificate{cert}, []string{
		"spiffe://vectis.internal/service/api",
	})
	if err != nil {
		t.Fatalf("AuthorizePeerCertificate: %v", err)
	}

	if identity != "spiffe://vectis.internal/service/api" {
		t.Fatalf("identity = %q, want api SPIFFE ID", identity)
	}
}

func TestAuthorizePeerCertificateIgnoresIntermediateURISANs(t *testing.T) {
	leaf := &x509.Certificate{}
	intermediate := certificateWithURIs(t, "spiffe://vectis.internal/service/api")

	_, err := AuthorizePeerCertificate([]*x509.Certificate{leaf, intermediate}, []string{
		"spiffe://vectis.internal/service/api",
	})
	if !errors.Is(err, ErrNoPeerIdentity) {
		t.Fatalf("AuthorizePeerCertificate error = %v, want ErrNoPeerIdentity", err)
	}
}

func TestAuthorizePeerCertificateDeniesMissingCertificate(t *testing.T) {
	_, err := AuthorizePeerCertificate(nil, []string{"spiffe://vectis.internal/service/api"})
	if !errors.Is(err, ErrNoPeerCertificate) {
		t.Fatalf("AuthorizePeerCertificate error = %v, want ErrNoPeerCertificate", err)
	}
}

func TestAuthorizePeerCertificateDeniesMissingURISAN(t *testing.T) {
	_, err := AuthorizePeerCertificate([]*x509.Certificate{{}}, []string{"spiffe://vectis.internal/service/api"})
	if !errors.Is(err, ErrNoPeerIdentity) {
		t.Fatalf("AuthorizePeerCertificate error = %v, want ErrNoPeerIdentity", err)
	}
}

func TestAuthorizePeerCertificateDeniesWrongIdentity(t *testing.T) {
	cert := certificateWithURIs(t, "spiffe://vectis.internal/service/worker")

	_, err := AuthorizePeerCertificate([]*x509.Certificate{cert}, []string{
		"spiffe://vectis.internal/service/api",
	})
	if !errors.Is(err, ErrIdentityDenied) {
		t.Fatalf("AuthorizePeerCertificate error = %v, want ErrIdentityDenied", err)
	}
}

func TestAuthorizePeerCertificateAllowsEmptyPolicy(t *testing.T) {
	if _, err := AuthorizePeerCertificate(nil, nil); err != nil {
		t.Fatalf("AuthorizePeerCertificate empty policy: %v", err)
	}
}

func certificateWithURIs(t *testing.T, rawURIs ...string) *x509.Certificate {
	t.Helper()

	cert := &x509.Certificate{}
	for _, raw := range rawURIs {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse URI %q: %v", raw, err)
		}

		cert.URIs = append(cert.URIs, u)
	}

	return cert
}
