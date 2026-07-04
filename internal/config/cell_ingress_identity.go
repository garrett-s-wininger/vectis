package config

import (
	"net/http"

	"vectis/internal/serviceidentity"
)

func CellIngressProducerIdentityAllowed(r *http.Request) bool {
	allowedIdentities, err := validateServiceIdentityAllowlist(
		"service_identity.cell_ingress_allowed_producer_identities",
		CellIngressAllowedProducerIdentities(),
	)

	if err != nil {
		return false
	}

	if len(allowedIdentities) == 0 {
		return true
	}

	if r == nil || r.TLS == nil {
		return false
	}

	_, err = serviceidentity.AuthorizePeerCertificate(r.TLS.PeerCertificates, allowedIdentities)
	return err == nil
}
