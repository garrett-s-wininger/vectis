//go:build linux

package platform

import (
	"context"
	"errors"
)

func installCertificateAuthority(ctx context.Context, caFile string) error {
	if trustCommandExists("install") && trustCommandExists("update-ca-certificates") {
		if err := runTrustStoreCommand(ctx, "install", "-m", "0644", "--", caFile, "/usr/local/share/ca-certificates/vectis-local-ca.crt"); err != nil {
			return err
		}

		return runTrustStoreCommand(ctx, "update-ca-certificates")
	}

	if trustCommandExists("install") && trustCommandExists("update-ca-trust") {
		if err := runTrustStoreCommand(ctx, "install", "-m", "0644", "--", caFile, "/etc/pki/ca-trust/source/anchors/vectis-local-ca.pem"); err != nil {
			return err
		}

		return runTrustStoreCommand(ctx, "update-ca-trust", "extract")
	}

	return errors.New("platform: no supported Linux CA installer found (need update-ca-certificates or update-ca-trust)")
}
