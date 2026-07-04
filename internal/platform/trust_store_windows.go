//go:build windows

package platform

import (
	"context"
	"errors"
)

func installCertificateAuthority(ctx context.Context, caFile string) error {
	if !trustCommandExists("certutil") {
		return errors.New("platform: certutil command not found")
	}

	return runTrustStoreCommand(ctx, "certutil", "-addstore", "-f", "Root", caFile)
}
