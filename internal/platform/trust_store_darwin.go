//go:build darwin

package platform

import (
	"context"
	"errors"
)

func installCertificateAuthority(ctx context.Context, caFile string) error {
	if !trustCommandExists("security") {
		return errors.New("platform: security command not found")
	}

	return runTrustStoreCommand(
		ctx,
		"security",
		"add-trusted-cert",
		"-d",
		"-r",
		"trustRoot",
		"-k",
		"/Library/Keychains/System.keychain",
		caFile,
	)
}
