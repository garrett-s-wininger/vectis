//go:build darwin

package platform

import (
	"context"
	"testing"
)

func TestInstallCertificateAuthority_Darwin(t *testing.T) {
	caFile := writeTestCA(t)
	commands := captureTrustCommands(t, map[string]bool{"security": true})

	if err := InstallCertificateAuthority(context.Background(), caFile); err != nil {
		t.Fatalf("InstallCertificateAuthority: %v", err)
	}

	assertCapturedTrustCommands(t, commands, []capturedTrustCommand{{
		name: "security",
		args: []string{"add-trusted-cert", "-d", "-r", "trustRoot", "-k", "/Library/Keychains/System.keychain", caFile},
	}})
}

func TestInstallCertificateAuthority_DarwinRequiresSecurity(t *testing.T) {
	caFile := writeTestCA(t)
	captureTrustCommands(t, map[string]bool{})

	if err := InstallCertificateAuthority(context.Background(), caFile); err == nil {
		t.Fatal("expected missing security command to fail")
	}
}
