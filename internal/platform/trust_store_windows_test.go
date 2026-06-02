//go:build windows

package platform

import (
	"context"
	"testing"
)

func TestInstallCertificateAuthority_Windows(t *testing.T) {
	caFile := writeTestCA(t)
	commands := captureTrustCommands(t, map[string]bool{"certutil": true})

	if err := InstallCertificateAuthority(context.Background(), caFile); err != nil {
		t.Fatalf("InstallCertificateAuthority: %v", err)
	}

	assertCapturedTrustCommands(t, commands, []capturedTrustCommand{{
		name: "certutil",
		args: []string{"-addstore", "-f", "Root", caFile},
	}})
}

func TestInstallCertificateAuthority_WindowsRequiresCertutil(t *testing.T) {
	caFile := writeTestCA(t)
	captureTrustCommands(t, map[string]bool{})

	if err := InstallCertificateAuthority(context.Background(), caFile); err == nil {
		t.Fatal("expected missing certutil command to fail")
	}
}
