//go:build linux

package platform

import (
	"context"
	"testing"
)

func TestInstallCertificateAuthority_LinuxUpdateCACertificates(t *testing.T) {
	caFile := writeTestCA(t)
	commands := captureTrustCommands(t, map[string]bool{
		"install":                true,
		"update-ca-certificates": true,
	})

	if err := InstallCertificateAuthority(context.Background(), caFile); err != nil {
		t.Fatalf("InstallCertificateAuthority: %v", err)
	}

	assertCapturedTrustCommands(t, commands, []capturedTrustCommand{
		{name: "install", args: []string{"-m", "0644", "--", caFile, "/usr/local/share/ca-certificates/vectis-local-ca.crt"}},
		{name: "update-ca-certificates"},
	})
}

func TestInstallCertificateAuthority_LinuxUpdateCATrust(t *testing.T) {
	caFile := writeTestCA(t)
	commands := captureTrustCommands(t, map[string]bool{
		"install":         true,
		"update-ca-trust": true,
	})

	if err := InstallCertificateAuthority(context.Background(), caFile); err != nil {
		t.Fatalf("InstallCertificateAuthority: %v", err)
	}

	assertCapturedTrustCommands(t, commands, []capturedTrustCommand{
		{name: "install", args: []string{"-m", "0644", "--", caFile, "/etc/pki/ca-trust/source/anchors/vectis-local-ca.pem"}},
		{name: "update-ca-trust", args: []string{"extract"}},
	})
}

func TestInstallCertificateAuthority_LinuxRequiresSupportedInstaller(t *testing.T) {
	caFile := writeTestCA(t)
	captureTrustCommands(t, map[string]bool{"install": true})

	if err := InstallCertificateAuthority(context.Background(), caFile); err == nil {
		t.Fatal("expected missing CA update command to fail")
	}
}
