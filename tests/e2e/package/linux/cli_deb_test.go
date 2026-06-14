//go:build e2e

package linux_test

import (
	"testing"
)

func TestE2EPackageCLIDeb(t *testing.T) {
	runPackageSmoke(t, packageSmokeCase{
		envPackagePath: "VECTIS_E2E_PACKAGE_CLI_DEB",
		profile:        "deb",
		instance:       envOrDefault("VECTIS_E2E_PACKAGE_LINUX_INSTANCE", defaultPackageInstance),
		remoteDir:      "/tmp/vectis-cli-package-deb",
		parseCommand:   []string{"dpkg-deb", "--info"},
		installCommand: []string{"sudo", "dpkg", "-i"},
		removeCommand:  []string{"sudo", "dpkg", "-r", "vectis-cli"},
	})
}
