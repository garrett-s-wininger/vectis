//go:build e2e

package linux_test

import "testing"

func TestE2EPackageLocalDeb(t *testing.T) {
	runPackageSmoke(t, packageSmokeCase{
		envPackagePaths: []string{
			"VECTIS_E2E_PACKAGE_CLI_DEB",
			"VECTIS_E2E_PACKAGE_LOCAL_DEB",
		},
		instance:        envOrDefault("VECTIS_E2E_PACKAGE_LINUX_INSTANCE", defaultPackageInstance),
		template:        envOrDefault("VECTIS_E2E_PACKAGE_LINUX_TEMPLATE", defaultPackageTemplate),
		remoteDir:       "/tmp/vectis-local-package-deb",
		parseCommand:    []string{"dpkg-deb", "--info"},
		installCommand:  []string{"sudo", "dpkg", "-i"},
		removeCommand:   []string{"sudo", "dpkg", "-r", "vectis-local", "vectis-cli"},
		verifyInstalled: verifyLocalInstalled,
		verifyRemoved:   verifyLocalRemoved,
	})
}
