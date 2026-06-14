//go:build e2e

package linux_test

import (
	"testing"
)

func TestE2EPackageServicesRPM(t *testing.T) {
	runPackageSmoke(t, packageSmokeCase{
		envPackagePaths: []string{
			"VECTIS_E2E_PACKAGE_CLI_RPM",
			"VECTIS_E2E_PACKAGE_SERVICES_RPM",
		},
		instance:        envOrDefault("VECTIS_E2E_PACKAGE_RPM_LINUX_INSTANCE", defaultRPMInstance),
		template:        envOrDefault("VECTIS_E2E_PACKAGE_RPM_LINUX_TEMPLATE", defaultRPMTemplate),
		remoteDir:       "/tmp/vectis-services-package-rpm",
		parseCommand:    []string{"rpm", "-qp", "--nosignature"},
		installCommand:  []string{"sudo", "rpm", "-Uvh", "--nosignature"},
		removeCommand:   append([]string{"sudo", "rpm", "-e"}, servicePackageRemovalNames()...),
		verifyInstalled: verifyServicesInstalled,
		verifyRemoved:   verifyServicesRemoved,
	})
}
