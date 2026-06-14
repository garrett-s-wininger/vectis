//go:build e2e

package linux_test

import (
	"testing"
)

func TestE2EPackageCLIRPM(t *testing.T) {
	runPackageSmoke(t, packageSmokeCase{
		envPackagePath: "VECTIS_E2E_PACKAGE_CLI_RPM",
		instance:       envOrDefault("VECTIS_E2E_PACKAGE_RPM_LINUX_INSTANCE", defaultRPMInstance),
		template:       envOrDefault("VECTIS_E2E_PACKAGE_RPM_LINUX_TEMPLATE", defaultRPMTemplate),
		remoteDir:      "/tmp/vectis-cli-package-rpm",
		parseCommand:   []string{"rpm", "-qp", "--nosignature"},
		installCommand: []string{"sudo", "rpm", "-Uvh", "--nosignature"},
		removeCommand:  []string{"sudo", "rpm", "-e", "vectis-cli"},
	})
}
