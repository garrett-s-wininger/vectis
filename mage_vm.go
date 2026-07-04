//go:build mage

package main

import (
	"os"
	"path/filepath"
	"sort"
)

// VmCheck runs the VM doctor checks.
func VmCheck() error {
	return VmDoctor()
}

// VmDeploySmokeCheck checks the deploy smoke VM.
func VmDeploySmokeCheck() error {
	return runVMDoctor("doctor", "deploy-smoke")
}

// VmDeploySmokePrepare prepares the deploy smoke VM.
func VmDeploySmokePrepare() error {
	return runPacker("build", packerDeploySmokeVars(), envDefault("PACKER_DEPLOY_SMOKE_DIR", filepath.Join("build", "packer", "deploy-smoke")))
}

// VmDeploySmokeValidate validates the deploy smoke VM Packer template.
func VmDeploySmokeValidate() error {
	return runPacker("validate", packerDeploySmokeVars(), envDefault("PACKER_DEPLOY_SMOKE_DIR", filepath.Join("build", "packer", "deploy-smoke")))
}

// VmDoctor runs VM readiness checks.
func VmDoctor() error {
	return runVMDoctor("doctor", "")
}

// VmPackageBuilderCheck checks the package builder VM.
func VmPackageBuilderCheck() error {
	return runVMDoctor("doctor", "package-builder")
}

// VmPackageBuilderPrepare prepares the package builder VM.
func VmPackageBuilderPrepare() error {
	return runPacker("build", packerPackageBuilderVars(), envDefault("PACKER_PACKAGE_BUILDER_DIR", filepath.Join("build", "packer", "package-builder")))
}

// VmPackageBuilderValidate validates the package builder VM Packer template.
func VmPackageBuilderValidate() error {
	return runPacker("validate", packerPackageBuilderVars(), envDefault("PACKER_PACKAGE_BUILDER_DIR", filepath.Join("build", "packer", "package-builder")))
}

// VmPackageSmokeCheck checks both package smoke VMs.
func VmPackageSmokeCheck() error {
	if err := VmPackageSmokeDebCheck(); err != nil {
		return err
	}

	return VmPackageSmokeRPMCheck()
}

// VmPackageSmokeDebCheck checks the DEB package smoke VM.
func VmPackageSmokeDebCheck() error {
	return runVMDoctor("doctor", "package-smoke-deb")
}

// VmPackageSmokeDebPrepare prepares the DEB package smoke VM.
func VmPackageSmokeDebPrepare() error {
	return runPacker("build", packerPackageSmokeVars("deb", envDefault("PACKER_PACKAGE_DEB_SMOKE_INSTANCE", "vectis-package-smoke"), envDefault("PACKER_PACKAGE_DEB_SMOKE_TEMPLATE", "ubuntu-lts")), envDefault("PACKER_PACKAGE_SMOKE_DIR", filepath.Join("build", "packer", "package-smoke")))
}

// VmPackageSmokePrepare prepares both package smoke VMs.
func VmPackageSmokePrepare() error {
	if err := VmPackageSmokeDebPrepare(); err != nil {
		return err
	}

	return VmPackageSmokeRPMPrepare()
}

// VmPackageSmokeRPMCheck checks the RPM package smoke VM.
func VmPackageSmokeRPMCheck() error {
	return runVMDoctor("doctor", "package-smoke-rpm")
}

// VmPackageSmokeRPMPrepare prepares the RPM package smoke VM.
func VmPackageSmokeRPMPrepare() error {
	return runPacker("build", packerPackageSmokeVars("rpm", envDefault("PACKER_PACKAGE_RPM_SMOKE_INSTANCE", "vectis-package-rpm-smoke"), envDefault("PACKER_PACKAGE_RPM_SMOKE_TEMPLATE", "fedora")), envDefault("PACKER_PACKAGE_SMOKE_DIR", filepath.Join("build", "packer", "package-smoke")))
}

// VmPackageSmokeValidate validates both package smoke VM Packer templates.
func VmPackageSmokeValidate() error {
	if err := runPacker("validate", packerPackageSmokeVars("deb", envDefault("PACKER_PACKAGE_DEB_SMOKE_INSTANCE", "vectis-package-smoke"), envDefault("PACKER_PACKAGE_DEB_SMOKE_TEMPLATE", "ubuntu-lts")), envDefault("PACKER_PACKAGE_SMOKE_DIR", filepath.Join("build", "packer", "package-smoke"))); err != nil {
		return err
	}

	return runPacker("validate", packerPackageSmokeVars("rpm", envDefault("PACKER_PACKAGE_RPM_SMOKE_INSTANCE", "vectis-package-rpm-smoke"), envDefault("PACKER_PACKAGE_RPM_SMOKE_TEMPLATE", "fedora")), envDefault("PACKER_PACKAGE_SMOKE_DIR", filepath.Join("build", "packer", "package-smoke")))
}

// VmPrepare prepares all configured VMs.
func VmPrepare() error {
	if err := VmDeploySmokePrepare(); err != nil {
		return err
	}

	if err := VmPackageBuilderPrepare(); err != nil {
		return err
	}

	return VmPackageSmokePrepare()
}

// VmScriptsTest runs Packer script tests.
func VmScriptsTest() error {
	return run("", nil, goCommand(), "test", "./build/packer")
}

// VmStatus reports VM readiness status.
func VmStatus() error {
	return runVMDoctor("status", "")
}

// VmValidate validates VM scripts and Packer templates.
func VmValidate() error {
	if err := VmScriptsTest(); err != nil {
		return err
	}

	if err := VmDeploySmokeValidate(); err != nil {
		return err
	}

	if err := VmPackageBuilderValidate(); err != nil {
		return err
	}

	return VmPackageSmokeValidate()
}

func runPacker(action string, vars []string, dir string) error {
	args := append([]string{action}, vars...)
	args = append(args, dir)
	return run("", nil, envDefault("PACKER", "packer"), args...)
}

func packerDeploySmokeVars() []string {
	return packerVarArgs(map[string]string{
		"instance":           envDefault("PACKER_DEPLOY_SMOKE_INSTANCE", "vectis-deploy-smoke"),
		"base_template":      envDefault("PACKER_DEPLOY_SMOKE_TEMPLATE", "ubuntu-lts"),
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "2"),
		"cpus":               envDefault("PACKER_DEPLOY_SMOKE_CPUS", "2"),
		"memory":             envDefault("PACKER_DEPLOY_SMOKE_MEMORY", "2"),
		"disk":               envDefault("PACKER_DEPLOY_SMOKE_DISK", "30"),
		"stop_after_prepare": envDefault("PACKER_DEPLOY_SMOKE_STOP", "true"),
		"lima_bin":           envDefault("PACKER_DEPLOY_SMOKE_LIMA_BIN", "limactl"),
	})
}

func packerPackageBuilderVars() []string {
	values := map[string]string{
		"instance":           envDefault("PACKER_PACKAGE_BUILDER_INSTANCE", "vectis-package-builder"),
		"base_template":      envDefault("PACKER_PACKAGE_BUILDER_TEMPLATE", "ubuntu-lts"),
		"go_version":         envDefault("PACKER_PACKAGE_BUILDER_GO_VERSION", goModGoVersion()),
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "2"),
		"cpus":               envDefault("PACKER_PACKAGE_BUILDER_CPUS", "4"),
		"memory":             envDefault("PACKER_PACKAGE_BUILDER_MEMORY", "4"),
		"disk":               envDefault("PACKER_PACKAGE_BUILDER_DISK", "60"),
		"stop_after_prepare": envDefault("PACKER_PACKAGE_BUILDER_STOP", "true"),
		"lima_bin":           packageBuilderLimaBin(),
		"workspace_root":     envDefault("PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT", "/var/tmp/vectis-package-local-workspaces"),
		"cache_root":         envDefault("PACKER_PACKAGE_BUILDER_CACHE_ROOT", "/var/tmp/vectis-package-local-cache"),
	}

	if sha := os.Getenv("PACKER_PACKAGE_BUILDER_GO_SHA256"); sha != "" {
		values["go_sha256"] = sha
	}

	return packerVarArgs(values)
}

func packerPackageSmokeVars(profile, instance, template string) []string {
	return packerVarArgs(map[string]string{
		"profile":            profile,
		"instance":           instance,
		"base_template":      template,
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "2"),
		"cpus":               envDefault("PACKER_PACKAGE_SMOKE_CPUS", "2"),
		"memory":             envDefault("PACKER_PACKAGE_SMOKE_MEMORY", "2"),
		"disk":               envDefault("PACKER_PACKAGE_SMOKE_DISK", "30"),
		"stop_after_prepare": envDefault("PACKER_PACKAGE_SMOKE_STOP", "true"),
		"lima_bin":           envDefault("PACKER_PACKAGE_SMOKE_LIMA_BIN", "limactl"),
	})
}

func packerVarArgs(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	args := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, "-var", key+"="+values[key])
	}

	return args
}

func packageBuilderLimaBin() string {
	if value := os.Getenv("PACKER_PACKAGE_BUILDER_LIMA_BIN"); value != "" {
		return value
	}

	if value := os.Getenv("PACKAGE_LOCAL_VM_PROVIDER_PATH"); value != "" {
		return value
	}

	return "limactl"
}

func runVMDoctor(mode, lane string) error {
	args := []string{"run", "./tools/vm-doctor"}
	args = append(args, vmDoctorArgs(mode, lane)...)
	return run("", nil, goCommand(), args...)
}

func vmDoctorArgs(mode, lane string) []string {
	args := []string{
		"--provider", envDefault("VM_PROVIDER", "auto"),
		"--timeout", envDefault("VM_DOCTOR_TIMEOUT", "10m"),
		"--packer", envDefault("PACKER", "packer"),
		"--prep-version", envDefault("PACKER_VM_PREP_VERSION", "2"),
		"--deploy-lima-bin", envDefault("PACKER_DEPLOY_SMOKE_LIMA_BIN", "limactl"),
		"--deploy-instance", envDefault("PACKER_DEPLOY_SMOKE_INSTANCE", "vectis-deploy-smoke"),
		"--builder-lima-bin", packageBuilderLimaBin(),
		"--builder-instance", envDefault("PACKER_PACKAGE_BUILDER_INSTANCE", "vectis-package-builder"),
		"--builder-go-version", envDefault("PACKER_PACKAGE_BUILDER_GO_VERSION", goModGoVersion()),
		"--builder-cache-root", envDefault("PACKER_PACKAGE_BUILDER_CACHE_ROOT", "/var/tmp/vectis-package-local-cache"),
		"--builder-workspace-root", envDefault("PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT", "/var/tmp/vectis-package-local-workspaces"),
		"--smoke-lima-bin", envDefault("PACKER_PACKAGE_SMOKE_LIMA_BIN", "limactl"),
		"--deb-smoke-instance", envDefault("PACKER_PACKAGE_DEB_SMOKE_INSTANCE", "vectis-package-smoke"),
		"--rpm-smoke-instance", envDefault("PACKER_PACKAGE_RPM_SMOKE_INSTANCE", "vectis-package-rpm-smoke"),
		"--mode", mode,
	}

	if lane != "" {
		args = append(args, "--lane", lane)
	}

	return args
}
