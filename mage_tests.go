//go:build mage

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Test runs the full Go test suite.
func Test() error {
	return run("", nil, goCommand(), "test", "./...")
}

// TestIntegration runs the integration-tagged Go test suite.
func TestIntegration() error {
	return run("", nil, goCommand(), "test", "-tags=integration", "./...")
}

// TestE2E runs the e2e-tagged Go test suite.
func TestE2E() error {
	return run("", nil, goCommand(), "test", "-tags=e2e", "./tests/e2e/...")
}

// TestE2EDeployLinux runs the Linux deployment e2e tests.
func TestE2EDeployLinux() error {
	env := map[string]string{
		"PACKER_VM_PREP_VERSION": envDefault("PACKER_VM_PREP_VERSION", "2"),
	}

	return run("", env, goCommand(), "test", "-tags=e2e", "./tests/e2e/deploy/linux", "-count=1", "-v")
}

// TestE2EPackageCLIDeb runs the Linux CLI DEB package e2e test.
func TestE2EPackageCLIDeb() error {
	cfg := newPackageConfig()
	if err := packageCLIDebArch(cfg, cfg.arch); err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageCLIDeb", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_DEB": mustAbsPath(cfg.cliDebPath(cfg.arch)),
	})
}

// TestE2EPackageCLIRPM runs the Linux CLI RPM package e2e test.
func TestE2EPackageCLIRPM() error {
	cfg := newPackageConfig()
	if err := packageCLIRPMArch(cfg, cfg.arch); err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageCLIRPM", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_RPM": mustAbsPath(cfg.cliRPMPath(cfg.arch)),
	})
}

// TestE2EPackageServicesDeb runs the Linux service DEB package e2e test.
func TestE2EPackageServicesDeb() error {
	cfg := newPackageConfig()
	if err := packageCLIDebArch(cfg, cfg.arch); err != nil {
		return err
	}

	if err := packageServicesDebArch(cfg, cfg.arch); err != nil {
		return err
	}

	paths, err := packagePathEnv(packageServicesDebPaths(cfg, cfg.arch))
	if err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageServicesDeb", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_DEB":      mustAbsPath(cfg.cliDebPath(cfg.arch)),
		"VECTIS_E2E_PACKAGE_SERVICES_DEB": paths,
	})
}

// TestE2EPackageServicesRPM runs the Linux service RPM package e2e test.
func TestE2EPackageServicesRPM() error {
	cfg := newPackageConfig()
	if err := packageCLIRPMArch(cfg, cfg.arch); err != nil {
		return err
	}

	if err := packageServicesRPMArch(cfg, cfg.arch); err != nil {
		return err
	}

	paths, err := packagePathEnv(packageServicesRPMPaths(cfg, cfg.arch))
	if err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageServicesRPM", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_RPM":      mustAbsPath(cfg.cliRPMPath(cfg.arch)),
		"VECTIS_E2E_PACKAGE_SERVICES_RPM": paths,
	})
}

// TestE2EPackageLocalDeb runs the Linux vectis-local DEB package e2e test.
func TestE2EPackageLocalDeb() error {
	cfg := newPackageConfig()
	if err := packageCLIDebArch(cfg, cfg.arch); err != nil {
		return err
	}

	if err := packageLocalDispatch(cfg, "deb", cfg.arch); err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageLocalDeb", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_DEB":   mustAbsPath(cfg.cliDebPath(cfg.arch)),
		"VECTIS_E2E_PACKAGE_LOCAL_DEB": mustAbsPath(cfg.localDebPath(cfg.arch)),
	})
}

// TestE2EPackageLocalRPM runs the Linux vectis-local RPM package e2e test.
func TestE2EPackageLocalRPM() error {
	cfg := newPackageConfig()
	if err := packageCLIRPMArch(cfg, cfg.arch); err != nil {
		return err
	}

	if err := packageLocalDispatch(cfg, "rpm", cfg.arch); err != nil {
		return err
	}

	return runPackageE2E("TestE2EPackageLocalRPM", map[string]string{
		"VECTIS_E2E_PACKAGE_CLI_RPM":   mustAbsPath(cfg.cliRPMPath(cfg.arch)),
		"VECTIS_E2E_PACKAGE_LOCAL_RPM": mustAbsPath(cfg.localRPMPath(cfg.arch)),
	})
}

// TestE2EPackageLocal runs the Linux vectis-local DEB and RPM package e2e tests.
func TestE2EPackageLocal() error {
	if err := TestE2EPackageLocalDeb(); err != nil {
		return err
	}

	return TestE2EPackageLocalRPM()
}

// TestE2EVM runs the VM-backed deploy and package e2e suite.
func TestE2EVM() error {
	if err := VmCheck(); err != nil {
		return err
	}

	if err := TestE2EDeployLinux(); err != nil {
		return err
	}

	if err := TestE2EPackageCLIDeb(); err != nil {
		return err
	}

	if err := TestE2EPackageCLIRPM(); err != nil {
		return err
	}

	if err := TestE2EPackageServicesDeb(); err != nil {
		return err
	}

	if err := TestE2EPackageServicesRPM(); err != nil {
		return err
	}

	return TestE2EPackageLocal()
}

// TestFault runs the fault-injection focused suites.
func TestFault() error {
	if err := run("", nil, goCommand(), "test", "-count=1", "./internal/faultinject"); err != nil {
		return err
	}

	return run("", nil, goCommand(), "test", "-count=1",
		"./internal/artifact",
		"./internal/catalog",
		"./internal/cron",
		"./internal/job",
		"./internal/logforwarder",
		"./internal/queue",
		"./internal/reconciler",
		"-run", "Fault|RestoreSkew|QueueDown|QueueRecovery|MinGap|DuplicateDelivery|DBUnavailable",
	)
}

// TestLima runs the Lima virtual machine integration tests.
func TestLima() error {
	return run("", nil, goCommand(), "test", "./internal/platform", "./internal/job", "-run", "TestVirtualMachineIntegration", "-count=1", "-v")
}

// TestPostgresIntegration runs the Postgres integration tests.
func TestPostgresIntegration() error {
	return run("", nil, goCommand(), "test", "-tags=integration", "./tests/integration/postgres")
}

// TestPackage runs deploy package tests.
func TestPackage() error {
	return run("", nil, goCommand(), "test", "./deploy/package/...")
}

// TestProperty runs property-focused test suites.
func TestProperty() error {
	if err := run("", nil, goCommand(), "test", "-count=1", "./internal/queue", "-run", "Property"); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "test", "-count=1", "./internal/logforwarder", "-run", "Property"); err != nil {
		return err
	}

	return run("", nil, goCommand(), "test", "-count=1", "./internal/catalog", "-run", "Property")
}

// TestQuick runs the fast feedback test set.
func TestQuick() error {
	timeout := envDefault("TESTQUICK_TIMEOUT", defaultTestQuickTimeout())
	args := []string{
		"test",
		"-count=1",
		"-short",
		"-timeout=" + timeout,
		"./internal/...",
		"./cmd/...",
		"./api/...",
		"./sdk/...",
		"./examples/...",
		"./tools/...",
	}

	env, err := goTestEnv()
	if err != nil {
		return err
	}

	return run("", env, goCommand(), args...)
}

func defaultTestQuickTimeout() string {
	if runtime.GOOS == "windows" {
		return "600s"
	}

	return "60s"
}

func goTestEnv() (map[string]string, error) {
	env := map[string]string{}

	tmpDir, err := goTestTempDir()
	if err != nil {
		return nil, err
	}

	if tmpDir != "" {
		env["GOTMPDIR"] = tmpDir
		env["TEMP"] = tmpDir
		env["TMP"] = tmpDir
		env["TMPDIR"] = tmpDir
	}

	if len(env) == 0 {
		return nil, nil
	}

	return env, nil
}

func goTestTempDir() (string, error) {
	value := strings.TrimSpace(os.Getenv("VECTIS_TEST_TEMPDIR"))
	if value == "" || strings.EqualFold(value, "0") || strings.EqualFold(value, "off") || strings.EqualFold(value, "false") {
		return "", nil
	}

	return ensureGoTestTempDir(value)
}

func ensureGoTestTempDir(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(absPath, 0o755); err != nil {
		return "", err
	}

	return absPath, nil
}

// TestWindowsCompile runs a Windows nosqlite compile-only check for all packages.
func TestWindowsCompile() error {
	return testWindowsCompile("0", "nosqlite", false)
}

// TestWindowsSQLiteCompile runs a Windows CGO/SQLite compile-only check for all packages.
func TestWindowsSQLiteCompile() error {
	return testWindowsCompile("1", "", true)
}

func testWindowsCompile(cgo, defaultTags string, configureCGO bool) error {
	execPath := "true"
	cleanupExec := func() {}
	if runtime.GOOS == "windows" {
		path, cleanup, err := windowsNoopTestExec()
		if err != nil {
			return err
		}

		execPath = quoteExecArg(path)
		cleanupExec = cleanup
	}
	defer cleanupExec()

	args := []string{
		"test",
		"-exec=" + execPath,
	}

	if tags := os.Getenv("WINDOWS_TEST_TAGS"); tags != "" {
		args = append(args, "-tags="+tags)
	} else if defaultTags != "" {
		args = append(args, "-tags="+defaultTags)
	}

	packages := strings.Fields(os.Getenv("WINDOWS_TEST_PACKAGES"))
	if len(packages) == 0 {
		packages = []string{"./..."}
	}

	args = append(args, packages...)
	env := windowsTargetEnv(cgo)
	if configureCGO {
		configureWindowsCGOEnv(env)
	}

	return run("", env, goCommand(), args...)
}

func windowsNoopTestExec() (string, func(), error) {
	dir, err := os.MkdirTemp("", "vectis-go-test-exec-*")
	if err != nil {
		return "", nil, err
	}

	cleanup := func() {
		_ = os.RemoveAll(dir)
	}

	path := filepath.Join(dir, "noop.bat")
	if err := os.WriteFile(path, []byte("@echo off\r\nexit /b 0\r\n"), 0o755); err != nil {
		cleanup()
		return "", nil, err
	}

	return path, cleanup, nil
}

func quoteExecArg(value string) string {
	if !strings.ContainsAny(value, " \t\"") {
		return value
	}

	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}

func configureWindowsCGOEnv(env map[string]string) {
	if value := os.Getenv("WINDOWS_CC"); value != "" {
		env["CC"] = value
	}

	if value := os.Getenv("WINDOWS_CXX"); value != "" {
		env["CXX"] = value
	}

	if runtime.GOOS == "windows" || env["CC"] != "" || os.Getenv("CC") != "" {
		return
	}

	zig, err := exec.LookPath("zig")
	if err != nil {
		return
	}

	target := envDefault("WINDOWS_ZIG_TARGET", defaultWindowsZigTarget())
	env["CC"] = zig + " cc -target " + target
	env["CXX"] = zig + " c++ -target " + target
	setDefaultEnv(env, "ZIG_LOCAL_CACHE_DIR", filepath.Join(os.TempDir(), "vectis-zig-cache"))
	setDefaultEnv(env, "ZIG_GLOBAL_CACHE_DIR", filepath.Join(os.TempDir(), "vectis-zig-global-cache"))
}

func defaultWindowsZigTarget() string {
	switch windowsTargetArch() {
	case "386":
		return "x86-windows-gnu"
	case "arm64":
		return "aarch64-windows-gnu"
	default:
		return "x86_64-windows-gnu"
	}
}

func setDefaultEnv(env map[string]string, key, value string) {
	if os.Getenv(key) == "" {
		env[key] = value
	}
}

// TestRace runs the Go race detector suite.
func TestRace() error {
	return run("", nil, goCommand(), "test", "-race", "./...")
}
