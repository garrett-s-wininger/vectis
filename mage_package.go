//go:build mage

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

const (
	defaultPackageServiceApps = "api artifact catalog cell-ingress cron docs log log-forwarder orchestrator queue reconciler registry secrets spiffe worker worker-core"
	defaultPackageLocalApps   = "local api artifact catalog cell-ingress cron docs log orchestrator queue reconciler registry secrets worker worker-core"
)

var (
	builtPackageBinaries       = map[string]struct{}{}
	packageLinuxArtifactsReady bool
)

type packageConfig struct {
	outDir      string
	buildDir    string
	arch        string
	archs       []string
	serviceApps []string
	localArchs  []string
	localApps   []string
	version     string
	release     string
}

// PackageCLIBinary builds the Linux vectis-cli binary for an architecture.
func PackageCLIBinary(arch string) error {
	cfg := newPackageConfig()
	return packageCLIBinary(cfg, arch)
}

// PackageLinuxArtifacts renders Linux package support artifacts.
func PackageLinuxArtifacts() error {
	cfg := newPackageConfig()
	return packageLinuxArtifacts(cfg)
}

// PackageServiceBinary builds one Linux service binary for an architecture.
func PackageServiceBinary(arch, app string) error {
	cfg := newPackageConfig()
	return packageServiceBinary(cfg, arch, app)
}

// PackageLocalCGOCheck checks whether native vectis-local Linux packages can build on this host.
func PackageLocalCGOCheck(arch string) error {
	return packageLocalCGOCheck(arch)
}

// PackageLocalWrapper creates the vectis-local package wrapper script.
func PackageLocalWrapper() error {
	cfg := newPackageConfig()
	return packageLocalWrapper(cfg)
}

// PackageLocalBinary builds one native Linux vectis-local package binary for an architecture.
func PackageLocalBinary(arch, app string) error {
	cfg := newPackageConfig()
	return packageLocalBinary(cfg, arch, app)
}

// PackageCLIDebArch builds the vectis-cli DEB for one architecture.
func PackageCLIDebArch(arch string) error {
	cfg := newPackageConfig()
	return packageCLIDebArch(cfg, arch)
}

// PackageCLIRPMArch builds the vectis-cli RPM for one architecture.
func PackageCLIRPMArch(arch string) error {
	cfg := newPackageConfig()
	return packageCLIRPMArch(cfg, arch)
}

// PackageCLIDeb builds vectis-cli DEBs for PACKAGE_ARCHES.
func PackageCLIDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageCLIDebArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageCLIRPM builds vectis-cli RPMs for PACKAGE_ARCHES.
func PackageCLIRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageCLIRPMArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageCLI builds vectis-cli DEB and RPM packages.
func PackageCLI() error {
	if err := PackageCLIDeb(); err != nil {
		return err
	}

	return PackageCLIRPM()
}

// PackageLocalDebArch builds the vectis-local DEB for one architecture.
func PackageLocalDebArch(arch string) error {
	cfg := newPackageConfig()
	return packageLocalDispatch(cfg, "deb", arch)
}

// PackageLocalRPMArch builds the vectis-local RPM for one architecture.
func PackageLocalRPMArch(arch string) error {
	cfg := newPackageConfig()
	return packageLocalDispatch(cfg, "rpm", arch)
}

// PackageLocalDeb builds vectis-local DEBs for PACKAGE_LOCAL_ARCHES.
func PackageLocalDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.localArchs {
		if err := packageLocalDispatch(cfg, "deb", arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageLocalRPM builds vectis-local RPMs for PACKAGE_LOCAL_ARCHES.
func PackageLocalRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.localArchs {
		if err := packageLocalDispatch(cfg, "rpm", arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageLocal builds vectis-local DEB and RPM packages.
func PackageLocal() error {
	if err := PackageLocalDeb(); err != nil {
		return err
	}

	return PackageLocalRPM()
}

// PackageLocalNativeDebArch builds a native vectis-local DEB on a Linux-capable host.
func PackageLocalNativeDebArch(arch string) error {
	cfg := newPackageConfig()
	return packageLocalNativeArch(cfg, "deb", arch)
}

// PackageLocalNativeRPMArch builds a native vectis-local RPM on a Linux-capable host.
func PackageLocalNativeRPMArch(arch string) error {
	cfg := newPackageConfig()
	return packageLocalNativeArch(cfg, "rpm", arch)
}

// PackageLocalNativeDeb builds native vectis-local DEBs for PACKAGE_LOCAL_ARCHES.
func PackageLocalNativeDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.localArchs {
		if err := packageLocalNativeArch(cfg, "deb", arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageLocalNativeRPM builds native vectis-local RPMs for PACKAGE_LOCAL_ARCHES.
func PackageLocalNativeRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.localArchs {
		if err := packageLocalNativeArch(cfg, "rpm", arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageLocalNative builds native vectis-local DEB and RPM packages.
func PackageLocalNative() error {
	if err := PackageLocalNativeDeb(); err != nil {
		return err
	}

	return PackageLocalNativeRPM()
}

// PackageCommonDebArch builds the vectis-common DEB for one architecture.
func PackageCommonDebArch(arch string) error {
	cfg := newPackageConfig()
	return packageCommonDebArch(cfg, arch)
}

// PackageCommonRPMArch builds the vectis-common RPM for one architecture.
func PackageCommonRPMArch(arch string) error {
	cfg := newPackageConfig()
	return packageCommonRPMArch(cfg, arch)
}

// PackageCommonDeb builds vectis-common DEBs for PACKAGE_ARCHES.
func PackageCommonDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageCommonDebArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageCommonRPM builds vectis-common RPMs for PACKAGE_ARCHES.
func PackageCommonRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageCommonRPMArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageCommon builds vectis-common DEB and RPM packages.
func PackageCommon() error {
	if err := PackageCommonDeb(); err != nil {
		return err
	}

	return PackageCommonRPM()
}

// PackageServiceDebArch builds one service DEB for one architecture.
func PackageServiceDebArch(arch, app string) error {
	cfg := newPackageConfig()
	return packageServiceDebArch(cfg, arch, app)
}

// PackageServiceRPMArch builds one service RPM for one architecture.
func PackageServiceRPMArch(arch, app string) error {
	cfg := newPackageConfig()
	return packageServiceRPMArch(cfg, arch, app)
}

// PackageServiceDeb builds service DEBs for PACKAGE_ARCHES.
func PackageServiceDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		for _, app := range cfg.serviceApps {
			if err := packageServiceDebArch(cfg, arch, app); err != nil {
				return err
			}
		}
	}

	return nil
}

// PackageServiceRPM builds service RPMs for PACKAGE_ARCHES.
func PackageServiceRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		for _, app := range cfg.serviceApps {
			if err := packageServiceRPMArch(cfg, arch, app); err != nil {
				return err
			}
		}
	}

	return nil
}

// PackageService builds per-service DEB and RPM packages.
func PackageService() error {
	if err := PackageServiceDeb(); err != nil {
		return err
	}

	return PackageServiceRPM()
}

// PackageServicesDebArch builds the aggregate vectis-services DEB for one architecture.
func PackageServicesDebArch(arch string) error {
	cfg := newPackageConfig()
	return packageServicesDebArch(cfg, arch)
}

// PackageServicesRPMArch builds the aggregate vectis-services RPM for one architecture.
func PackageServicesRPMArch(arch string) error {
	cfg := newPackageConfig()
	return packageServicesRPMArch(cfg, arch)
}

// PackageServicesDeb builds aggregate vectis-services DEBs for PACKAGE_ARCHES.
func PackageServicesDeb() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageServicesDebArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageServicesRPM builds aggregate vectis-services RPMs for PACKAGE_ARCHES.
func PackageServicesRPM() error {
	cfg := newPackageConfig()
	for _, arch := range cfg.archs {
		if err := packageServicesRPMArch(cfg, arch); err != nil {
			return err
		}
	}

	return nil
}

// PackageServices builds aggregate vectis-services DEB and RPM packages.
func PackageServices() error {
	if err := PackageServicesDeb(); err != nil {
		return err
	}

	return PackageServicesRPM()
}

// PackageLinux builds Linux CLI and service packages.
func PackageLinux() error {
	if err := PackageCLI(); err != nil {
		return err
	}

	return PackageServices()
}

func newPackageConfig() packageConfig {
	outDir := envDefault("PACKAGE_OUT", filepath.Join("artifacts", "packages"))
	arch := envDefault("PACKAGE_ARCH", goArch())
	return packageConfig{
		outDir:      outDir,
		buildDir:    envDefault("PACKAGE_BUILD_DIR", filepath.Join(outDir, "build")),
		arch:        arch,
		archs:       fieldsEnv("PACKAGE_ARCHES", "amd64 arm64"),
		serviceApps: fieldsEnv("PACKAGE_SERVICE_APPS", defaultPackageServiceApps),
		localArchs:  fieldsEnv("PACKAGE_LOCAL_ARCHES", arch),
		localApps:   fieldsEnv("PACKAGE_LOCAL_APPS", defaultPackageLocalApps),
		version:     envDefault("PACKAGE_VERSION", "0.0.0+"+gitOutput("unknown", "rev-parse", "--short=12", "HEAD")),
		release:     envDefault("PACKAGE_RELEASE", "1"),
	}
}

func packageCLIBinary(cfg packageConfig, arch string) error {
	return buildPackageBinary(cfg.cliBinary(arch), arch, "cli", "0", "-tags=nosqlite")
}

func packageLinuxArtifacts(cfg packageConfig) error {
	if packageLinuxArtifactsReady {
		return nil
	}

	out := cfg.linuxArtifacts()
	if err := os.RemoveAll(out); err != nil {
		return err
	}

	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "run", "./deploy/linux/cmd/render", "-out", out); err != nil {
		return err
	}

	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	if err := os.WriteFile(filepath.Join(out, ".stamp"), stamp, 0o644); err != nil {
		return err
	}

	packageLinuxArtifactsReady = true
	return nil
}

func packageServiceBinary(cfg packageConfig, arch, app string) error {
	if !containsString(cfg.serviceApps, app) {
		return fmt.Errorf("unknown package service app %q", app)
	}

	return buildPackageBinary(cfg.serviceBinary(arch, app), arch, app, "0", "-tags=nosqlite")
}

func packageLocalCGOCheck(arch string) error {
	_ = arch
	hostOS := goEnv("GOOS")
	if hostOS == "" {
		hostOS = runtime.GOOS
	}

	if hostOS != "linux" && !truthy(os.Getenv("PACKAGE_LOCAL_ALLOW_CROSS_CGO")) {
		return fmt.Errorf("vectis-local Linux packages require a CGO Linux build. Run this target on Linux or set PACKAGE_LOCAL_ALLOW_CROSS_CGO=1 with a working Linux C toolchain")
	}

	return nil
}

func packageLocalWrapper(cfg packageConfig) error {
	path := cfg.localWrapper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec /usr/lib/vectis-local/bin/vectis-local \"$@\"\n"), 0o755); err != nil {
		return err
	}

	return os.Chmod(path, 0o755)
}

func packageLocalBinary(cfg packageConfig, arch, app string) error {
	if !containsString(cfg.localApps, app) {
		return fmt.Errorf("unknown package local app %q", app)
	}

	if err := packageLocalCGOCheck(arch); err != nil {
		return err
	}

	return buildPackageBinary(cfg.localBinary(arch, app), arch, app, "1", "")
}

func packageCLIDebArch(cfg packageConfig, arch string) error {
	if err := packageCLIBinary(cfg, arch); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-cli", "deb", arch, map[string]string{
		"vectis-cli": cfg.cliBinary(arch),
	})
}

func packageCLIRPMArch(cfg packageConfig, arch string) error {
	if err := packageCLIBinary(cfg, arch); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-cli", "rpm", arch, map[string]string{
		"vectis-cli": cfg.cliBinary(arch),
	})
}

func packageLocalDispatch(cfg packageConfig, format, arch string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	args := []string{
		"run", "./deploy/package/cmd/build-local",
		"--format", format,
		"--arch", arch,
		"--workdir", cwd,
		"--mage", envDefault("PACKAGE_LOCAL_MAGE", envDefault("MAGE", "mage")),
		"--provider", envDefault("PACKAGE_LOCAL_VM_PROVIDER", "auto"),
		"--provider-path", os.Getenv("PACKAGE_LOCAL_VM_PROVIDER_PATH"),
		"--instance", envDefault("PACKAGE_LOCAL_VM_INSTANCE", envDefault("PACKER_PACKAGE_BUILDER_INSTANCE", "vectis-package-builder")),
		"--timeout", envDefault("PACKAGE_LOCAL_VM_TIMEOUT", "30m"),
		"--allow-cross-cgo=" + envDefault("PACKAGE_LOCAL_ALLOW_CROSS_CGO", "0"),
		"--keep-vm=" + envDefault("PACKAGE_LOCAL_VM_KEEP", "0"),
		"--vm-workspace-root", envDefault("PACKAGE_LOCAL_VM_WORKSPACE_ROOT", envDefault("PACKER_PACKAGE_BUILDER_WORKSPACE_ROOT", "/var/tmp/vectis-package-local-workspaces")),
		"--vm-cache-root", envDefault("PACKAGE_LOCAL_VM_CACHE_ROOT", envDefault("PACKER_PACKAGE_BUILDER_CACHE_ROOT", "/var/tmp/vectis-package-local-cache")),
		"--vm-preserve-env=" + envDefault("PACKAGE_LOCAL_VM_PRESERVE_ENV", "0"),
		"--vm-go", envDefault("PACKAGE_LOCAL_VM_GO", "go"),
		"--env", "PACKAGE_OUT=" + cfg.outDir,
		"--env", "PACKAGE_BUILD_DIR=" + cfg.buildDir,
		"--env", "PACKAGE_VERSION=" + cfg.version,
		"--env", "PACKAGE_RELEASE=" + cfg.release,
		"--env", "PACKAGE_ARCH=" + arch,
		"--env", "PACKAGE_LOCAL_ARCHES=" + arch,
		"--env", "PACKAGE_LOCAL_APPS=" + strings.Join(cfg.localApps, " "),
	}

	env := map[string]string{
		"PACKER_VM_PREP_VERSION": envDefault("PACKER_VM_PREP_VERSION", "2"),
	}

	return run("", env, goCommand(), args...)
}

func packageLocalNativeArch(cfg packageConfig, format, arch string) error {
	if err := packageLocalWrapper(cfg); err != nil {
		return err
	}

	for _, app := range cfg.localApps {
		if err := packageLocalBinary(cfg, arch, app); err != nil {
			return err
		}
	}

	return runPackageBuild(cfg, "vectis-local", format, arch, map[string]string{
		"vectis-local-binaries": cfg.localBinDir(arch),
		"vectis-local-wrapper":  cfg.localWrapper(),
	})
}

func packageCommonDebArch(cfg packageConfig, arch string) error {
	if err := packageLinuxArtifacts(cfg); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-common", "deb", arch, map[string]string{
		"linux-artifacts": cfg.linuxArtifacts(),
	})
}

func packageCommonRPMArch(cfg packageConfig, arch string) error {
	if err := packageLinuxArtifacts(cfg); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-common", "rpm", arch, map[string]string{
		"linux-artifacts": cfg.linuxArtifacts(),
	})
}

func packageServiceDebArch(cfg packageConfig, arch, app string) error {
	if err := packageLinuxArtifacts(cfg); err != nil {
		return err
	}

	if err := packageServiceBinary(cfg, arch, app); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-"+app, "deb", arch, map[string]string{
		"linux-artifacts": cfg.linuxArtifacts(),
		"vectis-" + app:   cfg.serviceBinary(arch, app),
	})
}

func packageServiceRPMArch(cfg packageConfig, arch, app string) error {
	if err := packageLinuxArtifacts(cfg); err != nil {
		return err
	}

	if err := packageServiceBinary(cfg, arch, app); err != nil {
		return err
	}

	return runPackageBuild(cfg, "vectis-"+app, "rpm", arch, map[string]string{
		"linux-artifacts": cfg.linuxArtifacts(),
		"vectis-" + app:   cfg.serviceBinary(arch, app),
	})
}

func packageServicesDebArch(cfg packageConfig, arch string) error {
	if err := packageCommonDebArch(cfg, arch); err != nil {
		return err
	}

	for _, app := range cfg.serviceApps {
		if err := packageServiceDebArch(cfg, arch, app); err != nil {
			return err
		}
	}

	return runPackageBuild(cfg, "vectis-services", "deb", arch, nil)
}

func packageServicesRPMArch(cfg packageConfig, arch string) error {
	if err := packageCommonRPMArch(cfg, arch); err != nil {
		return err
	}

	for _, app := range cfg.serviceApps {
		if err := packageServiceRPMArch(cfg, arch, app); err != nil {
			return err
		}
	}

	return runPackageBuild(cfg, "vectis-services", "rpm", arch, nil)
}

func buildPackageBinary(out, arch, app, cgo, tags string) error {
	if _, ok := builtPackageBinaries[out]; ok {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return err
	}

	args := []string{"build"}
	if tags != "" {
		args = append(args, tags)
	}

	args = append(args, "-ldflags", buildLDFlags(false), "-o", out, "./"+filepath.ToSlash(filepath.Join("cmd", app)))
	env := map[string]string{
		"CGO_ENABLED": cgo,
		"GOARCH":      arch,
		"GOOS":        "linux",
	}

	if err := run("", env, goCommand(), args...); err != nil {
		return err
	}

	builtPackageBinaries[out] = struct{}{}
	return nil
}

func runPackageBuild(cfg packageConfig, name, format, arch string, inputs map[string]string) error {
	args := []string{
		"run", "./deploy/package/cmd/build",
		"--package", name,
		"--format", format,
		"--out", cfg.outDir,
		"--version", cfg.version,
		"--release", cfg.release,
		"--arch", arch,
	}

	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	for _, key := range keys {
		args = append(args, "--input", key+"="+inputs[key])
	}

	return run("", nil, goCommand(), args...)
}

func runPackageE2E(name string, env map[string]string) error {
	env["PACKER_VM_PREP_VERSION"] = envDefault("PACKER_VM_PREP_VERSION", "2")
	return run("", env, goCommand(), "test", "-tags=e2e", "./tests/e2e/package/linux", "-run", name, "-count=1", "-v")
}

func packageServicesDebPaths(cfg packageConfig, arch string) []string {
	paths := []string{cfg.commonDebPath(arch)}
	for _, app := range cfg.serviceApps {
		paths = append(paths, cfg.serviceDebPath(arch, app))
	}

	return append(paths, cfg.servicesDebPath(arch))
}

func packageServicesRPMPaths(cfg packageConfig, arch string) []string {
	paths := []string{cfg.commonRPMPath(arch)}
	for _, app := range cfg.serviceApps {
		paths = append(paths, cfg.serviceRPMPath(arch, app))
	}

	return append(paths, cfg.servicesRPMPath(arch))
}

func packagePathEnv(paths []string) (string, error) {
	absolute := make([]string, 0, len(paths))
	for _, path := range paths {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}

		absolute = append(absolute, abs)
	}

	return strings.Join(absolute, string(os.PathListSeparator)), nil
}

func mustAbsPath(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}

	return abs
}

func (cfg packageConfig) linuxArtifacts() string {
	return filepath.Join(cfg.buildDir, "linux-artifacts")
}

func (cfg packageConfig) cliBinary(arch string) string {
	return filepath.Join(cfg.buildDir, "linux-"+arch, "vectis-cli")
}

func (cfg packageConfig) serviceBinary(arch, app string) string {
	return filepath.Join(cfg.buildDir, "linux-"+arch, "vectis-"+app)
}

func (cfg packageConfig) localBinDir(arch string) string {
	return filepath.Join(cfg.buildDir, "linux-"+arch+"-local", "bin")
}

func (cfg packageConfig) localBinary(arch, app string) string {
	return filepath.Join(cfg.localBinDir(arch), "vectis-"+app)
}

func (cfg packageConfig) localWrapper() string {
	return filepath.Join(cfg.buildDir, "vectis-local-wrapper")
}

func (cfg packageConfig) cliDebPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-cli_%s-%s_%s.deb", cfg.version, cfg.release, packageDebArch(arch)))
}

func (cfg packageConfig) cliRPMPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-cli-%s-%s.%s.rpm", packageRPMVersion(cfg.version), packageRPMVersion(cfg.release), packageRPMArch(arch)))
}

func (cfg packageConfig) commonDebPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-common_%s-%s_%s.deb", cfg.version, cfg.release, packageDebArch(arch)))
}

func (cfg packageConfig) commonRPMPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-common-%s-%s.%s.rpm", packageRPMVersion(cfg.version), packageRPMVersion(cfg.release), packageRPMArch(arch)))
}

func (cfg packageConfig) serviceDebPath(arch, app string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-%s_%s-%s_%s.deb", app, cfg.version, cfg.release, packageDebArch(arch)))
}

func (cfg packageConfig) serviceRPMPath(arch, app string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-%s-%s-%s.%s.rpm", app, packageRPMVersion(cfg.version), packageRPMVersion(cfg.release), packageRPMArch(arch)))
}

func (cfg packageConfig) localDebPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-local_%s-%s_%s.deb", cfg.version, cfg.release, packageDebArch(arch)))
}

func (cfg packageConfig) localRPMPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-local-%s-%s.%s.rpm", packageRPMVersion(cfg.version), packageRPMVersion(cfg.release), packageRPMArch(arch)))
}

func (cfg packageConfig) servicesDebPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-services_%s-%s_%s.deb", cfg.version, cfg.release, packageDebArch(arch)))
}

func (cfg packageConfig) servicesRPMPath(arch string) string {
	return filepath.Join(cfg.outDir, fmt.Sprintf("vectis-services-%s-%s.%s.rpm", packageRPMVersion(cfg.version), packageRPMVersion(cfg.release), packageRPMArch(arch)))
}

func packageDebArch(arch string) string {
	switch arch {
	case "x86_64":
		return "amd64"
	case "aarch64":
		return "arm64"
	case "386":
		return "i386"
	default:
		return arch
	}
}

func packageRPMArch(arch string) string {
	switch arch {
	case "amd64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	default:
		return arch
	}
}

func packageRPMVersion(value string) string {
	return strings.ReplaceAll(value, "-", "_")
}
