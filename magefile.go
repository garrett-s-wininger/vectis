//go:build mage

package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

var appNames = []string{
	"api",
	"artifact",
	"catalog",
	"cell-ingress",
	"cli",
	"cron",
	"local",
	"log",
	"log-forwarder",
	"orchestrator",
	"queue",
	"reconciler",
	"registry",
	"secrets",
	"spiffe",
	"worker",
	"worker-core",
}

var imageComponentNames = []string{
	"cli",
	"api",
	"artifact",
	"catalog",
	"cell-ingress",
	"cron",
	"log",
	"log-forwarder",
	"orchestrator",
	"queue",
	"reconciler",
	"registry",
	"secrets",
	"spiffe",
	"worker",
	"worker-core",
	"docs",
}

const (
	defaultPackageServiceApps = "api artifact catalog cell-ingress cron docs log log-forwarder orchestrator queue reconciler registry secrets spiffe worker worker-core"
	defaultPackageLocalApps   = "local api artifact catalog cell-ingress cron docs log orchestrator queue reconciler registry secrets worker worker-core"
)

var (
	builtPackageBinaries       = map[string]struct{}{}
	packageLinuxArtifactsReady bool
)

type buildConfig struct {
	args      []string
	cgo       string
	strip     bool
	outputExt string
}

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

// Doctor runs the native development environment preflight.
func Doctor() error {
	args := strings.Fields(os.Getenv("VECTIS_DOCTOR_ARGS"))
	if runtime.GOOS == "windows" {
		shell, err := firstOnPath("pwsh", "powershell.exe", "powershell")
		if err != nil {
			return err

		}
		psArgs := []string{"-NoProfile", "-ExecutionPolicy", "Bypass", "-File", filepath.Join("scripts", "dev-doctor.ps1")}
		psArgs = append(psArgs, args...)
		return run("", nil, shell, psArgs...)
	}

	shArgs := append([]string{filepath.Join("scripts", "dev-doctor.sh")}, args...)
	return run("", nil, "sh", shArgs...)
}

// Proto regenerates Go protobuf stubs.
func Proto() error {
	if err := os.RemoveAll(filepath.Join("api", "gen")); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join("api", "gen", "go"), 0o755); err != nil {
		return err
	}

	protoFiles, err := filepath.Glob(filepath.Join("api", "proto", "*.proto"))
	if err != nil {
		return err
	}

	if len(protoFiles) == 0 {
		return fmt.Errorf("no protobuf files found in %s", filepath.Join("api", "proto"))
	}

	sort.Strings(protoFiles)
	args := []string{
		"-I", filepath.ToSlash(filepath.Join("api", "proto")),
		"--plugin=protoc-gen-go=" + protocPlugin("PROTOC_GEN_GO", "protoc-gen-go"),
		"--plugin=protoc-gen-go-grpc=" + protocPlugin("PROTOC_GEN_GO_GRPC", "protoc-gen-go-grpc"),
		"--go_out=" + filepath.ToSlash(filepath.Join("api", "gen", "go")),
		"--go_opt=paths=source_relative",
		"--go-grpc_out=" + filepath.ToSlash(filepath.Join("api", "gen", "go")),
		"--go-grpc_opt=paths=source_relative",
	}

	for _, file := range protoFiles {
		args = append(args, filepath.ToSlash(file))
	}

	return run("", nil, envDefault("PROTOC", "protoc"), args...)
}

// Build builds the default local binaries.
func Build() error {
	return buildBinaries(buildConfig{
		args:      strings.Fields(os.Getenv("BUILD_OPTS")),
		cgo:       envDefault("CGO_ENABLED", "1"),
		outputExt: hostExecutableExt(),
	})
}

// BuildContainer builds the container-profile binaries with nosqlite tags.
func BuildContainer() error {
	return buildBinaries(buildConfig{
		args:  []string{"-tags=nosqlite"},
		cgo:   "0",
		strip: true,
	})
}

// CiQuick runs the local CI workflow in a clean temporary worktree.
func CiQuick() error {
	status, err := gitStatusPorcelain()
	if err != nil {
		return err
	}

	if status != "" {
		return fmt.Errorf("ci-quick requires a clean git tree. Commit, stash, or discard local changes before running it.\n%s", status)
	}

	tmpDir, err := os.MkdirTemp("", "vectis-ci-quick.*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	worktree := filepath.Join(tmpDir, "worktree")
	if err := run("", nil, "git", "worktree", "add", "--detach", worktree, "HEAD"); err != nil {
		return err
	}
	defer func() {
		_ = run("", nil, "git", "worktree", "remove", "--force", worktree)
	}()

	return run(worktree, nil, goCommand(), "run", "./cmd/worker", "run-local", ".vectis/ci.json", "--workspace", ".")
}

// Clean removes generated local build, test, and docs artifacts.
func Clean() error {
	paths := []string{
		filepath.Join("artifacts", "perf"),
		filepath.Join("artifacts", "deploy"),
		filepath.Join("artifacts", "release-readiness"),
		envDefault("OUT_DIR", "bin"),
		"states",
		filepath.Join("website", ".docusaurus"),
		filepath.Join("website", "build"),
	}

	for _, path := range paths {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	traces, err := filepath.Glob(filepath.Join("formal", "tla", "*_TTrace_*"))
	if err != nil {
		return err
	}

	for _, trace := range traces {
		if err := os.RemoveAll(trace); err != nil {
			return err
		}
	}

	return cleanDirExcept(filepath.Join("cmd", "docs", "embedded"), ".gitkeep")
}

// PodmanGrafanaConfigmaps regenerates Podman Grafana configmaps.
func PodmanGrafanaConfigmaps() error {
	return run("", nil, goCommand(), "run", "./deploy/podman/cmd/generate-grafana-configmaps", "-o", filepath.Join("deploy", "podman", "grafana-configmaps.gen.yaml"))
}

// DocsAssets rebuilds and embeds the documentation site assets.
func DocsAssets() error {
	return buildDocsAssets()
}

// WebsiteA11y runs the website accessibility test suite.
func WebsiteA11y() error {
	browsersPath, err := filepath.Abs(filepath.Join("website", "node_modules", ".cache", "ms-playwright"))
	if err != nil {
		return err
	}

	env := map[string]string{"PLAYWRIGHT_BROWSERS_PATH": browsersPath}
	if err := run("website", env, "npm", "ci"); err != nil {
		return err
	}

	if err := run("website", env, "npx", "playwright", "install", "chromium"); err != nil {
		return err
	}

	return run("website", env, "npm", "run", "test:a11y")
}

// DeployArtifactsRender renders Linux deployment artifacts.
func DeployArtifactsRender() error {
	out := envDefault("DEPLOY_LINUX_OUT", filepath.Join("artifacts", "deploy", "linux"))
	return run("", nil, goCommand(), "run", "./cmd/cli", "deploy", "linux", "render", "--output", out)
}

// DeployArtifactsTest tests Linux deployment artifact rendering.
func DeployArtifactsTest() error {
	return run("", nil, goCommand(), "test", "./deploy/linux/...")
}

func buildBinaries(cfg buildConfig) error {
	apps := append([]string(nil), appNames...)
	if !truthy(os.Getenv("SKIP_WEB_BUILD")) {
		if !truthy(os.Getenv("SKIP_DOCS_ASSETS")) {
			if err := buildDocsAssets(); err != nil {
				return err
			}
		}

		apps = append(apps, "docs")
	}

	outDir := envDefault("OUT_DIR", "bin")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	ldflags := buildLDFlags(cfg.strip)
	for _, app := range apps {
		out := filepath.Join(outDir, "vectis-"+app+cfg.outputExt)
		pkg := "./" + filepath.ToSlash(filepath.Join("cmd", app))
		args := append([]string{"build"}, cfg.args...)
		args = append(args, "-ldflags", ldflags, "-o", out, pkg)
		if err := run("", map[string]string{"CGO_ENABLED": cfg.cgo}, goCommand(), args...); err != nil {
			return err
		}
	}

	return nil
}

// Format applies standard Go fixes, formatting, and module tidying.
func Format() error {
	if err := run("", nil, goCommand(), "fix", "./..."); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "fmt", "./..."); err != nil {
		return err
	}

	return run("", nil, goCommand(), "mod", "tidy")
}

// FormalVerification runs all configured TLA+ formal models.
func FormalVerification() error {
	models := strings.Fields(envDefault("FORMAL_MODELS", "execution reconciliation"))
	if len(models) == 0 {
		return fmt.Errorf("FORMAL_MODELS is empty")
	}

	for _, model := range models {
		if err := runFormalVerification(model); err != nil {
			return err
		}
	}

	return nil
}

// FormalVerificationModel runs one TLA+ formal model named by FORMAL_MODEL.
func FormalVerificationModel() error {
	model := os.Getenv("FORMAL_MODEL")
	if model == "" {
		return fmt.Errorf("FORMAL_MODEL is required")
	}

	return runFormalVerification(model)
}

// FuzzAPIAuth runs the API auth fuzz targets.
func FuzzAPIAuth() error {
	fuzztime := envDefault("FUZZTIME", "30s")
	if err := run("", nil, goCommand(), "test", "-fuzz=FuzzBearerToken", "-fuzztime="+fuzztime, "./internal/api"); err != nil {
		return err
	}

	if err := run("", nil, goCommand(), "test", "-fuzz=FuzzHashAPIToken", "-fuzztime="+fuzztime, "./internal/api"); err != nil {
		return err
	}

	return run("", nil, goCommand(), "test", "-fuzz=FuzzActionForRequest", "-fuzztime="+fuzztime, "./internal/api/authz")
}

// Lint runs route and Go static analysis checks.
func Lint() error {
	if err := LintAPIRoutes(); err != nil {
		return err
	}

	version := envDefault("GOLANGCI_LINT_VERSION", "v2.6.1")
	return run("", nil, goCommand(), "run", "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@"+version, "run", "./...")
}

// LintAPIRoutes runs the API route security lint.
func LintAPIRoutes() error {
	return run("", nil, goCommand(), "run", "./tools/vectis-lint", "./internal/api")
}

// Image builds one component container image. Usage: mage image <component>.
func Image(component string) error {
	if !validImageComponent(component) {
		return fmt.Errorf("unknown image component %q", component)
	}

	return podmanBuild("vectis-"+component+":latest", component)
}

// ImageFull builds the all-in-one container image.
func ImageFull() error {
	return podmanBuild("vectis:latest", "all-in-one")
}

// ImagesAll builds the all-in-one image and all component images.
func ImagesAll() error {
	if err := ImageFull(); err != nil {
		return err
	}

	return ImagesComponents()
}

// ImagesComponents builds the component container images.
func ImagesComponents() error {
	for _, component := range imageComponentNames {
		if component == "docs" && truthy(os.Getenv("SKIP_WEB_BUILD")) {
			continue
		}

		if err := Image(component); err != nil {
			return err
		}
	}

	return nil
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

// Perf runs a benchmark suite through the perf helper.
func Perf() error {
	bin, err := buildPerf()
	if err != nil {
		return err
	}

	args := append([]string{envDefault("SUITE", "queue")}, strings.Fields(os.Getenv("PERF_ARGS"))...)
	return run("", nil, bin, args...)
}

// PerfCompare compares two perf result files.
func PerfCompare() error {
	bin, err := buildPerf()
	if err != nil {
		return err
	}

	return run("", nil, bin, "compare", "--baseline", os.Getenv("BASELINE"), "--current", os.Getenv("CURRENT"))
}

// ReleaseReadinessReport runs release readiness checks.
func ReleaseReadinessReport() error {
	args := []string{"run", "./tools/release-readiness", "--profile", envDefault("RELEASE_READINESS_PROFILE", "local")}
	if checks := os.Getenv("RELEASE_READINESS_CHECKS"); checks != "" {
		args = append(args, "--checks", checks)
	}
	args = append(args, strings.Fields(os.Getenv("RELEASE_READINESS_ARGS"))...)
	return run("", nil, goCommand(), args...)
}

// ReleaseLocalValidate runs the local release validation workflow.
func ReleaseLocalValidate() error {
	if err := TestQuick(); err != nil {
		return err
	}

	if err := DeployArtifactsTest(); err != nil {
		return err
	}

	if err := TestPackage(); err != nil {
		return err
	}

	return Build()
}

// Vulncheck runs govulncheck with the module's declared Go toolchain.
func Vulncheck() error {
	env := map[string]string{
		"GOTOOLCHAIN": "go" + goModGoVersion(),
	}

	version := envDefault("GOVULNCHECK_VERSION", "v1.1.4")
	return run("", env, goCommand(), "run", "golang.org/x/vuln/cmd/govulncheck@"+version, "./...")
}

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
		"PACKER_VM_PREP_VERSION": envDefault("PACKER_VM_PREP_VERSION", "1"),
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
	args := []string{
		"test",
		"-count=1",
		"-timeout=60s",
		"./internal/...",
		"./cmd/...",
		"./api/...",
		"./sdk/...",
		"./examples/...",
		"./tools/...",
	}

	return run("", nil, goCommand(), args...)
}

// TestRace runs the Go race detector suite.
func TestRace() error {
	return run("", nil, goCommand(), "test", "-race", "./...")
}

func buildPerf() (string, error) {
	bin := os.Getenv("PERF_BIN")
	if bin == "" {
		bin = filepath.Join(envDefault("OUT_DIR", "bin"), "vectis-perf"+hostExecutableExt())
	}

	if err := os.MkdirAll(filepath.Dir(bin), 0o755); err != nil {
		return "", err
	}

	args := append([]string{"build"}, strings.Fields(os.Getenv("BUILD_OPTS"))...)
	args = append(args, "-o", bin, "./scripts/perf")
	if err := run("", map[string]string{"CGO_ENABLED": envDefault("CGO_ENABLED", "1")}, goCommand(), args...); err != nil {
		return "", err
	}

	return bin, nil
}

func podmanBuild(tag, target string) error {
	return run("", nil, envDefault("PODMAN", "podman"),
		"build",
		"-t", tag,
		"-f", filepath.Join("build", "Containerfile"),
		"--target", target,
		".",
	)
}

func validImageComponent(component string) bool {
	for _, valid := range imageComponentNames {
		if component == valid {
			return true
		}
	}

	return false
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
		"--make", envDefault("PACKAGE_LOCAL_MAKE", "make"),
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
		"PACKER_VM_PREP_VERSION": envDefault("PACKER_VM_PREP_VERSION", "1"),
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
	env["PACKER_VM_PREP_VERSION"] = envDefault("PACKER_VM_PREP_VERSION", "1")
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

func runPacker(action string, vars []string, dir string) error {
	args := append([]string{action}, vars...)
	args = append(args, dir)
	return run("", nil, envDefault("PACKER", "packer"), args...)
}

func packerDeploySmokeVars() []string {
	return packerVarArgs(map[string]string{
		"instance":           envDefault("PACKER_DEPLOY_SMOKE_INSTANCE", "vectis-deploy-smoke"),
		"base_template":      envDefault("PACKER_DEPLOY_SMOKE_TEMPLATE", "ubuntu-lts"),
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "1"),
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
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "1"),
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
		"prep_version":       envDefault("PACKER_VM_PREP_VERSION", "1"),
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
		"--prep-version", envDefault("PACKER_VM_PREP_VERSION", "1"),
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

func buildDocsAssets() error {
	if err := run("website", nil, "npm", "ci"); err != nil {
		return err
	}

	if err := run("website", nil, "npm", "run", "build"); err != nil {
		return err
	}

	dest := filepath.Join("cmd", "docs", "embedded")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}

	if err := cleanDirExcept(dest, ".gitkeep"); err != nil {
		return err
	}

	if err := copyDir(filepath.Join("website", "build"), dest); err != nil {
		return err
	}

	stamp := []byte(time.Now().UTC().Format(time.RFC3339) + "\n")
	return os.WriteFile(filepath.Join(dest, ".stamp"), stamp, 0o644)
}

func cleanDirExcept(dir string, keep ...string) error {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}

	if err != nil {
		return err
	}

	keepSet := make(map[string]struct{}, len(keep))
	for _, name := range keep {
		keepSet[name] = struct{}{}
	}

	for _, entry := range entries {
		if _, ok := keepSet[entry.Name()]; ok {
			continue
		}

		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}

	return nil
}

func copyDir(src, dest string) error {
	return filepath.WalkDir(src, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		if rel == "." {
			return nil
		}

		target := filepath.Join(dest, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}

		return copyFile(path, target, info.Mode())
	})
}

func copyFile(src, dest string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}

	return out.Close()
}

func buildLDFlags(strip bool) string {
	flags := []string{
		"-X vectis/internal/version.Version=" + gitOutput("dev", "describe", "--tags", "--always", "--dirty"),
		"-X vectis/internal/version.Commit=" + gitOutput("unknown", "rev-parse", "--short=12", "HEAD"),
		"-X vectis/internal/version.BuildDate=" + time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}

	if strip {
		flags = append(flags, "-s", "-w")
	}

	return strings.Join(flags, " ")
}

func gitOutput(fallback string, args ...string) string {
	cmd := exec.Command("git", args...)
	out, err := cmd.Output()
	if err != nil {
		return fallback
	}

	value := strings.TrimSpace(string(out))
	if value == "" {
		return fallback
	}

	return value
}

func gitStatusPorcelain() (string, error) {
	cmd := exec.Command("git", "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

func runFormalVerification(model string) error {
	return run("", nil,
		envDefault("JAVA", "java"),
		"-jar", envDefault("TLA_TOOLS_JAR", "/opt/tla+/tla2tools.jar"),
		"-workers", "auto",
		filepath.Join("formal", "tla", model+".tla"),
		"-config", filepath.Join("formal", "tla", model+".cfg"),
	)
}

func protocPlugin(envName, tool string) string {
	if value := os.Getenv(envName); value != "" {
		return value
	}

	gopath := goEnv("GOPATH")
	if gopath == "" {
		return exeName(tool)
	}

	return filepath.Join(gopath, "bin", exeName(tool))
}

func goEnv(name string) string {
	cmd := exec.Command(goCommand(), "env", name)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func goArch() string {
	if arch := goEnv("GOARCH"); arch != "" {
		return arch
	}

	return runtime.GOARCH
}

func goModGoVersion() string {
	data, err := os.ReadFile("go.mod")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "go" {
				return fields[1]
			}
		}
	}

	if version := strings.TrimPrefix(goEnv("GOVERSION"), "go"); version != "" {
		return version
	}

	return "1.25.10"
}

func goCommand() string {
	return envDefault("GO", "go")
}

func firstOnPath(names ...string) (string, error) {
	for _, name := range names {
		path, err := exec.LookPath(name)
		if err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("none of these commands are on PATH: %s", strings.Join(names, ", "))
}

func exeName(name string) string {
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		return name + ".exe"
	}

	return name
}

func hostExecutableExt() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}

	return ""
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func fieldsEnv(name, fallback string) []string {
	fields := strings.Fields(envDefault(name, fallback))
	if len(fields) > 0 {
		return fields
	}

	return strings.Fields(fallback)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func run(dir string, extraEnv map[string]string, name string, args ...string) error {
	fmt.Println("$", commandLine(name, args))
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}

	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), envPairs(extraEnv)...)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func envPairs(values map[string]string) []string {
	pairs := make([]string, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, key+"="+value)
	}

	sort.Strings(pairs)
	return pairs
}

func commandLine(name string, args []string) string {
	parts := append([]string{name}, args...)
	return strings.Join(parts, " ")
}
