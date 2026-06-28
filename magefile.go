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

type buildConfig struct {
	args      []string
	cgo       string
	strip     bool
	outputExt string
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
