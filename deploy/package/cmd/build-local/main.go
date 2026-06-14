package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"vectis/internal/interfaces"
	"vectis/internal/platform"
)

const (
	defaultBuildVMProvider = platform.VirtualMachineProviderAuto
	defaultBuildVMInstance = "vectis-package-local-build"
	defaultBuildVMTemplate = "ubuntu-lts"
	defaultBuildTimeout    = 30 * time.Minute
)

type envFlags []string

type options struct {
	Format        string
	Arch          string
	WorkDir       string
	MakePath      string
	Provider      string
	ProviderPath  string
	Instance      string
	Template      string
	Timeout       time.Duration
	VMGo          string
	VMWorkspace   string
	VMCacheRoot   string
	VMBootstrap   bool
	AllowCrossCGO bool
	KeepVM        bool
	VMPreserveEnv bool
	Env           []string
}

func (f *envFlags) String() string {
	if f == nil {
		return ""
	}

	return strings.Join(*f, ",")
}

func (f *envFlags) Set(raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	if !ok || strings.TrimSpace(key) == "" {
		return fmt.Errorf("env must be name=value")
	}

	*f = append(*f, strings.TrimSpace(key)+"="+value)
	return nil
}

func main() {
	if err := run(os.Args[1:], runtime.GOOS, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "vectis-local package build failed: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, hostOS string, stdout, stderr io.Writer) error {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	return runBuild(ctx, opts, hostOS, stdout, stderr)
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var (
		opts       = defaultOptionsFromEnv()
		timeoutRaw = defaultTimeoutFromEnv()
		env        envFlags
	)

	flags := flag.NewFlagSet("build-local", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.Format, "format", opts.Format, "Package format to build: deb or rpm")
	flags.StringVar(&opts.Arch, "arch", opts.Arch, "Target architecture, using Go architecture names")
	flags.StringVar(&opts.WorkDir, "workdir", opts.WorkDir, "Workspace directory to build in")
	flags.StringVar(&opts.MakePath, "make", opts.MakePath, "make executable available on the selected build host")
	flags.StringVar(&opts.Provider, "provider", opts.Provider, "VM provider for non-Linux hosts")
	flags.StringVar(&opts.ProviderPath, "provider-path", opts.ProviderPath, "Path to the VM provider executable")
	flags.StringVar(&opts.Instance, "instance", opts.Instance, "VM instance for non-Linux hosts")
	flags.StringVar(&opts.Template, "template", opts.Template, "VM template used when creating the build instance")
	flags.StringVar(&timeoutRaw, "timeout", timeoutRaw, "Overall build timeout")
	flags.StringVar(&opts.VMGo, "vm-go", opts.VMGo, "Go executable to use inside the VM")
	flags.StringVar(&opts.VMWorkspace, "vm-workspace-root", opts.VMWorkspace, "Guest directory for writable package build workspaces")
	flags.StringVar(&opts.VMCacheRoot, "vm-cache-root", opts.VMCacheRoot, "Guest directory for persistent Go build and module caches")
	flags.BoolVar(&opts.VMBootstrap, "vm-bootstrap", opts.VMBootstrap, "Install local package build prerequisites in apt-based VMs when missing")
	flags.BoolVar(&opts.AllowCrossCGO, "allow-cross-cgo", opts.AllowCrossCGO, "Allow host builds even when the host is not Linux")
	flags.BoolVar(&opts.KeepVM, "keep-vm", opts.KeepVM, "Leave the build VM running after completion")
	flags.BoolVar(&opts.VMPreserveEnv, "vm-preserve-env", opts.VMPreserveEnv, "Preserve VM shell environment while applying explicit build env")
	flags.Var(&env, "env", "Environment variable for the native make target; may be repeated")

	if err := flags.Parse(args); err != nil {
		return options{}, err
	}

	opts.Env = append(opts.Env, env...)
	timeout, err := time.ParseDuration(timeoutRaw)
	if err != nil {
		return options{}, fmt.Errorf("parse timeout %q: %w", timeoutRaw, err)
	}

	opts.Timeout = timeout
	return normalizeOptions(opts)
}

func defaultOptionsFromEnv() options {
	return options{
		WorkDir:       ".",
		MakePath:      envOrDefault("PACKAGE_LOCAL_MAKE", "make"),
		Provider:      envOrDefault("PACKAGE_LOCAL_VM_PROVIDER", defaultBuildVMProvider),
		ProviderPath:  strings.TrimSpace(os.Getenv("PACKAGE_LOCAL_VM_PROVIDER_PATH")),
		Instance:      envOrDefault("PACKAGE_LOCAL_VM_INSTANCE", defaultBuildVMInstance),
		Template:      envOrDefault("PACKAGE_LOCAL_VM_TEMPLATE", defaultBuildVMTemplate),
		Timeout:       defaultBuildTimeout,
		VMGo:          envOrDefault("PACKAGE_LOCAL_VM_GO", "go"),
		VMWorkspace:   envOrDefault("PACKAGE_LOCAL_VM_WORKSPACE_ROOT", "/tmp/vectis-package-local-workspaces"),
		VMCacheRoot:   envOrDefault("PACKAGE_LOCAL_VM_CACHE_ROOT", "/var/tmp/vectis-package-local-cache"),
		VMBootstrap:   truthyDefault(os.Getenv("PACKAGE_LOCAL_VM_BOOTSTRAP"), true),
		AllowCrossCGO: truthy(os.Getenv("PACKAGE_LOCAL_ALLOW_CROSS_CGO")),
		KeepVM:        truthy(os.Getenv("PACKAGE_LOCAL_VM_KEEP")),
		VMPreserveEnv: truthy(os.Getenv("PACKAGE_LOCAL_VM_PRESERVE_ENV")),
	}
}

func defaultTimeoutFromEnv() string {
	if value := strings.TrimSpace(os.Getenv("PACKAGE_LOCAL_VM_TIMEOUT")); value != "" {
		return value
	}

	return defaultBuildTimeout.String()
}

func normalizeOptions(opts options) (options, error) {
	opts.Format = strings.ToLower(strings.TrimSpace(opts.Format))
	if opts.Format != "deb" && opts.Format != "rpm" {
		return options{}, fmt.Errorf("format must be deb or rpm")
	}

	opts.Arch = strings.TrimSpace(opts.Arch)
	if opts.Arch == "" {
		return options{}, fmt.Errorf("arch is required")
	}

	opts.MakePath = strings.TrimSpace(opts.MakePath)
	if opts.MakePath == "" {
		return options{}, fmt.Errorf("make executable is required")
	}

	workDir := strings.TrimSpace(opts.WorkDir)
	if workDir == "" {
		workDir = "."
	}

	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return options{}, fmt.Errorf("resolve workdir %q: %w", workDir, err)
	}

	opts.WorkDir = absWorkDir
	opts.Provider = platform.ResolveVirtualMachineProvider(opts.Provider)
	opts.ProviderPath = strings.TrimSpace(opts.ProviderPath)
	opts.Instance = strings.TrimSpace(opts.Instance)
	if opts.Instance == "" {
		opts.Instance = defaultBuildVMInstance
	}

	opts.Template = strings.TrimSpace(opts.Template)
	if opts.Template == "" {
		opts.Template = defaultBuildVMTemplate
	}

	opts.VMGo = strings.TrimSpace(opts.VMGo)
	if opts.VMGo == "" {
		opts.VMGo = "go"
	}

	opts.VMWorkspace = strings.TrimSpace(opts.VMWorkspace)
	if opts.VMWorkspace == "" {
		opts.VMWorkspace = "/tmp/vectis-package-local-workspaces"
	}

	opts.VMCacheRoot = strings.TrimSpace(opts.VMCacheRoot)
	if opts.VMCacheRoot == "" {
		opts.VMCacheRoot = "/var/tmp/vectis-package-local-cache"
	}

	if opts.Timeout <= 0 {
		return options{}, fmt.Errorf("timeout must be positive")
	}

	return opts, nil
}

func runBuild(ctx context.Context, opts options, hostOS string, stdout, stderr io.Writer) error {
	target := nativeTarget(opts.Format, opts.Arch)
	if useNativeBuild(hostOS, opts.AllowCrossCGO) {
		fmt.Fprintf(stdout, "building %s on host with target %s\n", localPackageLabel(opts), target)
		return runMake(ctx, opts, target, stdout, stderr)
	}

	fmt.Fprintf(stdout, "building %s in %s VM %q with target %s\n", localPackageLabel(opts), opts.Provider, opts.Instance, target)
	return runMakeInVM(ctx, opts, target, stdout, stderr)
}

func runMake(ctx context.Context, opts options, target string, stdout, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, opts.MakePath, target) //#nosec G204
	cmd.Dir = opts.WorkDir
	cmd.Env = append(os.Environ(), opts.Env...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("run %s: %w", target, err)
	}

	return nil
}

func runMakeInVM(ctx context.Context, opts options, target string, stdout, stderr io.Writer) error {
	manager, err := platform.NewVirtualMachineManager(platform.VirtualMachineManagerConfig{
		Provider:     opts.Provider,
		ProviderPath: opts.ProviderPath,
		Stdout:       stdout,
		Stderr:       stderr,
	})

	if err != nil {
		return err
	}

	if err := manager.CheckAvailable(); err != nil {
		return err
	}

	exists, err := manager.InstanceExists(ctx, opts.Instance)
	if err != nil {
		return fmt.Errorf("check build VM %q: %w", opts.Instance, err)
	}

	if !exists {
		fmt.Fprintf(stdout, "creating %s VM %q from template %q\n", manager.Provider(), opts.Instance, opts.Template)
		if err := manager.Create(ctx, opts.Instance, opts.Template); err != nil {
			return fmt.Errorf("create build VM %q: %w", opts.Instance, err)
		}
	}

	if err := manager.Start(ctx, opts.Instance); err != nil {
		return fmt.Errorf("start build VM %q: %w", opts.Instance, err)
	}

	guestWorkDir, err := prepareVMWorkspace(ctx, manager, opts, stdout)
	if err != nil {
		return err
	}

	buildErr := runVMBuildCommand(ctx, opts, target, guestWorkDir, stdout, stderr)
	if buildErr == nil {
		buildErr = copyVMOutput(ctx, manager, opts, guestWorkDir)
	}

	if opts.KeepVM {
		return buildErr
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := manager.Stop(stopCtx, opts.Instance); err != nil && buildErr == nil {
		return fmt.Errorf("stop build VM %q: %w", opts.Instance, err)
	}

	return buildErr
}

func prepareVMWorkspace(ctx context.Context, manager platform.VirtualMachineManager, opts options, stdout io.Writer) (string, error) {
	workspaceParent := path.Join(opts.VMWorkspace, safeGuestName(filepath.Base(opts.WorkDir)+"-"+opts.Format+"-"+opts.Arch))
	if err := manager.Shell(ctx, opts.Instance, nil, "rm", "-rf", workspaceParent); err != nil {
		return "", fmt.Errorf("clean build workspace %q in VM %q: %w", workspaceParent, opts.Instance, err)
	}

	if err := manager.Shell(ctx, opts.Instance, nil, "mkdir", "-p", workspaceParent); err != nil {
		return "", fmt.Errorf("create build workspace %q in VM %q: %w", workspaceParent, opts.Instance, err)
	}

	fmt.Fprintf(stdout, "copying worktree to VM workspace %s\n", workspaceParent)
	if err := manager.CopyDir(ctx, opts.WorkDir, opts.Instance, workspaceParent); err != nil {
		return "", fmt.Errorf("copy worktree to build VM %q: %w", opts.Instance, err)
	}

	nestedWorkDir := path.Join(workspaceParent, filepath.Base(opts.WorkDir))
	for _, guestWorkDir := range []string{nestedWorkDir, workspaceParent} {
		if err := manager.Shell(ctx, opts.Instance, nil, "test", "-f", path.Join(guestWorkDir, "Makefile")); err == nil {
			return guestWorkDir, nil
		}
	}

	return "", fmt.Errorf("copied worktree in VM %q did not contain a Makefile under %s", opts.Instance, workspaceParent)
}

func runVMBuildCommand(ctx context.Context, opts options, target, guestWorkDir string, stdout, stderr io.Writer) error {
	executor, err := platform.NewVirtualMachineCommandExecutor(platform.VirtualMachineConfig{
		Provider:     opts.Provider,
		Instance:     opts.Instance,
		ProviderPath: opts.ProviderPath,
		Start:        true,
		PreserveEnv:  opts.VMPreserveEnv,
	})

	if err != nil {
		return err
	}

	env := vmBuildEnv(opts.Env, opts.VMGo, opts.MakePath, opts.VMCacheRoot)
	if err := runVMProcess(ctx, executor, "sh", []string{"-c", vmPreflightScript}, "", env, stdout, stderr); err != nil {
		if !opts.VMBootstrap {
			return fmt.Errorf("build VM %q is missing local package prerequisites (go, make, and a C compiler): %w", opts.Instance, err)
		}

		fmt.Fprintf(stdout, "bootstrapping local package build prerequisites in VM %q\n", opts.Instance)
		if bootstrapErr := runVMProcess(ctx, executor, "sh", []string{"-c", vmBootstrapScript}, "", env, stdout, stderr); bootstrapErr != nil {
			return fmt.Errorf("bootstrap build VM %q: %w", opts.Instance, bootstrapErr)
		}

		if preflightErr := runVMProcess(ctx, executor, "sh", []string{"-c", vmPreflightScript}, "", env, stdout, stderr); preflightErr != nil {
			return fmt.Errorf("build VM %q is missing local package prerequisites after bootstrap: %w", opts.Instance, preflightErr)
		}
	}

	if err := runVMProcess(ctx, executor, opts.MakePath, []string{target}, guestWorkDir, env, stdout, stderr); err != nil {
		return fmt.Errorf("run %s in build VM %q: %w", target, opts.Instance, err)
	}

	return nil
}

func copyVMOutput(ctx context.Context, manager platform.VirtualMachineManager, opts options, guestWorkDir string) error {
	relOut, err := relativePackageOut(opts)
	if err != nil {
		return err
	}

	guestOut := path.Join(guestWorkDir, filepath.ToSlash(relOut))
	guestStage := path.Join(opts.VMWorkspace, safeGuestName("output-"+opts.Format+"-"+opts.Arch))
	if err := manager.Shell(ctx, opts.Instance, nil, "rm", "-rf", guestStage); err != nil {
		return fmt.Errorf("clean package output stage %q in VM %q: %w", guestStage, opts.Instance, err)
	}

	if err := manager.Shell(ctx, opts.Instance, nil, "mkdir", "-p", guestStage); err != nil {
		return fmt.Errorf("create package output stage %q in VM %q: %w", guestStage, opts.Instance, err)
	}

	pattern := "vectis-local*." + opts.Format
	if err := manager.Shell(ctx, opts.Instance, nil, "sh", "-c", vmStageOutputScript, "vectis-stage-output", guestOut, guestStage, pattern); err != nil {
		return fmt.Errorf("stage package outputs from build VM %q: %w", opts.Instance, err)
	}

	localOut := filepath.Join(opts.WorkDir, relOut)
	if err := os.MkdirAll(localOut, 0o755); err != nil {
		return fmt.Errorf("create local package output %q: %w", localOut, err)
	}

	if err := manager.CopyDirFrom(ctx, opts.Instance, guestStage, localOut); err != nil {
		return fmt.Errorf("copy package outputs from build VM %q: %w", opts.Instance, err)
	}

	return nil
}

func runVMProcess(ctx context.Context, executor *platform.VirtualMachineCommandExecutor, path string, args []string, workDir string, env []string, stdout, stderr io.Writer) error {
	process, err := executor.Start(ctx, path, args, workDir, env)
	if err != nil {
		return err
	}

	return waitProcess(process, stdout, stderr)
}

func waitProcess(process interfaces.Process, stdout, stderr io.Writer) error {
	errs := make(chan error, 2)
	go func() {
		_, err := io.Copy(stdout, process.Stdout())
		errs <- err
	}()

	go func() {
		_, err := io.Copy(stderr, process.Stderr())
		errs <- err
	}()

	waitErr := process.Wait()
	stdoutErr := <-errs
	stderrErr := <-errs

	if waitErr != nil {
		return waitErr
	}

	if stdoutErr != nil {
		return fmt.Errorf("copy stdout: %w", stdoutErr)
	}

	if stderrErr != nil {
		return fmt.Errorf("copy stderr: %w", stderrErr)
	}

	return nil
}

func nativeTarget(format, arch string) string {
	return "package-local-native-" + format + "-" + arch
}

func useNativeBuild(hostOS string, allowCrossCGO bool) bool {
	return hostOS == "linux" || allowCrossCGO
}

func localPackageLabel(opts options) string {
	return "vectis-local " + opts.Format + "/" + opts.Arch
}

func relativePackageOut(opts options) (string, error) {
	out := envValue(opts.Env, "PACKAGE_OUT", "artifacts/packages")
	out = filepath.Clean(out)
	if !filepath.IsAbs(out) {
		return out, nil
	}

	rel, err := filepath.Rel(opts.WorkDir, out)
	if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return "", fmt.Errorf("PACKAGE_OUT must be relative to the worktree for VM builds: %s", out)
	}

	return rel, nil
}

func envValue(env []string, name, fallback string) string {
	prefix := name + "="
	for i := len(env) - 1; i >= 0; i-- {
		if strings.HasPrefix(env[i], prefix) {
			value := strings.TrimSpace(strings.TrimPrefix(env[i], prefix))
			if value != "" {
				return value
			}
		}
	}

	return fallback
}

func safeGuestName(raw string) string {
	raw = strings.TrimSpace(raw)
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}

	value := strings.Trim(b.String(), "-._")
	if value == "" {
		return "workspace"
	}

	return value
}

func vmBuildEnv(extra []string, vmGo, makePath, cacheRoot string) []string {
	cacheRoot = strings.TrimSpace(cacheRoot)
	if cacheRoot == "" {
		cacheRoot = "/var/tmp/vectis-package-local-cache"
	}

	env := []string{
		"HOME=/tmp",
		"PATH=/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"GOCACHE=" + path.Join(cacheRoot, "go-build"),
		"GOMODCACHE=" + path.Join(cacheRoot, "gomod"),
		"GOTOOLCHAIN=auto",
	}

	if strings.TrimSpace(vmGo) != "" {
		env = append(env, "GO="+strings.TrimSpace(vmGo))
	}

	if strings.TrimSpace(makePath) != "" {
		env = append(env, "VECTIS_PACKAGE_LOCAL_MAKE="+strings.TrimSpace(makePath))
	}

	return append(env, extra...)
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}

	return fallback
}

func truthy(raw string) bool {
	return truthyDefault(raw, false)
}

func truthyDefault(raw string, fallback bool) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}

	switch strings.ToLower(raw) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		return fallback
	}
}

const vmPreflightScript = `set -eu
: "${GO:=go}"
: "${VECTIS_PACKAGE_LOCAL_MAKE:=make}"
mkdir -p "$GOCACHE" "$GOMODCACHE"
command -v "$GO" >/dev/null
command -v "$VECTIS_PACKAGE_LOCAL_MAKE" >/dev/null
if command -v cc >/dev/null || command -v gcc >/dev/null || command -v clang >/dev/null; then
	exit 0
fi
echo "no C compiler found in build VM" >&2
exit 1`

const vmBootstrapScript = `set -eu
if ! command -v apt-get >/dev/null; then
	echo "automatic bootstrap currently supports apt-based Linux build VMs" >&2
	exit 1
fi
sudo apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates build-essential golang-go
`

const vmStageOutputScript = `set -eu
out_dir=$1
stage_dir=$2
pattern=$3
found=0
for artifact in "$out_dir"/$pattern; do
	if [ ! -e "$artifact" ]; then
		continue
	fi
	cp "$artifact" "$stage_dir"/
	found=1
done
if [ "$found" != 1 ]; then
	echo "no package artifacts matching $pattern under $out_dir" >&2
	exit 1
fi
`
