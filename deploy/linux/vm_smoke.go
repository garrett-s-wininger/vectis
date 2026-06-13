package linux

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"vectis/internal/platform"
)

const (
	vmSmokeRemoteArtifactDir = "/tmp/vectis-linux-artifacts"
	vmSmokeMarker            = "vectis linux smoke stub"
	vmSmokeMarkerSource      = "smoke/.vectis-linux-smoke"
	vmSmokeMarkerDestination = "/etc/vectis/.vectis-linux-smoke"
	vmSmokeBinDir            = "smoke/bin"
	vmSmokeRealBinDir        = "smoke/real-bin"
	vmSmokeGuestRoot         = "/opt/vectis-smoke"
	vmSmokeGuestRealBinDir   = vmSmokeGuestRoot + "/bin"
	vmSmokeLocalEnvSource    = "env/vectis-local.env.example"
	vmSmokeLocalHealthURL    = "http://127.0.0.1:8080/health/live"
	vmSmokeGuestWaitTimeout  = 45 * time.Second
	vmSmokeGuestWaitInterval = time.Second
	defaultLimaInstance      = "vectis-deploy-smoke"
	defaultLimaTemplate      = "ubuntu-lts"
)

const (
	VMSmokeProfileUnits = "units"
	VMSmokeProfileLocal = "local"
)

type VMSmokeOptions struct {
	Manager       platform.VirtualMachineManager
	Provider      string
	ProviderPath  string
	Instance      string
	Template      string
	Profile       string
	BinaryDir     string
	ArtifactDir   string
	ManifestPath  string
	KeepArtifacts bool
	Stdout        io.Writer
	Stderr        io.Writer
}

type VMSmokeResult struct {
	Status       string `json:"status"`
	Provider     string `json:"provider,omitempty"`
	Instance     string `json:"instance"`
	Template     string `json:"template,omitempty"`
	Profile      string `json:"profile,omitempty"`
	ArtifactDir  string `json:"artifact_dir,omitempty"`
	ManifestPath string `json:"manifest_path,omitempty"`
	Files        int    `json:"files,omitempty"`
	GuestCleaned bool   `json:"guest_cleaned,omitempty"`
	LocalCleaned bool   `json:"local_cleaned,omitempty"`
}

func RunVMSmokeVerify(ctx context.Context, opts VMSmokeOptions) (VMSmokeResult, error) {
	opts, err := normalizeVMSmokeOptions(opts)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if opts.Profile == VMSmokeProfileLocal && opts.BinaryDir == "" {
		return VMSmokeResult{}, errVMSmokeLocalBinaryDirRequired()
	}

	manager := opts.Manager
	if err := manager.CheckAvailable(); err != nil {
		return VMSmokeResult{}, err
	}

	exists, err := manager.InstanceExists(ctx, opts.Instance)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if !exists {
		vmSmokeLogf(opts.Stdout, "creating %s instance %s from template:%s", manager.Provider(), opts.Instance, opts.Template)
		if err := manager.Create(ctx, opts.Instance, opts.Template); err != nil {
			return VMSmokeResult{}, err
		}
	}

	vmSmokeLogf(opts.Stdout, "starting %s instance %s", manager.Provider(), opts.Instance)
	if err := manager.Start(ctx, opts.Instance); err != nil {
		return VMSmokeResult{}, err
	}

	localCleanup, err := prepareVMSmokeArtifactDir(&opts)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if localCleanup {
		defer func() { _ = os.RemoveAll(opts.ArtifactDir) }()
	}

	vmSmokeLogf(opts.Stdout, "rendering Linux artifacts to %s", opts.ArtifactDir)
	renderResult, err := RenderToDir(RenderOptions{ManifestPath: opts.ManifestPath, OutDir: opts.ArtifactDir})
	if err != nil {
		return VMSmokeResult{}, err
	}

	installPlan, stubBinaries, err := vmSmokeInstallPlan(opts.ManifestPath)
	if err != nil {
		return VMSmokeResult{}, err
	}

	guestArtifacts, err := prepareVMSmokeGuestArtifacts(opts, stubBinaries)
	if err != nil {
		return VMSmokeResult{}, err
	}

	vmSmokeLogf(opts.Stdout, "copying rendered artifacts into %s:%s", opts.Instance, vmSmokeRemoteArtifactDir)
	if err := manager.Shell(ctx, opts.Instance, nil, "sudo", "rm", "-rf", vmSmokeRemoteArtifactDir); err != nil {
		return VMSmokeResult{}, err
	}

	if err := manager.CopyDir(ctx, opts.ArtifactDir, opts.Instance, vmSmokeRemoteArtifactDir); err != nil {
		return VMSmokeResult{}, err
	}

	guestCleaned := false
	cleanGuest := func() error {
		if opts.KeepArtifacts || guestCleaned {
			return nil
		}

		if err := runVMSmokeGuestClean(ctx, opts, installPlan, guestArtifacts.wrapperBinaries); err != nil {
			return err
		}

		guestCleaned = true
		return nil
	}

	if err := installAndVerifyVMSmokeGuest(ctx, opts, installPlan, guestArtifacts); err != nil {
		if cleanupErr := cleanGuest(); cleanupErr != nil {
			return VMSmokeResult{}, fmt.Errorf("verify Linux systemd artifacts: %w; cleanup failed: %v", err, cleanupErr)
		}

		return VMSmokeResult{}, err
	}

	if err := cleanGuest(); err != nil {
		return VMSmokeResult{}, err
	}

	return VMSmokeResult{
		Status:       "verified",
		Provider:     manager.Provider(),
		Instance:     opts.Instance,
		Template:     opts.Template,
		Profile:      opts.Profile,
		ArtifactDir:  opts.ArtifactDir,
		ManifestPath: renderResult.ManifestPath,
		Files:        renderResult.Files,
		GuestCleaned: guestCleaned,
		LocalCleaned: localCleanup,
	}, nil
}

func RunVMSmokeClean(ctx context.Context, opts VMSmokeOptions) (VMSmokeResult, error) {
	opts, err := normalizeVMSmokeOptions(opts)
	if err != nil {
		return VMSmokeResult{}, err
	}

	manager := opts.Manager
	if err := manager.CheckAvailable(); err != nil {
		return VMSmokeResult{}, err
	}

	exists, err := manager.InstanceExists(ctx, opts.Instance)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if !exists {
		vmSmokeLogf(opts.Stdout, "%s instance %s does not exist", manager.Provider(), opts.Instance)
		return VMSmokeResult{Status: "missing", Provider: manager.Provider(), Instance: opts.Instance, Profile: opts.Profile}, nil
	}

	installPlan, stubBinaries, err := vmSmokeInstallPlan(opts.ManifestPath)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if err := runVMSmokeGuestClean(ctx, opts, installPlan, vmSmokeCleanupBinaries(stubBinaries)); err != nil {
		return VMSmokeResult{}, err
	}

	return VMSmokeResult{Status: "cleaned", Provider: manager.Provider(), Instance: opts.Instance, Profile: opts.Profile, GuestCleaned: true}, nil
}

func RunVMSmokeDown(ctx context.Context, opts VMSmokeOptions) (VMSmokeResult, error) {
	opts, err := normalizeVMSmokeOptions(opts)
	if err != nil {
		return VMSmokeResult{}, err
	}

	manager := opts.Manager
	if err := manager.CheckAvailable(); err != nil {
		return VMSmokeResult{}, err
	}

	if err := manager.Stop(ctx, opts.Instance); err != nil {
		return VMSmokeResult{}, err
	}

	return VMSmokeResult{Status: "down", Provider: manager.Provider(), Instance: opts.Instance, Profile: opts.Profile}, nil
}

func RunVMSmokeDelete(ctx context.Context, opts VMSmokeOptions) (VMSmokeResult, error) {
	opts, err := normalizeVMSmokeOptions(opts)
	if err != nil {
		return VMSmokeResult{}, err
	}

	manager := opts.Manager
	if err := manager.CheckAvailable(); err != nil {
		return VMSmokeResult{}, err
	}

	if err := manager.Delete(ctx, opts.Instance); err != nil {
		return VMSmokeResult{}, err
	}

	return VMSmokeResult{Status: "deleted", Provider: manager.Provider(), Instance: opts.Instance, Profile: opts.Profile}, nil
}

func normalizeVMSmokeOptions(opts VMSmokeOptions) (VMSmokeOptions, error) {
	opts.Profile = strings.ToLower(strings.TrimSpace(opts.Profile))
	if opts.Profile == "" {
		opts.Profile = VMSmokeProfileUnits
	}

	switch opts.Profile {
	case VMSmokeProfileUnits, VMSmokeProfileLocal:
	default:
		return VMSmokeOptions{}, fmt.Errorf("unsupported Linux deploy smoke profile %q", opts.Profile)
	}

	opts.BinaryDir = strings.TrimSpace(opts.BinaryDir)

	if opts.Provider == "" && opts.Manager != nil {
		opts.Provider = opts.Manager.Provider()
	}

	if opts.Manager == nil {
		opts.Provider = platform.ResolveVirtualMachineProvider(opts.Provider)
	}

	if opts.Manager == nil {
		manager, err := platform.NewVirtualMachineManager(platform.VirtualMachineManagerConfig{
			Provider:     opts.Provider,
			ProviderPath: opts.ProviderPath,
			Stdout:       opts.Stdout,
			Stderr:       opts.Stderr,
		})
		if err != nil {
			return VMSmokeOptions{}, err
		}

		opts.Manager = manager
	}

	if opts.Instance == "" {
		opts.Instance = defaultVMSmokeInstance(opts.Provider)
	}

	if opts.Template == "" {
		opts.Template = defaultVMSmokeTemplate(opts.Provider)
	}

	if opts.ManifestPath == "" {
		opts.ManifestPath = DefaultManifestPath
	}

	return opts, nil
}

func prepareVMSmokeArtifactDir(opts *VMSmokeOptions) (bool, error) {
	if opts.ArtifactDir != "" {
		return false, nil
	}

	dir, err := os.MkdirTemp("", "vectis-linux-artifacts-")
	if err != nil {
		return false, fmt.Errorf("create temporary Linux artifact directory: %w", err)
	}

	opts.ArtifactDir = dir
	return !opts.KeepArtifacts, nil
}

func defaultVMSmokeInstance(provider string) string {
	switch provider {
	case platform.VirtualMachineProviderLima:
		return defaultLimaInstance
	default:
		return ""
	}
}

func defaultVMSmokeTemplate(provider string) string {
	switch provider {
	case platform.VirtualMachineProviderLima:
		return defaultLimaTemplate
	default:
		return ""
	}
}

func vmSmokeInstallPlan(manifestPath string) ([]InstallEntry, []string, error) {
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return nil, nil, err
	}

	installPlan, err := manifest.InstallPlan()
	if err != nil {
		return nil, nil, err
	}

	return installPlan, vmSmokeStubBinaries(manifest), nil
}

func vmSmokeStubBinaries(manifest Manifest) []string {
	seen := map[string]struct{}{}
	var binaries []string
	for _, unit := range manifest.Units {
		execStart := unit.ExecStart
		if execStart == "" {
			execStart = "/usr/bin/vectis-" + unit.ID
		}

		fields := strings.Fields(execStart)
		if len(fields) == 0 {
			continue
		}

		binary := path.Base(fields[0])
		if !strings.HasPrefix(binary, "vectis-") {
			continue
		}

		if _, ok := seen[binary]; ok {
			continue
		}

		seen[binary] = struct{}{}
		binaries = append(binaries, binary)
	}

	sort.Strings(binaries)
	return binaries
}

type vmSmokeGuestArtifacts struct {
	wrapperBinaries []string
	realBinaries    []string
}

func prepareVMSmokeGuestArtifacts(opts VMSmokeOptions, manifestBinaries []string) (vmSmokeGuestArtifacts, error) {
	if err := writeVMSmokeMarker(opts.ArtifactDir); err != nil {
		return vmSmokeGuestArtifacts{}, err
	}

	switch opts.Profile {
	case VMSmokeProfileUnits:
		if err := writeVMSmokeStubFiles(opts.ArtifactDir, manifestBinaries); err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		return vmSmokeGuestArtifacts{wrapperBinaries: manifestBinaries}, nil
	case VMSmokeProfileLocal:
		if err := writeVMSmokeLocalEnv(opts.ArtifactDir); err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		realBinaries, err := copyVMSmokeBinaryDir(opts.ArtifactDir, opts.BinaryDir)
		if err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		if err := requireVMSmokeBinaries(realBinaries, vmSmokeLocalRequiredBinaries()); err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		if err := writeVMSmokeStubFiles(opts.ArtifactDir, missingVMSmokeBinaries(manifestBinaries, realBinaries)); err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		if err := writeVMSmokeBinaryWrappers(opts.ArtifactDir, realBinaries); err != nil {
			return vmSmokeGuestArtifacts{}, err
		}

		return vmSmokeGuestArtifacts{
			wrapperBinaries: mergeVMSmokeBinaries(manifestBinaries, realBinaries),
			realBinaries:    realBinaries,
		}, nil
	default:
		return vmSmokeGuestArtifacts{}, fmt.Errorf("unsupported Linux deploy smoke profile %q", opts.Profile)
	}
}

func writeVMSmokeMarker(root string) error {
	markerPath := filepath.Join(root, filepath.FromSlash(vmSmokeMarkerSource))
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o755); err != nil {
		return err
	}

	return os.WriteFile(markerPath, []byte("vectis linux smoke artifacts\n"), 0o640)
}

func writeVMSmokeStubFiles(root string, stubBinaries []string) error {
	binDir := filepath.Join(root, filepath.FromSlash(vmSmokeBinDir))
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}

	for _, binary := range stubBinaries {
		stubPath := filepath.Join(binDir, binary)
		content := fmt.Sprintf("#!/bin/sh\necho %q >&2\nexit 0\n", vmSmokeMarker+": $0 $*")
		if err := os.WriteFile(stubPath, []byte(content), 0o755); err != nil {
			return err
		}
	}

	return nil
}

func writeVMSmokeBinaryWrappers(root string, binaries []string) error {
	binDir := filepath.Join(root, filepath.FromSlash(vmSmokeBinDir))
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return err
	}

	for _, binary := range binaries {
		wrapperPath := filepath.Join(binDir, binary)
		content := fmt.Sprintf("#!/bin/sh\n# %s\nexec %s/%s \"$@\"\n", vmSmokeMarker, vmSmokeGuestRealBinDir, binary)
		if err := os.WriteFile(wrapperPath, []byte(content), 0o755); err != nil {
			return err
		}
	}

	return nil
}

func writeVMSmokeLocalEnv(root string) error {
	envPath := filepath.Join(root, filepath.FromSlash(vmSmokeLocalEnvSource))
	if err := os.MkdirAll(filepath.Dir(envPath), 0o755); err != nil {
		return err
	}

	content := strings.Join([]string{
		"# Vectis local smoke configuration.",
		"XDG_DATA_HOME=/var/lib",
		"XDG_CACHE_HOME=/var/cache",
		"XDG_CONFIG_HOME=/etc",
		"XDG_RUNTIME_DIR=/run",
		"VECTIS_LOCAL_HOST=127.0.0.1",
		"VECTIS_LOCAL_PROFILE=simple",
		"VECTIS_LOCAL_DOCS_ENABLED=false",
		"VECTIS_LOCAL_GRPC_INSECURE=true",
		"VECTIS_LOCAL_HTTP_TLS=off",
		"VECTIS_LOG_FORMAT=json",
		"VECTIS_LOG_DIR=/var/log/vectis/components",
		"",
	}, "\n")

	return os.WriteFile(envPath, []byte(content), 0o644)
}

func copyVMSmokeBinaryDir(root, binaryDir string) ([]string, error) {
	if binaryDir == "" {
		return nil, errVMSmokeLocalBinaryDirRequired()
	}

	entries, err := os.ReadDir(binaryDir)
	if err != nil {
		return nil, fmt.Errorf("read Linux binary directory %s: %w", binaryDir, err)
	}

	realBinDir := filepath.Join(root, filepath.FromSlash(vmSmokeRealBinDir))
	if err := os.MkdirAll(realBinDir, 0o755); err != nil {
		return nil, err
	}

	var binaries []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "vectis-") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("stat Linux binary %s: %w", entry.Name(), err)
		}

		if info.Mode().Perm()&0o111 == 0 {
			continue
		}

		source := filepath.Join(binaryDir, entry.Name())
		destination := filepath.Join(realBinDir, entry.Name())
		if err := copyVMSmokeFile(source, destination, 0o755); err != nil {
			return nil, err
		}

		binaries = append(binaries, entry.Name())
	}

	sort.Strings(binaries)
	if len(binaries) == 0 {
		return nil, fmt.Errorf("Linux binary directory %s does not contain executable vectis-* files", binaryDir)
	}

	return binaries, nil
}

func errVMSmokeLocalBinaryDirRequired() error {
	return fmt.Errorf("Linux deploy smoke profile %q requires --binary-dir with Linux vectis-* binaries", VMSmokeProfileLocal)
}

func copyVMSmokeFile(source, destination string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open %s: %w", source, err)
	}
	defer in.Close()

	out, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("create %s: %w", destination, err)
	}

	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copy %s to %s: %w", source, destination, err)
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", destination, err)
	}

	if err := os.Chmod(destination, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", destination, err)
	}

	return nil
}

func requireVMSmokeBinaries(available, required []string) error {
	availableSet := map[string]bool{}
	for _, binary := range available {
		availableSet[binary] = true
	}

	var missing []string
	for _, binary := range required {
		if !availableSet[binary] {
			missing = append(missing, binary)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("Linux deploy smoke profile %q missing required binaries in --binary-dir: %s", VMSmokeProfileLocal, strings.Join(missing, ", "))
	}

	return nil
}

func missingVMSmokeBinaries(required, available []string) []string {
	availableSet := map[string]bool{}
	for _, binary := range available {
		availableSet[binary] = true
	}

	var missing []string
	for _, binary := range required {
		if !availableSet[binary] {
			missing = append(missing, binary)
		}
	}

	return missing
}

func mergeVMSmokeBinaries(groups ...[]string) []string {
	seen := map[string]bool{}
	var out []string
	for _, group := range groups {
		for _, binary := range group {
			if seen[binary] {
				continue
			}

			seen[binary] = true
			out = append(out, binary)
		}
	}

	sort.Strings(out)
	return out
}

func vmSmokeLocalRequiredBinaries() []string {
	return []string{
		"vectis-api",
		"vectis-artifact",
		"vectis-catalog",
		"vectis-cell-ingress",
		"vectis-cron",
		"vectis-local",
		"vectis-log",
		"vectis-orchestrator",
		"vectis-queue",
		"vectis-reconciler",
		"vectis-registry",
		"vectis-worker",
		"vectis-worker-core",
	}
}

func vmSmokeCleanupBinaries(manifestBinaries []string) []string {
	return mergeVMSmokeBinaries(manifestBinaries, vmSmokeLocalRequiredBinaries(), []string{"vectis-secrets", "vectis-spiffe"})
}

func installAndVerifyVMSmokeGuest(ctx context.Context, opts VMSmokeOptions, installPlan []InstallEntry, guestArtifacts vmSmokeGuestArtifacts) error {
	if err := runVMSmokeGuest(ctx, opts, "sudo", "install", "-d", "-m", "0755", "/usr/lib/sysusers.d", "/usr/lib/tmpfiles.d", "/usr/bin"); err != nil {
		return err
	}

	for _, kind := range []string{"sysusers", "tmpfiles"} {
		entry, ok := findInstallEntryByKind(installPlan, kind)
		if !ok {
			return fmt.Errorf("install manifest missing %s entry", kind)
		}

		if err := installVMSmokeGuestEntry(ctx, opts, entry); err != nil {
			return err
		}
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemd-sysusers", "/usr/lib/sysusers.d/vectis.conf"); err != nil {
		return err
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemd-tmpfiles", "--create", "/usr/lib/tmpfiles.d/vectis.conf"); err != nil {
		return err
	}

	if err := installVMSmokeGuestFile(ctx, opts, vmSmokeMarkerSource, vmSmokeMarkerDestination, "0640", "root", "vectis"); err != nil {
		return err
	}

	if len(guestArtifacts.realBinaries) > 0 {
		if err := runVMSmokeGuest(ctx, opts, "sudo", "install", "-d", "-m", "0755", vmSmokeGuestRealBinDir); err != nil {
			return err
		}

		for _, binary := range guestArtifacts.realBinaries {
			if err := installVMSmokeRealBinary(ctx, opts, binary); err != nil {
				return err
			}
		}
	}

	for _, binary := range guestArtifacts.wrapperBinaries {
		if err := installVMSmokeStub(ctx, opts, binary); err != nil {
			return err
		}
	}

	for _, entry := range installPlan {
		if entry.Kind == "sysusers" || entry.Kind == "tmpfiles" {
			continue
		}

		if err := installVMSmokeGuestEntry(ctx, opts, entry); err != nil {
			return err
		}
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "daemon-reload"); err != nil {
		return err
	}

	systemdUnits := installDestinationsByKind(installPlan, "systemd")
	if err := runVMSmokeGuest(ctx, opts, append([]string{"sudo", "systemd-analyze", "verify"}, systemdUnits...)...); err != nil {
		return err
	}

	switch opts.Profile {
	case VMSmokeProfileUnits:
		if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "start", "vectis-db-migrate.service"); err != nil {
			return err
		}
	case VMSmokeProfileLocal:
		if err := verifyVMSmokeLocalService(ctx, opts); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported Linux deploy smoke profile %q", opts.Profile)
	}

	vmSmokePrintln(opts.Stdout, "Linux systemd artifact verification passed")
	return nil
}

func installVMSmokeRealBinary(ctx context.Context, opts VMSmokeOptions, binary string) error {
	return installVMSmokeGuestFile(ctx, opts, path.Join(vmSmokeRealBinDir, binary), path.Join(vmSmokeGuestRealBinDir, binary), "0755", "root", "root")
}

func verifyVMSmokeLocalService(ctx context.Context, opts VMSmokeOptions) error {
	if err := runVMSmokeGuest(ctx, opts, "curl", "--version"); err != nil {
		return fmt.Errorf("local smoke profile requires curl in the guest for API health checks: %w", err)
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "start", "vectis-local.service"); err != nil {
		return err
	}

	if err := waitVMSmokeGuestCommand(ctx, opts, "vectis-local.service to become active", vmSmokeGuestWaitTimeout, "sudo", "systemctl", "is-active", "--quiet", "vectis-local.service"); err != nil {
		return err
	}

	if err := waitVMSmokeGuestCommand(ctx, opts, "vectis API liveness endpoint", vmSmokeGuestWaitTimeout, "curl", "-fsS", vmSmokeLocalHealthURL); err != nil {
		return err
	}

	vmSmokePrintln(opts.Stdout, "Vectis local service smoke verification passed")
	return nil
}

func waitVMSmokeGuestCommand(ctx context.Context, opts VMSmokeOptions, description string, timeout time.Duration, args ...string) error {
	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		if err := runVMSmokeGuest(ctx, opts, args...); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s: %w", description, lastErr)
		}

		timer := time.NewTimer(vmSmokeGuestWaitInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func installVMSmokeGuestEntry(ctx context.Context, opts VMSmokeOptions, entry InstallEntry) error {
	return installVMSmokeGuestFile(ctx, opts, entry.Source, entry.Destination, entry.Mode, entry.Owner, entry.Group)
}

func installVMSmokeGuestFile(ctx context.Context, opts VMSmokeOptions, source, destination, mode, owner, group string) error {
	return runVMSmokeGuest(ctx, opts,
		"sudo", "install", "-D",
		"-m", mode,
		"-o", owner,
		"-g", group,
		path.Join(vmSmokeRemoteArtifactDir, source),
		destination,
	)
}

func installVMSmokeStub(ctx context.Context, opts VMSmokeOptions, binary string) error {
	destination := "/usr/bin/" + binary
	exists, err := vmSmokeGuestPathExists(ctx, opts, destination)
	if err != nil {
		return err
	}

	if exists {
		if err := runVMSmokeGuest(ctx, opts, "sudo", "grep", "-q", vmSmokeMarker, destination); err != nil {
			return fmt.Errorf("refusing to overwrite non-smoke binary %s", destination)
		}
	}

	return installVMSmokeGuestFile(ctx, opts, path.Join(vmSmokeBinDir, binary), destination, "0755", "root", "root")
}

func runVMSmokeGuestClean(ctx context.Context, opts VMSmokeOptions, installPlan []InstallEntry, stubBinaries []string) error {
	exists, err := vmSmokeGuestPathExists(ctx, opts, vmSmokeMarkerDestination)
	if err != nil {
		return err
	}

	if !exists {
		vmSmokeLogf(opts.Stdout, "no Vectis smoke marker found in guest; leaving system files unchanged")
		return nil
	}

	_ = runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "stop", "vectis.target", "vectis-local.service")

	var destinations []string
	for _, entry := range installPlan {
		destinations = append(destinations, entry.Destination)
	}
	destinations = append(destinations, vmSmokeMarkerDestination)
	sort.Strings(destinations)

	if err := runVMSmokeGuest(ctx, opts, append([]string{"sudo", "rm", "-f"}, destinations...)...); err != nil {
		return err
	}

	for _, binary := range stubBinaries {
		if err := removeVMSmokeStub(ctx, opts, binary); err != nil {
			return err
		}
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "rm", "-rf", vmSmokeGuestRoot); err != nil {
		return err
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "daemon-reload"); err != nil {
		return err
	}

	vmSmokePrintln(opts.Stdout, "Removed Vectis smoke artifacts from guest")
	return nil
}

func removeVMSmokeStub(ctx context.Context, opts VMSmokeOptions, binary string) error {
	destination := "/usr/bin/" + binary
	exists, err := vmSmokeGuestPathExists(ctx, opts, destination)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "grep", "-q", vmSmokeMarker, destination); err != nil {
		return nil
	}

	return runVMSmokeGuest(ctx, opts, "sudo", "rm", "-f", destination)
}

func findInstallEntryByKind(entries []InstallEntry, kind string) (InstallEntry, bool) {
	for _, entry := range entries {
		if entry.Kind == kind {
			return entry, true
		}
	}

	return InstallEntry{}, false
}

func installDestinationsByKind(entries []InstallEntry, kind string) []string {
	var out []string
	for _, entry := range entries {
		if entry.Kind == kind {
			out = append(out, entry.Destination)
		}
	}

	sort.Strings(out)
	return out
}

func vmSmokeGuestPathExists(ctx context.Context, opts VMSmokeOptions, guestPath string) (bool, error) {
	err := runVMSmokeGuest(ctx, opts, "sudo", "test", "-e", guestPath)
	if err != nil {
		return false, nil
	}

	return true, nil
}

func runVMSmokeGuest(ctx context.Context, opts VMSmokeOptions, args ...string) error {
	return opts.Manager.Shell(ctx, opts.Instance, nil, args...)
}

func vmSmokeLogf(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}

	fmt.Fprintf(w, "deploy-vm: "+format+"\n", args...)
}

func vmSmokePrintln(w io.Writer, message string) {
	if w == nil {
		return
	}

	fmt.Fprintln(w, message)
}
