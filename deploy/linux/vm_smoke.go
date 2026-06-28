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

	"vectis/internal/platform"
)

const (
	vmSmokeRemoteArtifactDir = "/tmp/vectis-linux-artifacts"
	vmSmokeMarker            = "vectis linux smoke stub"
	vmSmokeMarkerSource      = "smoke/.vectis-linux-smoke"
	vmSmokeMarkerDestination = "/etc/vectis/.vectis-linux-smoke"
	vmSmokeBinDir            = "smoke/bin"
	vmSmokeGuestProfile      = "systemd"
	vmSmokeGuestProfilePath  = "/etc/vectis-vm-prep/deploy-smoke-profile"
	vmSmokeGuestPrepVersion  = "2"
	vmSmokeGuestPrepPath     = "/etc/vectis-vm-prep/deploy-smoke-prep-version"
	defaultLimaInstance      = "vectis-deploy-smoke"
)

type VMSmokeOptions struct {
	Manager       platform.VirtualMachineManager
	Provider      string
	ProviderPath  string
	Instance      string
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

	manager := opts.Manager
	if err := manager.CheckAvailable(); err != nil {
		return VMSmokeResult{}, err
	}

	exists, err := manager.InstanceExists(ctx, opts.Instance)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if !exists {
		return VMSmokeResult{}, fmt.Errorf("deploy smoke VM %q does not exist; run mage vmDeploySmokePrepare", opts.Instance)
	}

	vmSmokeLogf(opts.Stdout, "starting %s instance %s", manager.Provider(), opts.Instance)
	if err := manager.Start(ctx, opts.Instance); err != nil {
		return VMSmokeResult{}, err
	}

	if err := verifyPreparedVMSmokeGuest(ctx, opts); err != nil {
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

	if err := writeVMSmokeLocalFiles(opts.ArtifactDir, stubBinaries); err != nil {
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

		if err := runVMSmokeGuestClean(ctx, opts, installPlan, stubBinaries); err != nil {
			return err
		}

		guestCleaned = true
		return nil
	}

	if err := installAndVerifyVMSmokeGuest(ctx, opts, installPlan, stubBinaries); err != nil {
		if cleanupErr := cleanGuest(); cleanupErr != nil {
			return VMSmokeResult{}, fmt.Errorf("verify Linux systemd artifacts: %w; cleanup failed: %w", err, cleanupErr)
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
		return VMSmokeResult{Status: "missing", Provider: manager.Provider(), Instance: opts.Instance}, nil
	}

	installPlan, stubBinaries, err := vmSmokeInstallPlan(opts.ManifestPath)
	if err != nil {
		return VMSmokeResult{}, err
	}

	if err := runVMSmokeGuestClean(ctx, opts, installPlan, stubBinaries); err != nil {
		return VMSmokeResult{}, err
	}

	return VMSmokeResult{Status: "cleaned", Provider: manager.Provider(), Instance: opts.Instance, GuestCleaned: true}, nil
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

	return VMSmokeResult{Status: "down", Provider: manager.Provider(), Instance: opts.Instance}, nil
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

	return VMSmokeResult{Status: "deleted", Provider: manager.Provider(), Instance: opts.Instance}, nil
}

func normalizeVMSmokeOptions(opts VMSmokeOptions) (VMSmokeOptions, error) {
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

func verifyPreparedVMSmokeGuest(ctx context.Context, opts VMSmokeOptions) error {
	checks := [][]string{
		{"test", "-r", vmSmokeGuestProfilePath},
		{"grep", "-qx", vmSmokeGuestProfile, vmSmokeGuestProfilePath},
		{"test", "-r", vmSmokeGuestPrepPath},
		{"grep", "-qx", expectedVMSmokeGuestPrepVersion(), vmSmokeGuestPrepPath},
		{"systemctl", "--version"},
		{"systemd-analyze", "--version"},
		{"systemd-sysusers", "--version"},
		{"systemd-tmpfiles", "--version"},
	}

	for _, check := range checks {
		if err := opts.Manager.Shell(ctx, opts.Instance, nil, check...); err != nil {
			return fmt.Errorf("deploy smoke VM %q is not prepared; run mage vmDeploySmokePrepare: %w", opts.Instance, err)
		}
	}

	return nil
}

func expectedVMSmokeGuestPrepVersion() string {
	if version := strings.TrimSpace(os.Getenv("PACKER_VM_PREP_VERSION")); version != "" {
		return version
	}

	return vmSmokeGuestPrepVersion
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

func writeVMSmokeLocalFiles(root string, stubBinaries []string) error {
	markerPath := filepath.Join(root, filepath.FromSlash(vmSmokeMarkerSource))
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o755); err != nil {
		return err
	}

	if err := os.WriteFile(markerPath, []byte("vectis linux smoke artifacts\n"), 0o640); err != nil {
		return err
	}

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

func installAndVerifyVMSmokeGuest(ctx context.Context, opts VMSmokeOptions, installPlan []InstallEntry, stubBinaries []string) error {
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

	for _, binary := range stubBinaries {
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

	targetMemberUnits := vmSmokeTargetMemberUnits(installPlan)
	if len(targetMemberUnits) > 0 {
		if err := runVMSmokeGuest(ctx, opts, append([]string{"sudo", "systemctl", "enable"}, targetMemberUnits...)...); err != nil {
			return err
		}
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "start", "vectis.target"); err != nil {
		return err
	}

	if err := runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "is-active", "--quiet", "vectis.target"); err != nil {
		return err
	}

	vmSmokePrintln(opts.Stdout, "Linux systemd artifact verification passed")
	return nil
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

	_ = runVMSmokeGuest(ctx, opts, "sudo", "systemctl", "stop", "vectis.target")
	targetMemberUnits := vmSmokeTargetMemberUnits(installPlan)
	if len(targetMemberUnits) > 0 {
		_ = runVMSmokeGuest(ctx, opts, append([]string{"sudo", "systemctl", "disable"}, targetMemberUnits...)...)
	}

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

	if !vmSmokeGuestFileHasMarker(ctx, opts, destination) {
		return nil
	}

	return runVMSmokeGuest(ctx, opts, "sudo", "rm", "-f", destination)
}

func vmSmokeGuestFileHasMarker(ctx context.Context, opts VMSmokeOptions, guestPath string) bool {
	return runVMSmokeGuest(ctx, opts, "sudo", "grep", "-q", vmSmokeMarker, guestPath) == nil
}

func findInstallEntryByKind(entries []InstallEntry, kind string) (InstallEntry, bool) {
	for _, entry := range entries {
		if entry.Kind == kind {
			return entry, true
		}
	}

	return InstallEntry{}, false
}

func vmSmokeTargetMemberUnits(entries []InstallEntry) []string {
	var units []string
	for _, entry := range entries {
		if entry.Kind != "systemd" {
			continue
		}

		name := path.Base(entry.Destination)
		if name == "vectis-db-migrate.service" || !strings.HasSuffix(name, ".service") {
			continue
		}

		units = append(units, name)
	}

	sort.Strings(units)
	return units
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
	return runVMSmokeGuest(ctx, opts, "sudo", "test", "-e", guestPath) == nil, nil
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
