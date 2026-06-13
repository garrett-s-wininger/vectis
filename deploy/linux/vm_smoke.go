package linux

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"vectis/internal/platform"
)

const vmSmokeRemoteArtifactDir = "/tmp/vectis-linux-artifacts"

type VMSmokeOptions struct {
	Manager       platform.VirtualMachineManager
	Provider      string
	ProviderPath  string
	Instance      string
	Template      string
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

	vmSmokeLogf(opts.Stdout, "copying rendered artifacts into %s:%s", opts.Instance, vmSmokeRemoteArtifactDir)
	if err := manager.Shell(ctx, opts.Instance, nil, "sudo", "rm", "-rf", vmSmokeRemoteArtifactDir); err != nil {
		return VMSmokeResult{}, err
	}

	if err := manager.CopyDir(ctx, opts.ArtifactDir, opts.Instance, vmSmokeRemoteArtifactDir); err != nil {
		return VMSmokeResult{}, err
	}

	if err := manager.Shell(ctx, opts.Instance, strings.NewReader(vmSmokeGuestVerifyScript), "sudo", "sh", "-s", "--", vmSmokeRemoteArtifactDir); err != nil {
		return VMSmokeResult{}, err
	}

	guestCleaned := false
	if !opts.KeepArtifacts {
		if err := runVMSmokeGuestClean(ctx, opts); err != nil {
			return VMSmokeResult{}, err
		}
		guestCleaned = true
	}

	return VMSmokeResult{
		Status:       "verified",
		Provider:     manager.Provider(),
		Instance:     opts.Instance,
		Template:     opts.Template,
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

	if err := runVMSmokeGuestClean(ctx, opts); err != nil {
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

	if opts.Provider == "" {
		opts.Provider = platform.VirtualMachineProviderLima
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
		return DefaultLimaInstance
	default:
		return ""
	}
}

func defaultVMSmokeTemplate(provider string) string {
	switch provider {
	case platform.VirtualMachineProviderLima:
		return DefaultLimaTemplate
	default:
		return ""
	}
}

func runVMSmokeGuestClean(ctx context.Context, opts VMSmokeOptions) error {
	return opts.Manager.Shell(ctx, opts.Instance, strings.NewReader(vmSmokeGuestCleanScript), "sudo", "sh", "-s")
}

func vmSmokeLogf(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}

	fmt.Fprintf(w, "deploy-vm: "+format+"\n", args...)
}

const vmSmokeGuestVerifyScript = `set -eu

artifact_dir="$1"
marker="vectis linux smoke stub"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf '%s\n' "missing required command in guest: $1" >&2
    exit 1
  fi
}

need systemctl
need systemd-analyze
need systemd-sysusers
need systemd-tmpfiles

install -d -m 0755 /etc/systemd/system
install -d -m 0750 /etc/vectis
install -d -m 0755 /usr/lib/sysusers.d
install -d -m 0755 /usr/lib/tmpfiles.d
install -d -m 0755 /usr/bin

printf '%s\n' "vectis linux smoke artifacts" > /etc/vectis/.vectis-linux-smoke
chmod 0640 /etc/vectis/.vectis-linux-smoke

install -m 0644 "$artifact_dir"/systemd/* /etc/systemd/system/
install -m 0644 "$artifact_dir"/sysusers.d/vectis.conf /usr/lib/sysusers.d/vectis.conf
install -m 0644 "$artifact_dir"/tmpfiles.d/vectis.conf /usr/lib/tmpfiles.d/vectis.conf

for example in "$artifact_dir"/env/*.example; do
  name="$(basename "$example" .example)"
  install -m 0640 "$example" "/etc/vectis/$name"
done

for bin in api catalog cell-ingress cli cron docs local log log-forwarder queue reconciler registry worker; do
  path="/usr/bin/vectis-$bin"
  if [ -e "$path" ] && ! grep -q "$marker" "$path" 2>/dev/null; then
    printf '%s\n' "leaving existing $path in place"
    continue
  fi

  {
    printf '%s\n' '#!/bin/sh'
    printf '%s\n' "echo \"$marker: \$0 \$*\" >&2"
    printf '%s\n' 'exit 0'
  } > "$path"
  chmod 0755 "$path"
done

systemd-sysusers /usr/lib/sysusers.d/vectis.conf
systemd-tmpfiles --create /usr/lib/tmpfiles.d/vectis.conf
systemctl daemon-reload
systemd-analyze verify /etc/systemd/system/vectis*.service /etc/systemd/system/vectis.target
systemctl cat vectis.target vectis-local.service >/dev/null
systemctl start vectis-db-migrate.service
systemctl reset-failed vectis-db-migrate.service >/dev/null 2>&1 || true

printf '%s\n' "Linux systemd artifact verification passed"
`

const vmSmokeGuestCleanScript = `set -eu

marker="vectis linux smoke stub"
marker_file="/etc/vectis/.vectis-linux-smoke"

if [ ! -f "$marker_file" ]; then
  printf '%s\n' "No Vectis smoke marker found in guest; leaving system files unchanged"
  exit 0
fi

systemctl stop vectis.target vectis-local.service >/dev/null 2>&1 || true

rm -f /etc/systemd/system/vectis*.service
rm -f /etc/systemd/system/vectis.target
rm -f /usr/lib/sysusers.d/vectis.conf
rm -f /usr/lib/tmpfiles.d/vectis.conf
rm -f /etc/vectis/vectis*.env

for path in /usr/bin/vectis-*; do
  [ -e "$path" ] || continue
  if grep -q "$marker" "$path" 2>/dev/null; then
    rm -f "$path"
  fi
done

systemctl daemon-reload
systemctl reset-failed >/dev/null 2>&1 || true
rm -f "$marker_file"
printf '%s\n' "Removed Vectis smoke artifacts from guest"
`
