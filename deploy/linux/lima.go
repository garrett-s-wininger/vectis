package linux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

const (
	DefaultLimaInstance = "vectis-deploy-smoke"
	DefaultLimaTemplate = "ubuntu-lts"

	limaRemoteArtifactDir = "/tmp/vectis-linux-artifacts"
)

type LimaOptions struct {
	Instance      string
	Template      string
	ArtifactDir   string
	ManifestPath  string
	KeepArtifacts bool
	Stdout        io.Writer
	Stderr        io.Writer
}

type LimaResult struct {
	Status       string `json:"status"`
	Instance     string `json:"instance"`
	Template     string `json:"template,omitempty"`
	ArtifactDir  string `json:"artifact_dir,omitempty"`
	ManifestPath string `json:"manifest_path,omitempty"`
	Files        int    `json:"files,omitempty"`
	GuestCleaned bool   `json:"guest_cleaned,omitempty"`
	LocalCleaned bool   `json:"local_cleaned,omitempty"`
}

func RunLimaVerify(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts = normalizeLimaOptions(opts)
	if err := requireLima(); err != nil {
		return LimaResult{}, err
	}

	exists, err := limaInstanceExists(ctx, opts.Instance)
	if err != nil {
		return LimaResult{}, err
	}

	if !exists {
		limaLogf(opts.Stdout, "creating Lima instance %s from template:%s", opts.Instance, opts.Template)
		if err := runLimaCommand(ctx, opts, nil, "create", "--name="+opts.Instance, "template:"+opts.Template); err != nil {
			return LimaResult{}, err
		}
	}

	limaLogf(opts.Stdout, "starting Lima instance %s", opts.Instance)
	if err := runLimaCommand(ctx, opts, nil, "start", opts.Instance); err != nil {
		return LimaResult{}, err
	}

	localCleanup, err := prepareLimaArtifactDir(&opts)
	if err != nil {
		return LimaResult{}, err
	}

	if localCleanup {
		defer func() { _ = os.RemoveAll(opts.ArtifactDir) }()
	}

	limaLogf(opts.Stdout, "rendering Linux artifacts to %s", opts.ArtifactDir)
	renderResult, err := RenderToDir(RenderOptions{ManifestPath: opts.ManifestPath, OutDir: opts.ArtifactDir})
	if err != nil {
		return LimaResult{}, err
	}

	limaLogf(opts.Stdout, "copying rendered artifacts into %s:%s", opts.Instance, limaRemoteArtifactDir)
	if err := runLimaCommand(ctx, opts, nil, "shell", opts.Instance, "sudo", "rm", "-rf", limaRemoteArtifactDir); err != nil {
		return LimaResult{}, err
	}

	if err := runLimaCommand(ctx, opts, nil, "copy", "-r", opts.ArtifactDir, opts.Instance+":"+limaRemoteArtifactDir); err != nil {
		return LimaResult{}, err
	}

	if err := runLimaCommand(ctx, opts, strings.NewReader(limaGuestVerifyScript), "shell", opts.Instance, "sudo", "sh", "-s", "--", limaRemoteArtifactDir); err != nil {
		return LimaResult{}, err
	}

	guestCleaned := false
	if !opts.KeepArtifacts {
		if err := runLimaGuestClean(ctx, opts); err != nil {
			return LimaResult{}, err
		}
		guestCleaned = true
	}

	return LimaResult{
		Status:       "verified",
		Instance:     opts.Instance,
		Template:     opts.Template,
		ArtifactDir:  opts.ArtifactDir,
		ManifestPath: renderResult.ManifestPath,
		Files:        renderResult.Files,
		GuestCleaned: guestCleaned,
		LocalCleaned: localCleanup,
	}, nil
}

func RunLimaClean(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts = normalizeLimaOptions(opts)
	if err := requireLima(); err != nil {
		return LimaResult{}, err
	}

	exists, err := limaInstanceExists(ctx, opts.Instance)
	if err != nil {
		return LimaResult{}, err
	}

	if !exists {
		limaLogf(opts.Stdout, "Lima instance %s does not exist", opts.Instance)
		return LimaResult{Status: "missing", Instance: opts.Instance}, nil
	}

	if err := runLimaGuestClean(ctx, opts); err != nil {
		return LimaResult{}, err
	}

	return LimaResult{Status: "cleaned", Instance: opts.Instance, GuestCleaned: true}, nil
}

func RunLimaDown(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts = normalizeLimaOptions(opts)
	if err := requireLima(); err != nil {
		return LimaResult{}, err
	}

	if err := runLimaCommand(ctx, opts, nil, "stop", opts.Instance); err != nil {
		return LimaResult{}, err
	}

	return LimaResult{Status: "down", Instance: opts.Instance}, nil
}

func RunLimaDelete(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts = normalizeLimaOptions(opts)
	if err := requireLima(); err != nil {
		return LimaResult{}, err
	}

	if err := runLimaCommand(ctx, opts, nil, "delete", "--force", opts.Instance); err != nil {
		return LimaResult{}, err
	}

	return LimaResult{Status: "deleted", Instance: opts.Instance}, nil
}

func normalizeLimaOptions(opts LimaOptions) LimaOptions {
	if opts.Instance == "" {
		opts.Instance = DefaultLimaInstance
	}

	if opts.Template == "" {
		opts.Template = DefaultLimaTemplate
	}

	if opts.ManifestPath == "" {
		opts.ManifestPath = DefaultManifestPath
	}

	return opts
}

func prepareLimaArtifactDir(opts *LimaOptions) (bool, error) {
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

func runLimaGuestClean(ctx context.Context, opts LimaOptions) error {
	return runLimaCommand(ctx, opts, strings.NewReader(limaGuestCleanScript), "shell", opts.Instance, "sudo", "sh", "-s")
}

func requireLima() error {
	if _, err := exec.LookPath("limactl"); err != nil {
		return fmt.Errorf("limactl is not installed; install Lima, then rerun this command")
	}

	return nil
}

func limaInstanceExists(ctx context.Context, instance string) (bool, error) {
	cmd := exec.CommandContext(ctx, "limactl", "list", instance) //#nosec G204
	if err := cmd.Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return false, nil
		}

		return false, fmt.Errorf("limactl list %s: %w", instance, err)
	}

	return true, nil
}

func runLimaCommand(ctx context.Context, opts LimaOptions, stdin io.Reader, args ...string) error {
	cmd := exec.CommandContext(ctx, "limactl", args...) //#nosec G204
	cmd.Stdin = stdin
	cmd.Stdout = opts.Stdout
	cmd.Stderr = opts.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("limactl %s failed: %w", strings.Join(args, " "), err)
	}

	return nil
}

func limaLogf(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}

	fmt.Fprintf(w, "deploy-lima: "+format+"\n", args...)
}

const limaGuestVerifyScript = `set -eu

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

const limaGuestCleanScript = `set -eu

marker="vectis linux smoke stub"
marker_file="/etc/vectis/.vectis-linux-smoke"

if [ ! -f "$marker_file" ]; then
  printf '%s\n' "No Vectis smoke marker found in Lima guest; leaving system files unchanged"
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
printf '%s\n' "Removed Vectis smoke artifacts from Lima guest"
`
