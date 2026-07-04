package packer

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPackerShellScriptsParse(t *testing.T) {
	for _, script := range []string{
		"scripts/lima-common.sh",
		"scripts/guest-common.sh",
		"deploy-smoke/scripts/prepare-lima-deploy-smoke.sh",
		"deploy-smoke/scripts/guest-deploy-smoke.sh",
		"package-builder/scripts/prepare-lima-package-builder.sh",
		"package-builder/scripts/guest-package-builder.sh",
		"package-smoke/scripts/prepare-lima-package-smoke.sh",
		"package-smoke/scripts/guest-package-smoke.sh",
	} {
		t.Run(script, func(t *testing.T) {
			runShell(t, "-n", script)
		})
	}
}

func TestPackerLimaWrappersUseSharedLifecycle(t *testing.T) {
	fakeLimactl, logPath := writeFakeLimactl(t)

	tests := []struct {
		name   string
		script string
		env    []string
	}{
		{
			name:   "deploy-smoke",
			script: "deploy-smoke/scripts/prepare-lima-deploy-smoke.sh",
			env: []string{
				"VECTIS_PACKER_LIMA_INSTANCE=vectis-deploy-smoke",
				"VECTIS_PACKER_LIMA_TEMPLATE=ubuntu-lts",
			},
		},
		{
			name:   "package-builder",
			script: "package-builder/scripts/prepare-lima-package-builder.sh",
			env: []string{
				"VECTIS_PACKER_LIMA_INSTANCE=vectis-package-builder",
				"VECTIS_PACKER_LIMA_TEMPLATE=ubuntu-lts",
				"VECTIS_PACKER_GO_VERSION=1.25.10",
			},
		},
		{
			name:   "package-smoke-deb",
			script: "package-smoke/scripts/prepare-lima-package-smoke.sh",
			env: []string{
				"VECTIS_PACKER_LIMA_INSTANCE=vectis-package-smoke",
				"VECTIS_PACKER_LIMA_TEMPLATE=ubuntu-lts",
				"VECTIS_PACKER_PACKAGE_PROFILE=deb",
			},
		},
		{
			name:   "package-smoke-rpm",
			script: "package-smoke/scripts/prepare-lima-package-smoke.sh",
			env: []string{
				"VECTIS_PACKER_LIMA_INSTANCE=vectis-package-rpm-smoke",
				"VECTIS_PACKER_LIMA_TEMPLATE=fedora",
				"VECTIS_PACKER_PACKAGE_PROFILE=rpm",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(logPath, nil, 0o600); err != nil {
				t.Fatalf("clear fake limactl log: %v", err)
			}

			env := append([]string{
				"VECTIS_PACKER_LIMA_BIN=" + fakeLimactl,
				"VECTIS_FAKE_LIMACTL_LOG=" + logPath,
			}, tt.env...)

			runShellWithEnv(t, env, tt.script)
			logBytes, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatalf("read fake limactl log: %v", err)
			}

			log := string(logBytes)
			for _, want := range []string{"list", "--tty=false start", "--tty=false shell", "--tty=false stop"} {
				if !strings.Contains(log, want) {
					t.Fatalf("fake limactl log missing %q:\n%s", want, log)
				}
			}
		})
	}
}

func writeFakeLimactl(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "limactl")
	logPath := filepath.Join(dir, "limactl.log")
	script := `#!/bin/sh
set -eu

printf '%s\n' "$*" >> "$VECTIS_FAKE_LIMACTL_LOG"

case "$1" in
	list)
		exit 0
		;;
	--tty=false)
		shift
		case "$1" in
			create|start|stop)
				exit 0
				;;
			shell)
				cat >/dev/null
				exit 0
				;;
		esac
		;;
esac

echo "unexpected limactl args: $*" >&2
exit 64
`

	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake limactl: %v", err)
	}

	return path, logPath
}

func runShell(t *testing.T, args ...string) {
	t.Helper()
	runShellWithEnv(t, nil, args...)
}

func runShellWithEnv(t *testing.T, extraEnv []string, args ...string) {
	t.Helper()

	cmd := exec.Command("sh", args...)
	cmd.Env = append(os.Environ(), extraEnv...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sh %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}
