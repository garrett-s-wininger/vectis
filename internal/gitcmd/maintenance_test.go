package gitcmd

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoAutoMaintenanceCloneArgsPersistsSettings(t *testing.T) {
	args := NoAutoMaintenanceCloneArgs("https://example.invalid/repo.git", ".")
	settings := NoAutoMaintenanceSettings()
	if len(args) < len(settings)*4+3 {
		t.Fatalf("clone args too short: %+v", args)
	}

	for i, setting := range settings {
		if got, want := args[i*2], "-c"; got != want {
			t.Fatalf("top-level args[%d]=%q, want %q; args=%+v", i*2, got, want, args)
		}

		if got, want := args[i*2+1], setting[0]+"="+setting[1]; got != want {
			t.Fatalf("top-level args[%d]=%q, want %q; args=%+v", i*2+1, got, want, args)
		}
	}

	cloneStart := len(settings) * 2
	if got := args[cloneStart]; got != "clone" {
		t.Fatalf("clone command arg = %q, want clone; args=%+v", got, args)
	}

	for i, setting := range settings {
		index := cloneStart + 1 + i*2
		if got, want := args[index], "-c"; got != want {
			t.Fatalf("clone config args[%d]=%q, want %q; args=%+v", index, got, want, args)
		}

		if got, want := args[index+1], setting[0]+"="+setting[1]; got != want {
			t.Fatalf("clone config args[%d]=%q, want %q; args=%+v", index+1, got, want, args)
		}
	}

	tail := args[cloneStart+1+len(settings)*2:]
	if got := strings.Join(tail, " "); got != "https://example.invalid/repo.git ." {
		t.Fatalf("clone args tail=%q, want remote and target; args=%+v", got, args)
	}
}

func TestNoAutoMaintenanceCloneArgsWritesLocalConfig(t *testing.T) {
	remote := filepath.Join(t.TempDir(), "remote.git")
	runGit(t, "", "init", "--bare", remote)

	checkout := filepath.Join(t.TempDir(), "checkout")
	if err := os.MkdirAll(checkout, 0o755); err != nil {
		t.Fatalf("create checkout dir: %v", err)
	}

	runGit(t, checkout, NoAutoMaintenanceCloneArgs(remote, ".")...)

	for _, setting := range NoAutoMaintenanceSettings() {
		if got := gitOutput(t, checkout, "config", "--get", setting[0]); got != setting[1] {
			t.Fatalf("cloned repo config %s = %q, want %q", setting[0], got, setting[1])
		}
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}

	return strings.TrimSpace(string(out))
}
