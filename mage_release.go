//go:build mage

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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

func runFormalVerification(model string) error {
	return run("", nil,
		envDefault("JAVA", "java"),
		"-jar", envDefault("TLA_TOOLS_JAR", "/opt/tla+/tla2tools.jar"),
		"-workers", "auto",
		filepath.Join("formal", "tla", model+".tla"),
		"-config", filepath.Join("formal", "tla", model+".cfg"),
	)
}
