package actionlauncher

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const modeArg = "__vectis_action_launcher_v1__"

// LaunchSpec is the declarative contract between worker executors and the
// action launcher. Platform-specific setup options should be added here instead
// of threading loose parameters through executor call sites.
type LaunchSpec struct {
	Path    string
	Args    []string
	WorkDir string
	Env     []string
}

type PreparedCommand struct {
	Path    string
	Args    []string
	WorkDir string
	Env     []string
}

// MaybeRun executes launcher mode when the current binary was re-execed by the
// hardened process executor. It must be called from an init path before normal
// command-line parsing starts.
func MaybeRun() {
	if len(os.Args) < 2 || os.Args[1] != modeArg {
		return
	}

	os.Exit(Run(os.Args[2:], os.Stderr))
}

func Command(spec LaunchSpec) (PreparedCommand, error) {
	if strings.TrimSpace(spec.Path) == "" {
		return PreparedCommand{}, fmt.Errorf("action launcher command path is required")
	}

	if strings.TrimSpace(spec.WorkDir) == "" {
		return PreparedCommand{}, fmt.Errorf("action launcher work directory is required")
	}

	target := spec.Path
	prepared := PreparedCommand{
		Path:    target,
		Args:    append([]string(nil), spec.Args...),
		WorkDir: spec.WorkDir,
		Env:     append([]string{}, spec.Env...),
	}

	if !enabled {
		return prepared, nil
	}

	exe, err := os.Executable()
	if err != nil {
		return PreparedCommand{}, fmt.Errorf("resolve action launcher executable: %w", err)
	}

	launcherArgs := make([]string, 0, len(spec.Args)+2)
	launcherArgs = append(launcherArgs, modeArg, target)
	launcherArgs = append(launcherArgs, spec.Args...)
	prepared.Path = exe
	prepared.Args = launcherArgs

	return prepared, nil
}

func Run(args []string, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "vectis action launcher: command path is required")
		return 127
	}

	target := args[0]
	if target == "" {
		fmt.Fprintln(stderr, "vectis action launcher: command path is required")
		return 127
	}

	if !strings.ContainsRune(target, filepath.Separator) {
		resolved, err := exec.LookPath(target)
		if err != nil {
			fmt.Fprintf(stderr, "vectis action launcher: resolve %q: %v\n", target, err)
			return 127
		}

		target = resolved
	}

	if err := prepare(); err != nil {
		fmt.Fprintf(stderr, "vectis action launcher: prepare: %v\n", err)
		return 126
	}

	argv := append([]string{target}, args[1:]...)
	if err := execTarget(target, argv, os.Environ()); err != nil {
		fmt.Fprintf(stderr, "vectis action launcher: exec %q: %v\n", target, err)
		return 127
	}

	return 0
}
