package scriptrunner

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const Auto = "auto"

type Runner struct {
	Name      string
	Path      string
	Extension string
}

func Resolve(raw, defaultName string) (Runner, error) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if name == "" {
		name = defaultName
	}

	if name == Auto {
		name = DefaultName()
	}

	switch name {
	case "sh":
		return Runner{Name: name, Path: "sh", Extension: ".sh"}, nil
	case "bash":
		return Runner{Name: name, Path: "bash", Extension: ".sh"}, nil
	case "cmd", "batch":
		return Runner{Name: "cmd", Path: "cmd", Extension: ".cmd"}, nil
	case "powershell":
		return Runner{Name: name, Path: "powershell", Extension: ".ps1"}, nil
	case "pwsh":
		return Runner{Name: name, Path: "pwsh", Extension: ".ps1"}, nil
	case "python", "python3":
		return Runner{Name: name, Path: name, Extension: ".py"}, nil
	case "node":
		return Runner{Name: name, Path: "node", Extension: ".js"}, nil
	default:
		return Runner{}, fmt.Errorf("unsupported runner %q", raw)
	}
}

func DefaultName() string {
	if runtime.GOOS == "windows" {
		return "powershell"
	}

	return "sh"
}

func Validate(raw string) error {
	_, err := Resolve(raw, Auto)
	return err
}

func (r Runner) InlineArgs(command string) []string {
	switch r.Name {
	case "cmd":
		return []string{"/D", "/S", "/C", command}
	case "powershell":
		return []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", command}
	case "pwsh":
		return []string{"-NoProfile", "-NonInteractive", "-Command", command}
	case "python", "python3":
		return []string{"-c", command}
	case "node":
		return []string{"-e", command}
	default:
		return []string{"-c", command}
	}
}

func (r Runner) FileArgs(path string) []string {
	switch r.Name {
	case "cmd":
		return []string{"/D", "/S", "/C", "call", path}
	case "powershell":
		return []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", path}
	case "pwsh":
		return []string{"-NoProfile", "-NonInteractive", "-File", path}
	default:
		return []string{path}
	}
}

func WriteWorkspaceScriptFile(workspace string, runner Runner, body string) (string, error) {
	dir, err := ensureWorkspaceScriptDir(workspace)
	if err != nil {
		return "", err
	}

	file, err := os.CreateTemp(dir, "script-*"+runner.Extension)
	if err != nil {
		return "", fmt.Errorf("create script file: %w", err)
	}

	path := file.Name()
	if _, err := file.WriteString(body); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", fmt.Errorf("write script file: %w", err)
	}

	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("close script file: %w", err)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("chmod script file: %w", err)
	}

	return path, nil
}

func ensureWorkspaceScriptDir(workspace string) (string, error) {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return "", fmt.Errorf("workspace is required")
	}

	root, err := filepath.Abs(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve workspace: %w", err)
	}

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve workspace symlinks: %w", err)
	}

	dir := realRoot
	for _, name := range []string{".vectis", "scripts"} {
		dir = filepath.Join(dir, name)
		if err := ensureRealDirectory(dir); err != nil {
			return "", err
		}
	}

	return dir, nil
}

func ensureRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if err := os.Mkdir(path, 0o700); err != nil {
			return fmt.Errorf("create script directory: %w", err)
		}
		return nil
	}

	if err != nil {
		return fmt.Errorf("stat script directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("script directory must not be a symlink: %s", path)
	}

	if !info.IsDir() {
		return fmt.Errorf("script path parent is not a directory: %s", path)
	}

	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("chmod script directory: %w", err)
	}

	return nil
}
