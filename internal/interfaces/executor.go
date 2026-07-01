package interfaces

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"vectis/internal/actionlauncher"
)

const (
	posixDefaultExecutableSearchPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	windowsDefaultSystemRoot         = `C:\Windows`
	windowsDefaultPathExt            = ".COM;.EXE;.BAT;.CMD"
)

type CommandExecutor interface {
	Start(ctx context.Context, command string, workDir string, env []string) (Process, error)
}

type ExecExecutor interface {
	Start(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error)
}

type Process interface {
	Wait() error
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
}

func init() {
	actionlauncher.MaybeRun()
}

// StartProviderProcess adapts provider wrapper commands to the Process
// interface. Host worker action code should use ExecExecutor so it flows
// through the action launcher contract.
func StartProviderProcess(cmd *exec.Cmd) (Process, error) {
	return startProcess(cmd)
}

// startProcess starts cmd with worker-safe process defaults and adapts it to
// the Process interface. A nil Stdin intentionally lets os/exec connect the
// child to the null device, and ExtraFiles are inherited only when the caller
// explicitly configured them.
func startProcess(cmd *exec.Cmd) (Process, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	configureCommandProcessIsolation(cmd)

	registerActiveProcess(cmd)
	if err := cmd.Start(); err != nil {
		unregisterActiveProcess(cmd)
		return nil, fmt.Errorf("failed to start command: %w", err)
	}

	process := &osProcess{
		cmd:    cmd,
		stdout: stdout,
		stderr: stderr,
	}

	return process, nil
}

func NewDirectExecutor() *DirectExecutor {
	return &DirectExecutor{}
}

type DirectExecutor struct{}

func (e *DirectExecutor) Start(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error) {
	return startExecProcess(ctx, path, args, workDir, env)
}

type OSExecutor struct{}

func NewOSExecutor() *OSExecutor {
	return &OSExecutor{}
}

func (e *OSExecutor) Start(ctx context.Context, command string, workDir string, env []string) (Process, error) {
	return startExecProcess(ctx, "sh", []string{"-c", command}, workDir, env)
}

func startExecProcess(ctx context.Context, path string, args []string, workDir string, env []string) (Process, error) {
	secureWorkDir, err := secureLaunchWorkDir(workDir)
	if err != nil {
		return nil, err
	}

	resolvedPath, err := resolveExecutablePath(path, env)
	if err != nil {
		return nil, err
	}

	launcherCommand, err := actionlauncher.Command(actionlauncher.LaunchSpec{
		Path:    resolvedPath,
		Args:    args,
		WorkDir: secureWorkDir,
		Env:     env,
	})

	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, launcherCommand.Path, launcherCommand.Args...) // #nosec G204 -- path and args were resolved through actionlauncher.Command and sanitized launch policy.
	cmd.Dir = launcherCommand.WorkDir
	cmd.Env = launcherCommand.Env
	return startProcess(cmd)
}

func resolveExecutablePath(path string, env []string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("command path is required")
	}

	if strings.ContainsRune(path, os.PathSeparator) {
		return path, nil
	}

	resolved, err := lookPath(path, executableSearchPath(env), env)
	if err != nil {
		return "", fmt.Errorf("resolve command path %q: %w", path, err)
	}

	return resolved, nil
}

func executableSearchPath(env []string) string {
	path, ok := lookupEnvValue(env, "PATH")
	if !ok || path == "" {
		return defaultExecutableSearchPath(env)
	}

	return path
}

func defaultExecutableSearchPath(env []string) string {
	if runtime.GOOS != "windows" {
		return posixDefaultExecutableSearchPath
	}

	systemRoot := windowsSystemRoot(env)
	return strings.Join([]string{
		filepath.Join(systemRoot, "System32"),
		systemRoot,
		filepath.Join(systemRoot, "System32", "WindowsPowerShell", "v1.0"),
	}, string(os.PathListSeparator))
}

func windowsSystemRoot(env []string) string {
	for _, key := range []string{"SystemRoot", "WINDIR"} {
		if value, ok := lookupEnvValue(env, key); ok && filepath.IsAbs(value) {
			return value
		}
	}

	return windowsDefaultSystemRoot
}

func lookupEnvValue(env []string, key string) (string, bool) {
	for _, entry := range env {
		envKey, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}

		if envKeyEqual(envKey, key) {
			return value, true
		}
	}

	return "", false
}

func envKeyEqual(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}

	return a == b
}

func lookPath(file, searchPath string, env []string) (string, error) {
	for _, dir := range filepath.SplitList(searchPath) {
		if dir == "" || !filepath.IsAbs(dir) {
			continue
		}

		for _, name := range executableCandidateNames(file, env) {
			candidate := filepath.Join(dir, name)
			if isExecutableFile(candidate) {
				return candidate, nil
			}
		}
	}

	return "", exec.ErrNotFound
}

func executableCandidateNames(file string, env []string) []string {
	if runtime.GOOS != "windows" {
		return []string{file}
	}

	return windowsExecutableCandidateNames(file, env)
}

func windowsExecutableCandidateNames(file string, env []string) []string {
	if filepath.Ext(file) != "" {
		return []string{file}
	}

	out := []string{file}
	for _, ext := range windowsExecutableExtensions(env) {
		out = append(out, file+ext)
	}

	return out
}

func windowsExecutableExtensions(env []string) []string {
	pathext, ok := lookupEnvValue(env, "PATHEXT")
	if !ok || strings.TrimSpace(pathext) == "" {
		pathext = windowsDefaultPathExt
	}

	return windowsExecutableExtensionsFromPATHEXT(pathext)
}

func windowsExecutableExtensionsFromPATHEXT(pathext string) []string {
	seen := map[string]struct{}{}
	var exts []string
	for _, raw := range strings.Split(pathext, ";") {
		ext := strings.TrimSpace(raw)
		if ext == "" {
			continue
		}

		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}

		key := strings.ToUpper(ext)
		if _, exists := seen[key]; exists {
			continue
		}

		seen[key] = struct{}{}
		exts = append(exts, ext)
	}

	return exts
}

func isExecutableFile(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}

	if info.IsDir() {
		return false
	}

	if runtime.GOOS == "windows" {
		return true
	}

	return info.Mode().Perm()&0o111 != 0
}

func secureLaunchWorkDir(workDir string) (string, error) {
	if strings.TrimSpace(workDir) == "" {
		return "", fmt.Errorf("work directory is required")
	}

	info, err := os.Lstat(workDir)
	if err != nil {
		return "", fmt.Errorf("stat work directory: %w", err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("work directory must not be a symlink")
	}

	if !info.IsDir() {
		return "", fmt.Errorf("work directory is not a directory")
	}

	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		return "", fmt.Errorf("resolve work directory: %w", err)
	}

	realWorkDir, err := filepath.EvalSymlinks(absWorkDir)
	if err != nil {
		return "", fmt.Errorf("resolve real work directory: %w", err)
	}

	info, err = os.Stat(realWorkDir)
	if err != nil {
		return "", fmt.Errorf("stat real work directory: %w", err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("real work directory is not a directory")
	}

	return realWorkDir, nil
}

type osProcess struct {
	cmd    *exec.Cmd
	stdout io.ReadCloser
	stderr io.ReadCloser
}

func (p *osProcess) Wait() error {
	defer unregisterActiveProcess(p.cmd)
	return p.cmd.Wait()
}

func (p *osProcess) Stdout() io.ReadCloser {
	return p.stdout
}

func (p *osProcess) Stderr() io.ReadCloser {
	return p.stderr
}
