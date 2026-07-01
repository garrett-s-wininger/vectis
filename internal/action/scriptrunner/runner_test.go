package scriptrunner

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestResolveRunners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw        string
		wantName   string
		wantPath   string
		wantExt    string
		wantInline []string
		wantFile   []string
	}{
		{raw: "sh", wantName: "sh", wantPath: "sh", wantExt: ".sh", wantInline: []string{"-c", "echo hi"}, wantFile: []string{"script.sh"}},
		{raw: "bash", wantName: "bash", wantPath: "bash", wantExt: ".sh", wantInline: []string{"-c", "echo hi"}, wantFile: []string{"script.sh"}},
		{raw: "batch", wantName: "cmd", wantPath: "cmd", wantExt: ".cmd", wantInline: []string{"/D", "/S", "/C", "echo hi"}, wantFile: []string{"/D", "/S", "/C", `"script.cmd"`}},
		{raw: "powershell", wantName: "powershell", wantPath: "powershell", wantExt: ".ps1", wantInline: []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", "echo hi"}, wantFile: []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", "script.ps1"}},
		{raw: "pwsh", wantName: "pwsh", wantPath: "pwsh", wantExt: ".ps1", wantInline: []string{"-NoProfile", "-NonInteractive", "-Command", "echo hi"}, wantFile: []string{"-NoProfile", "-NonInteractive", "-File", "script.ps1"}},
		{raw: "python", wantName: "python", wantPath: "python", wantExt: ".py", wantInline: []string{"-c", "echo hi"}, wantFile: []string{"script.py"}},
		{raw: "python3", wantName: "python3", wantPath: "python3", wantExt: ".py", wantInline: []string{"-c", "echo hi"}, wantFile: []string{"script.py"}},
		{raw: "node", wantName: "node", wantPath: "node", wantExt: ".js", wantInline: []string{"-e", "echo hi"}, wantFile: []string{"script.js"}},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			t.Parallel()

			got, err := Resolve(tt.raw, Auto)
			if err != nil {
				t.Fatalf("Resolve(%q): %v", tt.raw, err)
			}

			if got.Name != tt.wantName || got.Path != tt.wantPath || got.Extension != tt.wantExt {
				t.Fatalf("runner = %+v, want name=%q path=%q ext=%q", got, tt.wantName, tt.wantPath, tt.wantExt)
			}

			if strings.Join(got.InlineArgs("echo hi"), "\x00") != strings.Join(tt.wantInline, "\x00") {
				t.Fatalf("InlineArgs = %v, want %v", got.InlineArgs("echo hi"), tt.wantInline)
			}

			if strings.Join(got.FileArgs("script"+tt.wantExt), "\x00") != strings.Join(tt.wantFile, "\x00") {
				t.Fatalf("FileArgs = %v, want %v", got.FileArgs("script"+tt.wantExt), tt.wantFile)
			}
		})
	}
}

func TestResolveAutoUsesPlatformDefault(t *testing.T) {
	t.Parallel()

	got, err := Resolve("", Auto)
	if err != nil {
		t.Fatalf("Resolve default: %v", err)
	}

	want := "sh"
	if runtime.GOOS == "windows" {
		want = "powershell"
	}

	if got.Name != want {
		t.Fatalf("default runner = %q, want %q", got.Name, want)
	}
}

func TestValidateRejectsUnsupportedRunner(t *testing.T) {
	t.Parallel()

	if err := Validate("fish"); err == nil || !strings.Contains(err.Error(), "unsupported runner") {
		t.Fatalf("Validate unsupported = %v, want unsupported runner error", err)
	}
}

func TestWriteWorkspaceScriptFile(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	runner, err := Resolve("sh", Auto)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	path, err := WriteWorkspaceScriptFile(workspace, runner, "echo hi\n")
	if err != nil {
		t.Fatalf("WriteWorkspaceScriptFile: %v", err)
	}

	realWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatalf("resolve workspace: %v", err)
	}

	rel, err := filepath.Rel(realWorkspace, path)
	if err != nil {
		t.Fatalf("relative script path: %v", err)
	}

	if strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		t.Fatalf("script path %q escaped workspace %q", path, realWorkspace)
	}

	if filepath.Dir(rel) != filepath.Join(".vectis", "scripts") {
		t.Fatalf("script dir = %q, want .vectis/scripts", filepath.Dir(rel))
	}

	if filepath.Ext(path) != ".sh" {
		t.Fatalf("script extension = %q, want .sh", filepath.Ext(path))
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}

	if string(data) != "echo hi\n" {
		t.Fatalf("script contents = %q", data)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat script: %v", err)
		}

		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("script mode = %o, want 600", got)
		}
	}
}

func TestWriteWorkspaceScriptFileRejectsSymlinkScriptDirectory(t *testing.T) {
	t.Parallel()

	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, ".vectis")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	runner, err := Resolve("sh", Auto)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	_, err = WriteWorkspaceScriptFile(workspace, runner, "echo hi")
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("WriteWorkspaceScriptFile error = %v, want symlink rejection", err)
	}
}
