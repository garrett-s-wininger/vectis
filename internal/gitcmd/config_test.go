package gitcmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteConfigFileSettingsUpdatesAndAppends(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	initial := `[core]
	repositoryformatversion = 0
[gc]
	auto = 1
[remote "origin"]
	tagOpt = --tags
`
	if err := os.WriteFile(configPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	settings := [][2]string{
		{"gc.auto", "0"},
		{"remote.origin.tagOpt", "--no-tags"},
		{"maintenance.auto", "false"},
	}
	if err := WriteConfigFileSettings(configPath, settings); err != nil {
		t.Fatalf("WriteConfigFileSettings: %v", err)
	}

	got, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	text := string(got)
	for _, want := range []string{
		"\tauto = 0",
		"\ttagOpt = --no-tags",
		"[maintenance]\n\tauto = false",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("config missing %q:\n%s", want, text)
		}
	}
}

func TestWorkTreeConfigPathFollowsGitDirPointer(t *testing.T) {
	dir := t.TempDir()
	gitDir := filepath.Join(dir, "actual.git")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatalf("create git dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".git"), []byte("gitdir: actual.git\n"), 0o644); err != nil {
		t.Fatalf("write .git pointer: %v", err)
	}

	got, err := WorkTreeConfigPath(dir)
	if err != nil {
		t.Fatalf("WorkTreeConfigPath: %v", err)
	}

	if want := filepath.Join(gitDir, "config"); got != want {
		t.Fatalf("config path got %q, want %q", got, want)
	}
}

func TestReadConfigFileRemoteURLs(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	initial := `[core]
	repositoryformatversion = 0
[remote "origin"]
	url = https://example.invalid/origin.git
	fetch = +refs/heads/*:refs/remotes/origin/*
[remote "vectis-fallback-1"]
	url = https://example.invalid/fallback.git
	url = https://example.invalid/ignored.git
	fetch = +refs/heads/*:refs/remotes/vectis-fallback-1/*
`
	if err := os.WriteFile(configPath, []byte(initial), 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}

	got, err := ReadConfigFileRemoteURLs(configPath)
	if err != nil {
		t.Fatalf("ReadConfigFileRemoteURLs: %v", err)
	}

	if got["origin"] != "https://example.invalid/origin.git" {
		t.Fatalf("origin URL got %q", got["origin"])
	}
	if got["vectis-fallback-1"] != "https://example.invalid/fallback.git" {
		t.Fatalf("fallback URL got %q", got["vectis-fallback-1"])
	}
}
