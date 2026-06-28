//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

func buildLDFlags(strip bool) string {
	flags := []string{
		"-X vectis/internal/version.Version=" + gitOutput("dev", "describe", "--tags", "--always", "--dirty"),
		"-X vectis/internal/version.Commit=" + gitOutput("unknown", "rev-parse", "--short=12", "HEAD"),
		"-X vectis/internal/version.BuildDate=" + time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}

	if strip {
		flags = append(flags, "-s", "-w")
	}

	return strings.Join(flags, " ")
}

func gitOutput(fallback string, args ...string) string {
	cmd := exec.Command("git", args...)
	out, err := cmd.Output()
	if err != nil {
		return fallback
	}

	value := strings.TrimSpace(string(out))
	if value == "" {
		return fallback
	}

	return value
}

func gitStatusPorcelain() (string, error) {
	cmd := exec.Command("git", "status", "--porcelain")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

func protocPlugin(envName, tool string) string {
	if value := os.Getenv(envName); value != "" {
		return value
	}

	gopath := goEnv("GOPATH")
	if gopath == "" {
		return exeName(tool)
	}

	return filepath.Join(gopath, "bin", exeName(tool))
}

func goEnv(name string) string {
	cmd := exec.Command(goCommand(), "env", name)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

func goArch() string {
	if arch := goEnv("GOARCH"); arch != "" {
		return arch
	}

	return runtime.GOARCH
}

func goModGoVersion() string {
	data, err := os.ReadFile("go.mod")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "go" {
				return fields[1]
			}
		}
	}

	if version := strings.TrimPrefix(goEnv("GOVERSION"), "go"); version != "" {
		return version
	}

	return "1.25.10"
}

func goCommand() string {
	return envDefault("GO", "go")
}

func firstOnPath(names ...string) (string, error) {
	for _, name := range names {
		path, err := exec.LookPath(name)
		if err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("none of these commands are on PATH: %s", strings.Join(names, ", "))
}

func exeName(name string) string {
	if runtime.GOOS == "windows" && !strings.HasSuffix(strings.ToLower(name), ".exe") {
		return name + ".exe"
	}

	return name
}

func hostExecutableExt() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}

	return ""
}

func targetExecutableExt() string {
	if targetGoOS() == "windows" {
		return ".exe"
	}

	return ""
}

func targetGoOS() string {
	if value := os.Getenv("TARGET_GOOS"); value != "" {
		return value
	}

	if value := os.Getenv("GOOS"); value != "" {
		return value
	}

	if value := goEnv("GOOS"); value != "" {
		return value
	}

	return runtime.GOOS
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func fieldsEnv(name, fallback string) []string {
	fields := strings.Fields(envDefault(name, fallback))
	if len(fields) > 0 {
		return fields
	}

	return strings.Fields(fallback)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func run(dir string, extraEnv map[string]string, name string, args ...string) error {
	fmt.Println("$", commandLine(name, args))
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}

	if len(extraEnv) > 0 {
		cmd.Env = append(os.Environ(), envPairs(extraEnv)...)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func envPairs(values map[string]string) []string {
	pairs := make([]string, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, key+"="+value)
	}

	sort.Strings(pairs)
	return pairs
}

func commandLine(name string, args []string) string {
	parts := append([]string{name}, args...)
	return strings.Join(parts, " ")
}
