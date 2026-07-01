package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	gerritsmoke "vectis/deploy/gerrit"
)

func main() {
	var opts gerritsmoke.StreamSmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.URL, "url", envDefault("GERRIT_STREAM_SMOKE_URL", gerritsmoke.DefaultSmokeURL), "Gerrit base URL")
	flag.StringVar(&opts.AccountID, "account-id", envDefault("GERRIT_STREAM_SMOKE_ACCOUNT_ID", gerritsmoke.DefaultSmokeAccountID), "Development auth account id")
	flag.StringVar(&opts.Username, "username", envDefault("GERRIT_STREAM_SMOKE_USERNAME", gerritsmoke.DefaultSmokeUsername), "Gerrit username for HTTP and SSH auth")
	flag.StringVar(&opts.Project, "project", os.Getenv("GERRIT_STREAM_SMOKE_PROJECT"), "Project name; generated when empty")
	flag.StringVar(&opts.ProjectPrefix, "project-prefix", envDefault("GERRIT_STREAM_SMOKE_PROJECT_PREFIX", gerritsmoke.DefaultSmokeProjectPrefix), "Generated project prefix")
	flag.StringVar(&opts.SSHHost, "ssh-host", envDefault("GERRIT_STREAM_SMOKE_SSH_HOST", gerritsmoke.DefaultStreamSmokeSSHHost), "Gerrit SSH host")
	flag.IntVar(&opts.SSHPort, "ssh-port", envIntDefault("GERRIT_STREAM_SMOKE_SSH_PORT", gerritsmoke.DefaultStreamSmokeSSHPort), "Gerrit SSH port")
	flag.DurationVar(&opts.Timeout, "timeout", gerritsmoke.DefaultSmokeTimeout, "Maximum time to wait for the Gerrit stream smoke")
	flag.StringVar(&opts.GitBin, "git", envDefault("GERRIT_STREAM_SMOKE_GIT", gerritsmoke.DefaultSmokeGitBin), "git executable")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := gerritsmoke.RunStreamSmoke(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: write JSON result: %v\n", err)
			os.Exit(1)
		}

		return
	}

	fmt.Fprintf(os.Stdout, "Gerrit stream smoke succeeded: project=%s change=%s revision=%s\n", result.Project, result.Change, result.Revision)
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func envIntDefault(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}
