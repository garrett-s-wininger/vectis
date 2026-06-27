package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	gerritsmoke "vectis/deploy/gerrit"
)

func main() {
	var opts gerritsmoke.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.URL, "url", envDefault("GERRIT_SMOKE_URL", gerritsmoke.DefaultSmokeURL), "Gerrit base URL")
	flag.StringVar(&opts.AccountID, "account-id", envDefault("GERRIT_SMOKE_ACCOUNT_ID", gerritsmoke.DefaultSmokeAccountID), "Development auth account id")
	flag.StringVar(&opts.Username, "username", envDefault("GERRIT_SMOKE_USERNAME", gerritsmoke.DefaultSmokeUsername), "Gerrit username for HTTP auth")
	flag.StringVar(&opts.Project, "project", os.Getenv("GERRIT_SMOKE_PROJECT"), "Project name; generated when empty")
	flag.StringVar(&opts.ProjectPrefix, "project-prefix", envDefault("GERRIT_SMOKE_PROJECT_PREFIX", gerritsmoke.DefaultSmokeProjectPrefix), "Generated project prefix")
	flag.StringVar(&opts.Label, "label", envDefault("GERRIT_SMOKE_LABEL", gerritsmoke.DefaultSmokeLabel), "Review label to vote")
	flag.StringVar(&opts.Value, "value", envDefault("GERRIT_SMOKE_VALUE", gerritsmoke.DefaultSmokeValue), "Review label vote")
	flag.StringVar(&opts.Message, "message", envDefault("GERRIT_SMOKE_MESSAGE", gerritsmoke.DefaultSmokeMessage), "Review message")
	flag.DurationVar(&opts.Timeout, "timeout", gerritsmoke.DefaultSmokeTimeout, "Maximum time to wait for the Gerrit smoke")
	flag.StringVar(&opts.GitBin, "git", envDefault("GERRIT_SMOKE_GIT", gerritsmoke.DefaultSmokeGitBin), "git executable")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := gerritsmoke.RunSmoke(ctx, opts)
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

	fmt.Fprintf(os.Stdout, "Gerrit smoke succeeded: project=%s change=%s revision=%s fetch_ref=%s checkout=%t review=%t wrong_password_denied=%t\n", result.Project, result.Change, result.Revision, result.FetchRef, result.CheckoutVerified, result.ReviewPosted, result.WrongPasswordDenied)
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
