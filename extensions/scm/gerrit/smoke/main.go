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

	scmgerrit "vectis/extensions/scm/gerrit"
)

func main() {
	var opts scmgerrit.SmokeOptions
	var outputJSON bool

	flag.StringVar(&opts.BaseURL, "url", envDefault("GERRIT_SCM_SMOKE_URL", scmgerrit.DefaultSmokeBaseURL), "Gerrit base URL")
	flag.StringVar(&opts.Project, "project", os.Getenv("GERRIT_SCM_SMOKE_PROJECT"), "Gerrit project query term")
	flag.StringVar(&opts.Branch, "branch", os.Getenv("GERRIT_SCM_SMOKE_BRANCH"), "Gerrit branch query term")
	flag.StringVar(&opts.Query, "query", os.Getenv("GERRIT_SCM_SMOKE_QUERY"), "Additional Gerrit query")
	flag.StringVar(&opts.Cursor, "cursor", os.Getenv("GERRIT_SCM_SMOKE_CURSOR"), "Existing Gerrit SCM cursor JSON")
	flag.StringVar(&opts.Username, "username", os.Getenv("GERRIT_SCM_SMOKE_USERNAME"), "Gerrit HTTP username")
	flag.StringVar(&opts.Password, "password", os.Getenv("GERRIT_SCM_SMOKE_PASSWORD"), "Gerrit HTTP password; prefer --password-file")
	flag.StringVar(&opts.PasswordFile, "password-file", os.Getenv("GERRIT_SCM_SMOKE_PASSWORD_FILE"), "File containing the Gerrit HTTP password")
	flag.BoolVar(&opts.EmitExisting, "emit-existing", envBoolDefault("GERRIT_SCM_SMOKE_EMIT_EXISTING", false), "Use an empty bootstrapped cursor so matching changes emit events")
	flag.IntVar(&opts.MinEvents, "min-events", envIntDefault("GERRIT_SCM_SMOKE_MIN_EVENTS", 0), "Minimum number of events the smoke must observe")
	flag.DurationVar(&opts.Timeout, "timeout", scmgerrit.DefaultSmokeTimeout, "Maximum time to wait for the Gerrit SCM smoke")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.Stdout = os.Stdout
	if outputJSON {
		opts.Stdout = os.Stderr
	}

	result, err := scmgerrit.RunSmoke(ctx, opts)
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

	fmt.Fprintf(os.Stdout, "Gerrit SCM smoke succeeded: query=%q events=%d cursor=%t\n", result.Query, result.EventCount, result.Cursor != "")
}

func envDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

func envBoolDefault(key string, fallback bool) bool {
	value := os.Getenv(key)
	switch value {
	case "1", "t", "T", "true", "TRUE", "True", "yes", "YES", "Yes", "y", "Y", "on", "ON", "On":
		return true
	case "0", "f", "F", "false", "FALSE", "False", "no", "NO", "No", "n", "N", "off", "OFF", "Off":
		return false
	default:
		return fallback
	}
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
