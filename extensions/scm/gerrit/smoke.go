package gerrit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"vectis/sdk/scm"
)

const (
	DefaultSmokeBaseURL = "http://127.0.0.1:18088"
	DefaultSmokeTimeout = 30 * time.Second
)

const emptyBootstrappedCursor = `{"version":1,"changes":{}}`

type SmokeOptions struct {
	BaseURL      string
	Project      string
	Branch       string
	Query        string
	Cursor       string
	Username     string
	Password     string
	PasswordFile string
	EmitExisting bool
	MinEvents    int
	Timeout      time.Duration
	Stdout       io.Writer
}

type SmokeResult struct {
	Status       string       `json:"status"`
	BaseURL      string       `json:"base_url"`
	Project      string       `json:"project,omitempty"`
	Branch       string       `json:"branch,omitempty"`
	Query        string       `json:"query"`
	Cursor       string       `json:"cursor"`
	EventCount   int          `json:"event_count"`
	Events       []SmokeEvent `json:"events,omitempty"`
	EmitExisting bool         `json:"emit_existing"`
}

type SmokeEvent struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func RunSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeSmokeOptions(opts)
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()

	var lastErr error
	for {
		result, err := runSmokeOnce(ctx, opts)
		if err == nil {
			return result, nil
		}

		lastErr = err
		select {
		case <-ctx.Done():
			return SmokeResult{}, fmt.Errorf("gerrit scm smoke did not succeed within %s: %w", opts.Timeout, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(opts.Stdout, "Waiting for Gerrit SCM endpoint %s: %v\n", opts.BaseURL, err)
		}
	}
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.BaseURL = strings.TrimRight(strings.TrimSpace(opts.BaseURL), "/")
	if opts.BaseURL == "" {
		opts.BaseURL = DefaultSmokeBaseURL
	}

	opts.Project = strings.TrimSpace(opts.Project)
	opts.Branch = strings.TrimSpace(opts.Branch)
	opts.Query = strings.TrimSpace(opts.Query)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Username = strings.TrimSpace(opts.Username)
	opts.Password = strings.TrimSpace(opts.Password)
	opts.PasswordFile = strings.TrimSpace(opts.PasswordFile)

	if opts.Timeout == 0 {
		opts.Timeout = DefaultSmokeTimeout
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.BaseURL == "" {
		return fmt.Errorf("gerrit scm smoke url is required")
	}

	parsed, err := url.Parse(opts.BaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("gerrit scm smoke url must be absolute")
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("gerrit scm smoke url must use http or https")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("gerrit scm smoke timeout must be > 0")
	}

	hasUsername := opts.Username != ""
	hasPassword := opts.Password != "" || opts.PasswordFile != ""
	if hasUsername != hasPassword {
		return fmt.Errorf("gerrit scm smoke username and password must be configured together")
	}

	if opts.MinEvents < 0 {
		return fmt.Errorf("gerrit scm smoke min events must be >= 0")
	}

	return nil
}

func runSmokeOnce(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	provider, err := Config{
		Username:     opts.Username,
		Password:     opts.Password,
		PasswordFile: opts.PasswordFile,
	}.NewProvider()

	if err != nil {
		return SmokeResult{}, err
	}

	spec := scm.PollSpec{
		Provider: "gerrit",
		BaseURL:  opts.BaseURL,
		Project:  opts.Project,
		Branch:   opts.Branch,
		Query:    opts.Query,
		Cursor:   opts.Cursor,
	}

	if opts.EmitExisting && spec.Cursor == "" {
		spec.Cursor = emptyBootstrappedCursor
	}

	pollResult, err := provider.Poll(ctx, spec)
	if err != nil {
		return SmokeResult{}, err
	}

	if len(pollResult.Events) < opts.MinEvents {
		return SmokeResult{}, fmt.Errorf("gerrit scm smoke emitted %d events, want at least %d", len(pollResult.Events), opts.MinEvents)
	}

	events := make([]SmokeEvent, 0, len(pollResult.Events))
	for _, event := range pollResult.Events {
		events = append(events, SmokeEvent{
			Key:     event.Key,
			Payload: json.RawMessage(event.PayloadJSON),
		})
	}

	return SmokeResult{
		Status:       "ok",
		BaseURL:      opts.BaseURL,
		Project:      opts.Project,
		Branch:       opts.Branch,
		Query:        pollQuery(spec),
		Cursor:       pollResult.Cursor,
		EventCount:   len(events),
		Events:       events,
		EmitExisting: opts.EmitExisting,
	}, nil
}
