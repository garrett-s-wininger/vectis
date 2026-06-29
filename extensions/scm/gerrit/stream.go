package gerrit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"vectis/sdk/scm"
)

const maxStreamEventBytes = 1024 * 1024

type StreamOptions struct {
	Provider string
	BaseURL  string
	Project  string
	Branch   string
	Query    string
}

type StreamHandler func(context.Context, scm.Event) error

func ConsumeStream(ctx context.Context, r io.Reader, opts StreamOptions, handle StreamHandler) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if r == nil {
		return fmt.Errorf("gerrit stream reader is required")
	}

	if handle == nil {
		return fmt.Errorf("gerrit stream handler is required")
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxStreamEventBytes)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}

		event, ok, err := NormalizeStreamEvent(scanner.Bytes(), opts)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		if err := handle(ctx, event); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read gerrit stream event: %w", err)
	}

	return ctx.Err()
}

func NormalizeStreamEvent(raw []byte, opts StreamOptions) (scm.Event, bool, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return scm.Event{}, false, nil
	}

	var event gerritStreamEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return scm.Event{}, false, fmt.Errorf("decode gerrit stream event: %w", err)
	}

	if event.Change == nil || event.PatchSet == nil {
		return scm.Event{}, false, nil
	}

	change := *event.Change
	patchSet := *event.PatchSet
	project := strings.TrimSpace(change.Project)
	branch := strings.TrimSpace(change.Branch)
	if !matchesStreamFilter(project, opts.Project) || !matchesStreamFilter(branch, opts.Branch) {
		return scm.Event{}, false, nil
	}

	revision := strings.TrimSpace(patchSet.Revision)
	ref := strings.TrimSpace(patchSet.Ref)
	if revision == "" {
		return scm.Event{}, false, fmt.Errorf("gerrit stream event %q missing patchSet.revision", event.Type)
	}

	if ref == "" {
		return scm.Event{}, false, fmt.Errorf("gerrit stream event %q missing patchSet.ref", event.Type)
	}

	baseURL := strings.TrimSpace(opts.BaseURL)
	if baseURL == "" {
		return scm.Event{}, false, fmt.Errorf("gerrit stream event %q requires base_url", event.Type)
	}

	changeID := strings.TrimSpace(change.ID)
	number, err := parseStreamNumber(change.Number)
	if err != nil {
		return scm.Event{}, false, err
	}

	identity := streamChangeIdentity(project, branch, changeID, number)
	if identity == "" {
		return scm.Event{}, false, fmt.Errorf("gerrit stream event %q missing change identity", event.Type)
	}

	serverHash := hashServer(baseURL)
	payload := eventPayload{
		Provider:        providerName(opts.Provider),
		EventType:       strings.TrimSpace(event.Type),
		ServerHash:      serverHash,
		Project:         project,
		Branch:          branch,
		Query:           strings.TrimSpace(opts.Query),
		ChangeID:        changeID,
		ID:              identity,
		Number:          number,
		Status:          strings.TrimSpace(change.Status),
		CurrentRevision: revision,
		Ref:             ref,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return scm.Event{}, false, fmt.Errorf("marshal gerrit stream event payload: %w", err)
	}

	return scm.Event{
		Key:         fmt.Sprintf("gerrit:%s:%s:%s", shortServerHash(serverHash), identity, revision),
		PayloadJSON: string(payloadJSON),
	}, true, nil
}

func matchesStreamFilter(value, filter string) bool {
	filter = strings.TrimSpace(filter)
	return filter == "" || strings.TrimSpace(value) == filter
}

func streamChangeIdentity(project, branch, changeID string, number int) string {
	changeID = strings.TrimSpace(changeID)
	if changeID != "" {
		project = strings.TrimSpace(project)
		branch = strings.TrimSpace(branch)
		if project != "" && branch != "" {
			return project + "~" + branch + "~" + changeID
		}

		return changeID
	}

	if number != 0 {
		return strconv.Itoa(number)
	}

	return ""
}

func parseStreamNumber(raw json.RawMessage) (int, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0, nil
	}

	var number int
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, nil
	}

	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return 0, fmt.Errorf("decode gerrit stream change.number: %w", err)
	}

	text = strings.TrimSpace(text)
	if text == "" {
		return 0, nil
	}

	parsed, err := strconv.Atoi(text)
	if err != nil {
		return 0, fmt.Errorf("decode gerrit stream change.number: %w", err)
	}

	return parsed, nil
}

type gerritStreamEvent struct {
	Type     string                `json:"type"`
	Change   *gerritStreamChange   `json:"change"`
	PatchSet *gerritStreamPatchSet `json:"patchSet"`
}

type gerritStreamChange struct {
	Project string          `json:"project"`
	Branch  string          `json:"branch"`
	ID      string          `json:"id"`
	Number  json.RawMessage `json:"number"`
	Status  string          `json:"status"`
}

type gerritStreamPatchSet struct {
	Revision string `json:"revision"`
	Ref      string `json:"ref"`
}
