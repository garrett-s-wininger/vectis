package git

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"unicode"

	"vectis/sdk/scm"
)

const defaultRefPattern = "refs/heads/*"

// Provider polls generic Git refs with git ls-remote.
type Provider struct {
	runner remoteRunner
}

// NewProvider returns a generic Git SCM poll provider.
func NewProvider() *Provider {
	return newProviderWithRunner(commandRunner{})
}

func newProviderWithRunner(runner remoteRunner) *Provider {
	if runner == nil {
		runner = commandRunner{}
	}

	return &Provider{runner: runner}
}

func (p *Provider) Poll(ctx context.Context, spec scm.PollSpec) (scm.PollResult, error) {
	remote, err := remoteURL(spec)
	if err != nil {
		return scm.PollResult{}, err
	}

	patterns := refPatterns(spec)
	refs, err := p.runner.ListRemote(ctx, remote, patterns)
	if err != nil {
		return scm.PollResult{}, err
	}

	current, orderedRefs, err := normalizeRefs(refs)
	if err != nil {
		return scm.PollResult{}, err
	}

	previous, bootstrapped, err := decodeCursor(spec.Cursor)
	if err != nil {
		return scm.PollResult{}, err
	}

	cursor, err := encodeCursor(current)
	if err != nil {
		return scm.PollResult{}, err
	}

	if !bootstrapped {
		return scm.PollResult{Cursor: cursor}, nil
	}

	remoteHash := hashRemote(remote)
	events := make([]scm.Event, 0)
	for _, ref := range orderedRefs {
		previousSHA, existed := previous[ref.Name]
		if existed && previousSHA == ref.SHA {
			continue
		}

		event, err := eventForRef(spec, remoteHash, ref, previousSHA)
		if err != nil {
			return scm.PollResult{}, err
		}

		events = append(events, event)
	}

	return scm.PollResult{Events: events, Cursor: cursor}, nil
}

func remoteURL(spec scm.PollSpec) (string, error) {
	baseURL := strings.TrimSpace(spec.BaseURL)
	project := strings.TrimSpace(spec.Project)
	switch {
	case baseURL == "" && project == "":
		return "", fmt.Errorf("git provider requires base_url or project remote")
	case baseURL == "":
		return project, nil
	case project == "":
		return baseURL, nil
	default:
		return strings.TrimRight(baseURL, "/") + "/" + strings.TrimLeft(project, "/"), nil
	}
}

func refPatterns(spec scm.PollSpec) []string {
	var patterns []string
	if branch := strings.TrimSpace(spec.Branch); branch != "" {
		patterns = append(patterns, normalizeRefPattern(branch))
	}

	for _, token := range queryTokens(spec.Query) {
		patterns = append(patterns, normalizeRefPattern(token))
	}

	if len(patterns) == 0 {
		return []string{defaultRefPattern}
	}

	seen := make(map[string]struct{}, len(patterns))
	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		if _, ok := seen[pattern]; ok {
			continue
		}

		seen[pattern] = struct{}{}
		out = append(out, pattern)
	}

	return out
}

func queryTokens(query string) []string {
	return strings.FieldsFunc(query, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
}

func normalizeRefPattern(value string) string {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return value
	case strings.HasPrefix(value, "refs/"):
		return value
	case strings.HasPrefix(value, "heads/") || strings.HasPrefix(value, "tags/"):
		return "refs/" + value
	default:
		return "refs/heads/" + value
	}
}

type cursor struct {
	Version int               `json:"version"`
	Refs    map[string]string `json:"refs"`
}

func decodeCursor(raw string) (map[string]string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}

	var cur cursor
	if err := json.Unmarshal([]byte(raw), &cur); err != nil {
		return nil, true, fmt.Errorf("decode git cursor: %w", err)
	}

	if cur.Refs == nil {
		return nil, true, fmt.Errorf("decode git cursor: refs is required")
	}

	refs := make(map[string]string, len(cur.Refs))
	for ref, sha := range cur.Refs {
		ref = strings.TrimSpace(ref)
		sha = strings.TrimSpace(sha)
		if ref == "" || sha == "" {
			return nil, true, fmt.Errorf("decode git cursor: refs must not contain empty names or hashes")
		}

		refs[ref] = sha
	}

	return refs, true, nil
}

func encodeCursor(refs map[string]string) (string, error) {
	if refs == nil {
		refs = map[string]string{}
	}

	data, err := json.Marshal(cursor{Version: 1, Refs: refs})
	if err != nil {
		return "", fmt.Errorf("encode git cursor: %w", err)
	}

	return string(data), nil
}

type remoteRef struct {
	SHA  string
	Name string
}

func normalizeRefs(refs []remoteRef) (map[string]string, []remoteRef, error) {
	current := make(map[string]string, len(refs))
	for _, ref := range refs {
		name := strings.TrimSpace(ref.Name)
		sha := strings.TrimSpace(ref.SHA)
		if strings.HasSuffix(name, "^{}") {
			continue
		}

		if name == "" || sha == "" {
			return nil, nil, fmt.Errorf("git ls-remote returned an empty ref name or hash")
		}

		if existing, ok := current[name]; ok && existing != sha {
			return nil, nil, fmt.Errorf("git ls-remote returned duplicate ref %q with different hashes", name)
		}

		current[name] = sha
	}

	ordered := make([]remoteRef, 0, len(current))
	for name, sha := range current {
		ordered = append(ordered, remoteRef{Name: name, SHA: sha})
	}

	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Name < ordered[j].Name
	})

	return current, ordered, nil
}

type eventPayload struct {
	Provider    string `json:"provider"`
	RemoteHash  string `json:"remote_hash"`
	Project     string `json:"project,omitempty"`
	Ref         string `json:"ref"`
	Branch      string `json:"branch,omitempty"`
	SHA         string `json:"sha"`
	PreviousSHA string `json:"previous_sha,omitempty"`
}

func eventForRef(spec scm.PollSpec, remoteHash string, ref remoteRef, previousSHA string) (scm.Event, error) {
	payload := eventPayload{
		Provider:    providerName(spec.Provider),
		RemoteHash:  remoteHash,
		Project:     payloadProject(spec),
		Ref:         ref.Name,
		Branch:      branchName(ref.Name),
		SHA:         ref.SHA,
		PreviousSHA: previousSHA,
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return scm.Event{}, fmt.Errorf("marshal git event payload: %w", err)
	}

	return scm.Event{
		Key:         fmt.Sprintf("git:%s:%s:%s", shortRemoteHash(remoteHash), ref.Name, ref.SHA),
		PayloadJSON: string(payloadJSON),
	}, nil
}

func payloadProject(spec scm.PollSpec) string {
	if strings.TrimSpace(spec.BaseURL) == "" {
		return ""
	}

	return strings.TrimSpace(spec.Project)
}

func providerName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "git"
	}

	return name
}

func branchName(ref string) string {
	if !strings.HasPrefix(ref, "refs/heads/") {
		return ""
	}

	return strings.TrimPrefix(ref, "refs/heads/")
}

func hashRemote(remote string) string {
	sum := sha256.Sum256([]byte(remote))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func shortRemoteHash(remoteHash string) string {
	hash := strings.TrimPrefix(remoteHash, "sha256:")
	if len(hash) > 16 {
		return hash[:16]
	}

	return hash
}

type remoteRunner interface {
	ListRemote(ctx context.Context, remote string, patterns []string) ([]remoteRef, error)
}

type commandRunner struct{}

func (commandRunner) ListRemote(ctx context.Context, remote string, patterns []string) ([]remoteRef, error) {
	args := append([]string{"ls-remote", remote}, patterns...)
	cmd := exec.CommandContext(ctx, "git", args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			return nil, fmt.Errorf("git ls-remote failed: %w", err)
		}

		return nil, fmt.Errorf("git ls-remote failed: %w: %s", err, message)
	}

	return parseRemoteRefs(stdout.String())
}

func parseRemoteRefs(output string) ([]remoteRef, error) {
	var refs []remoteRef
	for lineNo, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) != 2 {
			return nil, fmt.Errorf("parse git ls-remote output line %d: expected hash and ref", lineNo+1)
		}

		refs = append(refs, remoteRef{SHA: fields[0], Name: fields[1]})
	}

	return refs, nil
}

var _ scm.PollProvider = (*Provider)(nil)
