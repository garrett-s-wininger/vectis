package gerrit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"vectis/sdk/scm"
)

const maxErrorBytes = 4096

// Provider polls Gerrit changes and emits events when a change's current revision advances.
type Provider struct {
	client   *http.Client
	username string
	password string
}

type Option func(*Provider)

func NewProvider(opts ...Option) *Provider {
	p := &Provider{}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}

	return p
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) {
		p.client = client
	}
}

func WithBasicAuth(username, password string) Option {
	return func(p *Provider) {
		p.username = username
		p.password = password
	}
}

func (p *Provider) Poll(ctx context.Context, spec scm.PollSpec) (scm.PollResult, error) {
	baseURL := strings.TrimSpace(spec.BaseURL)
	if baseURL == "" {
		return scm.PollResult{}, fmt.Errorf("gerrit provider requires base_url")
	}

	query := pollQuery(spec)
	changes, err := p.queryChanges(ctx, baseURL, query)
	if err != nil {
		return scm.PollResult{}, err
	}

	current, ordered, err := normalizeChanges(changes)
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

	serverHash := hashServer(baseURL)
	events := make([]scm.Event, 0)
	for _, change := range ordered {
		prevRevision, existed := previous[change.Identity]
		if existed && prevRevision == change.Revision {
			continue
		}

		event, err := eventForChange(spec, serverHash, query, change, prevRevision)
		if err != nil {
			return scm.PollResult{}, err
		}

		events = append(events, event)
	}

	return scm.PollResult{Events: events, Cursor: cursor}, nil
}

func pollQuery(spec scm.PollSpec) string {
	parts := []string{}
	if project := strings.TrimSpace(spec.Project); project != "" {
		parts = append(parts, "project:"+project)
	}

	if branch := strings.TrimSpace(spec.Branch); branch != "" {
		parts = append(parts, "branch:"+branch)
	}

	if query := strings.TrimSpace(spec.Query); query != "" {
		parts = append(parts, query)
	} else {
		parts = append(parts, "status:open")
	}

	return strings.Join(parts, " ")
}

func (p *Provider) queryChanges(ctx context.Context, baseURL, rawQuery string) ([]scm.Change, error) {
	endpoint, err := p.queryEndpoint(baseURL, rawQuery)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(p.username) != "" || strings.TrimSpace(p.password) != "" {
		req.SetBasicAuth(p.username, p.password)
	}

	resp, err := p.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("query Gerrit changes: %w", err)
	}
	defer resp.Body.Close()

	var changes []scm.Change
	if err := decodeGerritJSON(resp, &changes); err != nil {
		return nil, fmt.Errorf("query Gerrit changes: %w", err)
	}

	return changes, nil
}

func (p *Provider) queryEndpoint(baseURL, rawQuery string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("Gerrit URL must be absolute")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("Gerrit URL must use http or https")
	}

	path := "/changes/"
	if strings.TrimSpace(p.username) != "" || strings.TrimSpace(p.password) != "" {
		path = "/a/changes/"
	}

	values := url.Values{}
	values.Set("q", strings.TrimSpace(rawQuery))
	values.Add("o", "CURRENT_REVISION")

	prefix := u.Scheme + "://" + u.Host
	if basePath := strings.TrimRight(u.EscapedPath(), "/"); basePath != "" {
		prefix += basePath
	}

	return prefix + path + "?" + values.Encode(), nil
}

func (p *Provider) httpClient() *http.Client {
	if p.client != nil {
		return p.client
	}

	return http.DefaultClient
}

func decodeGerritJSON(resp *http.Response, out any) error {
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, readErrorBody(resp.Body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	data = stripXSSIPrefix(data)
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode Gerrit JSON: %w", err)
	}

	return nil
}

func readErrorBody(body io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(body, maxErrorBytes))
	if err != nil {
		return ""
	}

	return string(stripXSSIPrefix(b))
}

func stripXSSIPrefix(b []byte) []byte {
	return []byte(strings.TrimPrefix(string(b), ")]}'\n"))
}

type cursor struct {
	Version int               `json:"version"`
	Changes map[string]string `json:"changes"`
}

func decodeCursor(raw string) (map[string]string, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}

	var cur cursor
	if err := json.Unmarshal([]byte(raw), &cur); err != nil {
		return nil, true, fmt.Errorf("decode gerrit cursor: %w", err)
	}

	if cur.Changes == nil {
		return nil, true, fmt.Errorf("decode gerrit cursor: changes is required")
	}

	changes := make(map[string]string, len(cur.Changes))
	for identity, revision := range cur.Changes {
		identity = strings.TrimSpace(identity)
		revision = strings.TrimSpace(revision)
		if identity == "" || revision == "" {
			return nil, true, fmt.Errorf("decode gerrit cursor: changes must not contain empty identities or revisions")
		}

		changes[identity] = revision
	}

	return changes, true, nil
}

func encodeCursor(changes map[string]string) (string, error) {
	if changes == nil {
		changes = map[string]string{}
	}

	data, err := json.Marshal(cursor{Version: 1, Changes: changes})
	if err != nil {
		return "", fmt.Errorf("encode gerrit cursor: %w", err)
	}

	return string(data), nil
}

type pollChange struct {
	Identity string
	Info     scm.Change
	Revision string
	Ref      string
}

func normalizeChanges(changes []scm.Change) (map[string]string, []pollChange, error) {
	current := make(map[string]string, len(changes))
	byIdentity := make(map[string]pollChange, len(changes))
	for _, change := range changes {
		identity := changeIdentity(change)
		if identity == "" {
			return nil, nil, fmt.Errorf("gerrit change is missing id, change_id, and number")
		}

		revision, ref, err := change.CurrentRevisionRef()
		if err != nil {
			return nil, nil, err
		}

		if existing, ok := current[identity]; ok && existing != revision {
			return nil, nil, fmt.Errorf("gerrit query returned duplicate change %q with different current revisions", identity)
		}

		current[identity] = revision
		byIdentity[identity] = pollChange{
			Identity: identity,
			Info:     change,
			Revision: revision,
			Ref:      ref,
		}
	}

	ordered := make([]pollChange, 0, len(byIdentity))
	for _, change := range byIdentity {
		ordered = append(ordered, change)
	}

	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Identity < ordered[j].Identity
	})

	return current, ordered, nil
}

func changeIdentity(change scm.Change) string {
	if id := strings.TrimSpace(change.ID); id != "" {
		return id
	}

	if changeID := strings.TrimSpace(change.ChangeID); changeID != "" {
		return changeID
	}

	if change.Number != 0 {
		return strconv.Itoa(change.Number)
	}

	return ""
}

type eventPayload struct {
	Provider         string `json:"provider"`
	EventType        string `json:"event_type,omitempty"`
	ServerHash       string `json:"server_hash"`
	Project          string `json:"project,omitempty"`
	Branch           string `json:"branch,omitempty"`
	Query            string `json:"query,omitempty"`
	ChangeID         string `json:"change_id,omitempty"`
	ID               string `json:"id,omitempty"`
	Number           int    `json:"number,omitempty"`
	Status           string `json:"status,omitempty"`
	CurrentRevision  string `json:"current_revision"`
	PreviousRevision string `json:"previous_revision,omitempty"`
	Ref              string `json:"ref"`
}

func eventForChange(spec scm.PollSpec, serverHash, query string, change pollChange, previousRevision string) (scm.Event, error) {
	payload := eventPayload{
		Provider:         providerName(spec.Provider),
		ServerHash:       serverHash,
		Project:          strings.TrimSpace(change.Info.Project),
		Branch:           strings.TrimSpace(change.Info.Branch),
		Query:            strings.TrimSpace(query),
		ChangeID:         strings.TrimSpace(change.Info.ChangeID),
		ID:               strings.TrimSpace(change.Info.ID),
		Number:           change.Info.Number,
		Status:           strings.TrimSpace(change.Info.Status),
		CurrentRevision:  change.Revision,
		PreviousRevision: previousRevision,
		Ref:              change.Ref,
	}

	if payload.Project == "" {
		payload.Project = strings.TrimSpace(spec.Project)
	}

	if payload.Branch == "" {
		payload.Branch = strings.TrimSpace(spec.Branch)
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return scm.Event{}, fmt.Errorf("marshal gerrit event payload: %w", err)
	}

	return scm.Event{
		Key:         fmt.Sprintf("gerrit:%s:%s:%s", shortServerHash(serverHash), change.Identity, change.Revision),
		PayloadJSON: string(payloadJSON),
	}, nil
}

func providerName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "gerrit"
	}

	return name
}

func hashServer(baseURL string) string {
	sum := sha256.Sum256([]byte(strings.TrimRight(strings.TrimSpace(baseURL), "/")))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func shortServerHash(serverHash string) string {
	hash := strings.TrimPrefix(serverHash, "sha256:")
	if len(hash) > 16 {
		return hash[:16]
	}

	return hash
}

var _ scm.PollProvider = (*Provider)(nil)
