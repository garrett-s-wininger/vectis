package gerrit

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	gerritaction "vectis/extensions/actions/gerrit"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/action/custom"
	"vectis/internal/interfaces"
	sdkscm "vectis/sdk/scm"
)

const (
	DefaultSmokeURL           = "http://127.0.0.1:18088"
	DefaultSmokeAccountID     = "1000000"
	DefaultSmokeUsername      = "admin"
	DefaultSmokeProjectPrefix = "vectis-smoke"
	DefaultSmokeLabel         = "Code-Review"
	DefaultSmokeValue         = "+1"
	DefaultSmokeMessage       = "Vectis Gerrit smoke review"
	DefaultSmokeTimeout       = 90 * time.Second
	DefaultSmokeGitBin        = "git"
)

type SmokeOptions struct {
	URL           string
	AccountID     string
	Username      string
	Project       string
	ProjectPrefix string
	Label         string
	Value         string
	Message       string
	Timeout       time.Duration
	GitBin        string
	Stdout        io.Writer
}

type SmokeResult struct {
	Status              string `json:"status"`
	URL                 string `json:"url"`
	Project             string `json:"project"`
	Change              string `json:"change"`
	Revision            string `json:"revision"`
	FetchRef            string `json:"fetch_ref"`
	PollDiscovered      bool   `json:"poll_discovered"`
	CheckoutVerified    bool   `json:"checkout_verified"`
	ReviewPosted        bool   `json:"review_posted"`
	WrongPasswordDenied bool   `json:"wrong_password_denied"`
}

type smokeRunner struct {
	opts   SmokeOptions
	client *http.Client
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

	runner := smokeRunner{
		opts: opts,
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}

	if err := runner.waitForGerrit(ctx); err != nil {
		return SmokeResult{}, err
	}

	return runner.run(ctx)
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.URL = strings.TrimRight(strings.TrimSpace(opts.URL), "/")
	if opts.URL == "" {
		opts.URL = DefaultSmokeURL
	}

	opts.AccountID = strings.TrimSpace(opts.AccountID)
	if opts.AccountID == "" {
		opts.AccountID = DefaultSmokeAccountID
	}

	opts.Username = strings.TrimSpace(opts.Username)
	if opts.Username == "" {
		opts.Username = DefaultSmokeUsername
	}

	opts.Project = cleanProjectName(opts.Project)
	opts.ProjectPrefix = cleanProjectName(opts.ProjectPrefix)
	if opts.ProjectPrefix == "" {
		opts.ProjectPrefix = DefaultSmokeProjectPrefix
	}

	if opts.Project == "" {
		opts.Project = fmt.Sprintf("%s-%d", opts.ProjectPrefix, time.Now().UTC().UnixNano())
	}

	opts.Label = strings.TrimSpace(opts.Label)
	if opts.Label == "" {
		opts.Label = DefaultSmokeLabel
	}

	opts.Value = strings.TrimSpace(opts.Value)
	if opts.Value == "" {
		opts.Value = DefaultSmokeValue
	}

	opts.Message = strings.TrimSpace(opts.Message)
	if opts.Message == "" {
		opts.Message = DefaultSmokeMessage
	}

	if opts.Timeout == 0 {
		opts.Timeout = DefaultSmokeTimeout
	}

	opts.GitBin = strings.TrimSpace(opts.GitBin)
	if opts.GitBin == "" {
		opts.GitBin = DefaultSmokeGitBin
	}

	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}

	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.URL == "" {
		return fmt.Errorf("gerrit smoke url is required")
	}

	parsed, err := url.Parse(opts.URL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("gerrit smoke url must be absolute")
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("gerrit smoke url must use http or https")
	}

	if opts.Project == "" {
		return fmt.Errorf("gerrit smoke project is required")
	}

	if opts.Timeout <= 0 {
		return fmt.Errorf("gerrit smoke timeout must be > 0")
	}

	return nil
}

func (r smokeRunner) waitForGerrit(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	var lastErr error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.opts.URL+"/config/server/version", nil)
		if err != nil {
			return err
		}

		resp, err := r.client.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}

			err = fmt.Errorf("status=%d", resp.StatusCode)
		}

		lastErr = err
		select {
		case <-ctx.Done():
			if !deadline.IsZero() {
				return fmt.Errorf("gerrit smoke did not reach %s by %s: %w", r.opts.URL, deadline.Format(time.RFC3339), lastErr)
			}

			return fmt.Errorf("gerrit smoke did not reach %s: %w", r.opts.URL, lastErr)
		case <-time.After(time.Second):
			fmt.Fprintf(r.opts.Stdout, "Waiting for Gerrit endpoint %s: %v\n", r.opts.URL, lastErr)
		}
	}
}

func (r smokeRunner) run(ctx context.Context) (SmokeResult, error) {
	accessToken, err := r.loginDevelopmentAccount(ctx)
	if err != nil {
		return SmokeResult{}, err
	}

	password, err := r.generateHTTPPassword(ctx, accessToken)
	if err != nil {
		return SmokeResult{}, err
	}

	if err := r.createProject(ctx, password); err != nil {
		return SmokeResult{}, err
	}

	changeID, err := randomChangeID()
	if err != nil {
		return SmokeResult{}, err
	}

	workspaceRoot, err := os.MkdirTemp("", "vectis-gerrit-smoke-*")
	if err != nil {
		return SmokeResult{}, fmt.Errorf("create gerrit smoke workspace: %w", err)
	}
	defer os.RemoveAll(workspaceRoot)

	if err := r.pushChange(ctx, workspaceRoot, password, changeID); err != nil {
		return SmokeResult{}, err
	}

	client := gerritaction.Client{
		BaseURL:    r.opts.URL,
		Username:   r.opts.Username,
		Password:   password,
		HTTPClient: r.client,
	}

	discovered, err := sdkscm.PollChange(ctx, client, sdkscm.PollOptions{
		Query: sdkscm.Query{
			Project:  r.opts.Project,
			Branch:   "master",
			Status:   "open",
			ChangeID: changeID,
		},
		ChangeID: changeID,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	discoveredRevision, discoveredFetchRef, err := discovered.CurrentRevisionRef()
	if err != nil {
		return SmokeResult{}, err
	}

	change := fmt.Sprintf("%s~master~%s", r.opts.Project, changeID)
	info, err := client.ChangeDetail(ctx, change)
	if err != nil {
		return SmokeResult{}, err
	}

	revision, fetchRef, err := info.CurrentRevisionRef()
	if err != nil {
		return SmokeResult{}, err
	}

	if discoveredRevision != revision || discoveredFetchRef != fetchRef {
		return SmokeResult{}, fmt.Errorf("gerrit poll discovered revision/ref %s/%s, detail returned %s/%s", discoveredRevision, discoveredFetchRef, revision, fetchRef)
	}

	workspace := filepath.Join(workspaceRoot, "checkout")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		return SmokeResult{}, fmt.Errorf("create checkout workspace: %w", err)
	}

	if err := r.checkoutChange(ctx, workspace, fetchRef); err != nil {
		return SmokeResult{}, err
	}

	secretPath := filepath.Join(workspace, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(secretPath, 0o700); err != nil {
		return SmokeResult{}, fmt.Errorf("create gerrit password directory: %w", err)
	}

	if err := os.WriteFile(filepath.Join(secretPath, "http-password"), []byte(password), 0o600); err != nil {
		return SmokeResult{}, fmt.Errorf("write gerrit password file: %w", err)
	}

	if err := r.postReview(ctx, workspace, change, revision, ".vectis/secrets/gerrit/http-password", true); err != nil {
		return SmokeResult{}, err
	}

	if err := os.WriteFile(filepath.Join(secretPath, "wrong-password"), []byte("wrong-"+password), 0o600); err != nil {
		return SmokeResult{}, fmt.Errorf("write wrong gerrit password file: %w", err)
	}

	if err := r.postReview(ctx, workspace, change, revision, ".vectis/secrets/gerrit/wrong-password", false); err != nil {
		return SmokeResult{}, err
	}

	return SmokeResult{
		Status:              "ok",
		URL:                 r.opts.URL,
		Project:             r.opts.Project,
		Change:              change,
		Revision:            revision,
		FetchRef:            fetchRef,
		PollDiscovered:      true,
		CheckoutVerified:    true,
		ReviewPosted:        true,
		WrongPasswordDenied: true,
	}, nil
}

func (r smokeRunner) loginDevelopmentAccount(ctx context.Context) (string, error) {
	loginURL := r.opts.URL + "/login/%23%2F?account_id=" + url.QueryEscape(r.opts.AccountID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, loginURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("gerrit development login: %w", err)
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusFound && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gerrit development login failed: status=%d", resp.StatusCode)
	}

	for _, cookie := range resp.Cookies() {
		if cookie.Name == "GerritAccount" && cookie.Value != "" {
			return cookie.Value, nil
		}
	}

	return "", fmt.Errorf("gerrit development login did not return GerritAccount cookie")
}

func (r smokeRunner) generateHTTPPassword(ctx context.Context, accessToken string) (string, error) {
	endpoint := r.opts.URL + "/a/accounts/self/password.http?access_token=" + url.QueryEscape(accessToken)
	body := strings.NewReader(`{"generate": true}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, body)
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("generate gerrit http password: %w", err)
	}
	defer resp.Body.Close()

	var password string
	if err := gerritaction.DecodeJSON(resp, &password); err != nil {
		return "", fmt.Errorf("generate gerrit http password: %w", err)
	}

	if password == "" {
		return "", fmt.Errorf("generate gerrit http password returned empty token")
	}

	return password, nil
}

func (r smokeRunner) createProject(ctx context.Context, password string) error {
	endpoint := r.opts.URL + "/a/projects/" + url.PathEscape(r.opts.Project)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, strings.NewReader(`{"create_empty_commit": true}`))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.SetBasicAuth(r.opts.Username, password)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("create gerrit project: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("create gerrit project failed: status=%d body=%s", resp.StatusCode, gerritaction.ReadErrorBody(resp.Body))
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (r smokeRunner) pushChange(ctx context.Context, workspaceRoot, password, changeID string) error {
	repoDir := filepath.Join(workspaceRoot, "repo")
	header := "Authorization: Basic " + basicAuth(r.opts.Username, password)
	remote := r.opts.URL + "/a/" + url.PathEscape(r.opts.Project)
	if err := r.runGit(ctx, workspaceRoot, []string{"-c", "http.extraHeader=" + header}, []string{"clone", remote, repoDir}, "git clone"); err != nil {
		return err
	}

	readme := fmt.Sprintf("hello from Vectis Gerrit smoke\nproject=%s\nchange=%s\n", r.opts.Project, changeID)
	steps := []struct {
		args []string
		op   string
	}{
		{args: []string{"config", "user.name", "Vectis Gerrit Smoke"}, op: "git config user.name"},
		{args: []string{"config", "user.email", "vectis-smoke@example.org"}, op: "git config user.email"},
	}

	for _, step := range steps {
		if err := r.runGit(ctx, repoDir, nil, step.args, step.op); err != nil {
			return err
		}
	}

	if err := os.WriteFile(filepath.Join(repoDir, "README.md"), []byte(readme), 0o644); err != nil {
		return fmt.Errorf("write gerrit smoke README: %w", err)
	}

	if err := r.runGit(ctx, repoDir, nil, []string{"add", "README.md"}, "git add"); err != nil {
		return err
	}

	message := fmt.Sprintf("Gerrit smoke\n\nChange-Id: %s", changeID)
	if err := r.runGit(ctx, repoDir, nil, []string{"commit", "-m", message}, "git commit"); err != nil {
		return err
	}

	return r.runGit(ctx, repoDir, []string{"-c", "http.extraHeader=" + header}, []string{"push", "origin", "HEAD:refs/for/master"}, "git push")
}

func (r smokeRunner) checkoutChange(ctx context.Context, workspace, fetchRef string) error {
	state := &action.ExecutionState{
		Workspace: workspace,
		Logger:    interfaces.NewLogger("gerrit-smoke-checkout"),
	}

	result := builtins.NewCheckoutAction(nil).Execute(ctx, state, map[string]any{
		"url": r.opts.URL + "/" + r.opts.Project,
		"ref": fetchRef,
	}, nil)

	if result.Status != action.StatusSuccess {
		if result.Error != nil {
			return result.Error
		}

		return fmt.Errorf("gerrit checkout action failed")
	}

	data, err := os.ReadFile(filepath.Join(workspace, "README.md"))
	if err != nil {
		return fmt.Errorf("read gerrit checkout README: %w", err)
	}

	if !bytes.Contains(data, []byte("hello from Vectis Gerrit smoke")) {
		return fmt.Errorf("gerrit checkout README did not contain smoke payload")
	}

	return nil
}

func (r smokeRunner) postReview(ctx context.Context, workspace, change, revision, passwordFile string, wantSuccess bool) error {
	logs := &captureLogStream{}
	state := &action.ExecutionState{
		Workspace: workspace,
		Logger:    interfaces.NewLogger("gerrit-smoke-review"),
		LogStream: logs,
	}

	inputs := map[string]any{
		"url":           r.opts.URL,
		"change":        change,
		"revision":      revision,
		"message":       r.opts.Message,
		"label":         r.opts.Label,
		"value":         r.opts.Value,
		"username":      r.opts.Username,
		"password_file": passwordFile,
	}

	descriptor, err := gerritReviewDescriptor()
	if err != nil {
		return err
	}

	result := custom.NewProcessAction(descriptor, nil).Execute(ctx, state, inputs, nil)
	if wantSuccess {
		if result.Status != action.StatusSuccess {
			if result.Error != nil {
				return fmt.Errorf("%w: logs=%s", result.Error, logs.String())
			}

			return fmt.Errorf("gerrit review action failed: logs=%s", logs.String())
		}

		return nil
	}

	if result.Status != action.StatusFailure {
		return fmt.Errorf("gerrit review with wrong password unexpectedly succeeded")
	}

	if !strings.Contains(logs.String(), "401") {
		return fmt.Errorf("gerrit review wrong password logs = %q error=%v, want 401", logs.String(), result.Error)
	}

	return nil
}

func gerritReviewDescriptor() (actionregistry.Descriptor, error) {
	root, err := findRepoRoot()
	if err != nil {
		return actionregistry.Descriptor{}, err
	}

	source, err := actionregistry.NewLocalManifestSource(filepath.Join(root, "extensions", "actions"))
	if err != nil {
		return actionregistry.Descriptor{}, err
	}

	return source.ResolveDescriptor("gerrit/review@v1")
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "extensions", "actions", "gerrit", "review", "action.json")); err == nil {
				return dir, nil
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find repository root from %s", dir)
		}

		dir = parent
	}
}

type captureLogStream struct {
	mu     sync.Mutex
	chunks []string
}

func (s *captureLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.chunks = append(s.chunks, string(chunk.GetData()))
	return nil
}

func (s *captureLogStream) CloseSend() error { return nil }

func (s *captureLogStream) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.chunks, "")
}

func (r smokeRunner) runGit(ctx context.Context, dir string, gitOpts, args []string, operation string) error {
	fullArgs := append([]string{}, gitOpts...)
	fullArgs = append(fullArgs, args...)
	cmd := exec.CommandContext(ctx, r.opts.GitBin, fullArgs...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s failed: %w: %s", operation, err, sanitizeGerritOutput(stderr.String()+stdout.String()))
	}

	return nil
}

func randomChangeID() (string, error) {
	var data [20]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}

	return "I" + hex.EncodeToString(data[:]), nil
}

func basicAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

func cleanProjectName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	value = strings.ReplaceAll(value, " ", "-")
	return value
}

func sanitizeGerritOutput(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	lines := strings.Split(value, "\n")
	for i, line := range lines {
		if strings.Contains(strings.ToLower(line), "authorization: basic") {
			lines[i] = "authorization header redacted"
		}
	}

	return strings.Join(lines, "\n")
}
