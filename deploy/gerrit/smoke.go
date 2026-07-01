package gerrit

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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
	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/action/custom"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/database"
	_ "vectis/internal/dbdrivers"
	"vectis/internal/interfaces"
	jobexec "vectis/internal/job"
	"vectis/internal/migrations"
	"vectis/internal/scmpoller"
	"vectis/internal/secrets"
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
	SCMProviderPolled   bool   `json:"scm_provider_polled"`
	SCMEventEmitted     bool   `json:"scm_event_emitted"`
	CheckoutVerified    bool   `json:"checkout_verified"`
	ReviewPosted        bool   `json:"review_posted"`
	WrongPasswordDenied bool   `json:"wrong_password_denied"`
	JobExecuted         bool   `json:"job_executed"`
	JobReviewPosted     bool   `json:"job_review_posted"`
	PollerBackstop      bool   `json:"poller_backstop"`
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

	scmSmoke, err := scmgerrit.RunSmoke(ctx, scmgerrit.SmokeOptions{
		BaseURL:      r.opts.URL,
		Project:      r.opts.Project,
		Branch:       "master",
		Query:        "status:open",
		Username:     r.opts.Username,
		Password:     password,
		EmitExisting: true,
		MinEvents:    1,
		Timeout:      r.opts.Timeout,
		Stdout:       r.opts.Stdout,
	})

	if err != nil {
		return SmokeResult{}, err
	}

	if err := validateSCMSmokeEvent(scmSmoke, revision, fetchRef); err != nil {
		return SmokeResult{}, err
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

	jobMessage := r.opts.Message + " via Vectis job"
	if err := r.executeReviewJob(ctx, workspaceRoot, change, revision, fetchRef, password, jobMessage); err != nil {
		return SmokeResult{}, err
	}

	if err := r.verifyReviewMessage(ctx, password, change, jobMessage); err != nil {
		return SmokeResult{}, err
	}

	if err := r.runPollerBackstop(ctx, workspaceRoot, password, change, revision, fetchRef); err != nil {
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
		SCMProviderPolled:   true,
		SCMEventEmitted:     true,
		CheckoutVerified:    true,
		ReviewPosted:        true,
		WrongPasswordDenied: true,
		JobExecuted:         true,
		JobReviewPosted:     true,
		PollerBackstop:      true,
	}, nil
}

func validateSCMSmokeEvent(result scmgerrit.SmokeResult, revision, fetchRef string) error {
	for _, event := range result.Events {
		var payload struct {
			CurrentRevision string `json:"current_revision"`
			Ref             string `json:"ref"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return fmt.Errorf("decode gerrit scm smoke payload for %s: %w", event.Key, err)
		}

		if payload.CurrentRevision == revision && payload.Ref == fetchRef {
			return nil
		}
	}

	return fmt.Errorf("gerrit scm smoke did not emit revision/ref %s/%s", revision, fetchRef)
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

func (r smokeRunner) executeReviewJob(ctx context.Context, workspaceRoot, change, revision, fetchRef, password, message string) error {
	workspace := filepath.Join(workspaceRoot, "job")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		return fmt.Errorf("create gerrit job workspace: %w", err)
	}

	actionResolver, err := gerritActionResolver()
	if err != nil {
		return err
	}

	jobID := "gerrit-smoke-review-job"
	runID := fmt.Sprintf("gerrit-smoke-review-job-%d", time.Now().UTC().UnixNano())
	testJob := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   strPtr("root"),
			Uses: strPtr("builtins/sequence"),
			Steps: []*api.Node{
				{
					Id:   strPtr("checkout"),
					Uses: strPtr("builtins/checkout"),
					With: map[string]string{
						"url": r.opts.URL + "/" + r.opts.Project,
						"ref": fetchRef,
					},
				},
				{
					Id:   strPtr("review"),
					Uses: strPtr("gerrit/review@v1"),
					With: map[string]string{
						"url":           r.opts.URL,
						"change":        change,
						"revision":      revision,
						"message":       message,
						"label":         r.opts.Label,
						"value":         r.opts.Value,
						"username":      r.opts.Username,
						"password_file": ".vectis/secrets/gerrit/http-password",
					},
				},
			},
		},
	}

	logClient := &smokeLogClient{}
	executor := jobexec.NewExecutor()
	err = executor.ExecuteJobInWorkspaceWithOptions(ctx, testJob, logClient, interfaces.NewLogger("gerrit-smoke-job"), workspace, jobexec.ExecuteOptions{
		ActionResolver: actionResolver,
		SecretFiles: []secrets.FileMaterial{{
			ID:   "gerrit-http-password",
			Path: "gerrit/http-password",
			Data: []byte(password),
			Mode: 0o600,
		}},
	})

	if err != nil {
		return fmt.Errorf("execute gerrit review job: %w: logs=%s", err, logClient.String())
	}

	if !logClient.Contains("Gerrit review posted successfully") {
		return fmt.Errorf("gerrit review job logs did not confirm review post: logs=%s", logClient.String())
	}

	return nil
}

func (r smokeRunner) verifyReviewMessage(ctx context.Context, password, change, message string) error {
	endpoint := r.opts.URL + "/a/changes/" + url.PathEscape(change) + "/messages"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(r.opts.Username, password)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("list gerrit change messages: %w", err)
	}
	defer resp.Body.Close()

	var messages []struct {
		Message string `json:"message"`
	}

	if err := gerritaction.DecodeJSON(resp, &messages); err != nil {
		return fmt.Errorf("list gerrit change messages: %w", err)
	}

	for _, msg := range messages {
		if strings.Contains(msg.Message, message) {
			return nil
		}
	}

	return fmt.Errorf("gerrit review message %q was not found", message)
}

func (r smokeRunner) runPollerBackstop(ctx context.Context, workspaceRoot, password, change, revision, fetchRef string) error {
	restoreDriver, err := forceSQLiteDatabaseDriver()
	if err != nil {
		return err
	}
	defer restoreDriver()

	dbPath := filepath.Join(workspaceRoot, "poller-backstop.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("open gerrit poller backstop db: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		return fmt.Errorf("migrate gerrit poller backstop db: %w", err)
	}

	repos := dal.NewSQLRepositories(db)
	ns, err := repos.Namespaces().Create(ctx, "gerrit-smoke", nil)
	if err != nil {
		return fmt.Errorf("create gerrit poller smoke namespace: %w", err)
	}

	jobID := "gerrit-poller-backstop"
	definition := `{"id":"gerrit-poller-backstop","root":{"uses":"builtins/shell","with":{"command":"echo gerrit poller backstop"}}}`
	if err := repos.Jobs().CreateWithTriggers(ctx, jobID, definition, ns.ID, []dal.JobTriggerConfig{{
		ID:   "gerrit",
		Name: "Gerrit poller backstop",
		SCMPoll: &dal.JobSCMPollTriggerConfig{
			Provider: "gerrit",
			BaseURL:  r.opts.URL,
			Project:  r.opts.Project,
			Branch:   "master",
			Query:    "status:open",
			Interval: time.Minute,
		},
	}}); err != nil {
		return fmt.Errorf("create gerrit poller smoke job: %w", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE scm_poll_trigger_specs
		SET cursor = ?, next_poll_at = ?
	`, gerritEmptyBootstrappedCursor, time.Now().UTC().Add(-time.Second).Format(time.RFC3339)); err != nil {
		return fmt.Errorf("prime gerrit poller smoke cursor: %w", err)
	}

	ingress := &pollerSmokeIngress{}
	provider := scmgerrit.NewProvider(
		scmgerrit.WithHTTPClient(r.client),
		scmgerrit.WithBasicAuth(r.opts.Username, password),
	)

	service := scmpoller.NewService(interfaces.NewLogger("gerrit-smoke-poller"), db)
	service.SetInstanceID("gerrit-smoke-poller")
	service.SetExecutionIngress(ingress)
	service.RegisterProvider("gerrit", provider)

	if err := service.Process(ctx); err != nil {
		return fmt.Errorf("run gerrit poller backstop: %w", err)
	}

	submissions := ingress.Submissions()
	if len(submissions) != 1 {
		return fmt.Errorf("gerrit poller backstop dispatched %d submissions, want 1", len(submissions))
	}

	runID := submissions[0].Envelope.RunID
	if strings.TrimSpace(runID) == "" {
		return fmt.Errorf("gerrit poller backstop dispatched submission without run id")
	}

	if err := r.verifyPollerBackstopRows(ctx, db, runID, change, revision, fetchRef); err != nil {
		return err
	}

	return nil
}

func (r smokeRunner) verifyPollerBackstopRows(ctx context.Context, db *sql.DB, runID, change, revision, fetchRef string) error {
	var eventKey, payloadJSON, eventRunID, firstSource, firstInstance, lastSource, lastInstance string
	var observationCount int
	if err := db.QueryRowContext(ctx, `
		SELECT event_key, payload_json, COALESCE(run_id, ''), first_observed_source, first_observed_source_instance,
			last_observed_source, last_observed_source_instance, observation_count
		FROM scm_trigger_events
		ORDER BY discovered_at ASC
		LIMIT 1
	`).Scan(&eventKey, &payloadJSON, &eventRunID, &firstSource, &firstInstance, &lastSource, &lastInstance, &observationCount); err != nil {
		return fmt.Errorf("read gerrit poller backstop event: %w", err)
	}

	if eventRunID != runID {
		return fmt.Errorf("gerrit poller backstop event run_id = %q, want %q", eventRunID, runID)
	}

	if firstSource != dal.DispatchSourceSCMPoller || lastSource != dal.DispatchSourceSCMPoller ||
		firstInstance != "gerrit-smoke-poller" || lastInstance != "gerrit-smoke-poller" || observationCount != 1 {
		return fmt.Errorf("gerrit poller backstop observation metadata first=%s/%s last=%s/%s count=%d", firstSource, firstInstance, lastSource, lastInstance, observationCount)
	}

	if !strings.Contains(eventKey, revision) {
		return fmt.Errorf("gerrit poller backstop event key %q did not include revision %s", eventKey, revision)
	}

	if !strings.Contains(eventKey, change) {
		return fmt.Errorf("gerrit poller backstop event key %q did not include change identity %s", eventKey, change)
	}

	var payload struct {
		CurrentRevision string `json:"current_revision"`
		Ref             string `json:"ref"`
	}

	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return fmt.Errorf("decode gerrit poller backstop payload: %w", err)
	}

	if payload.CurrentRevision != revision || payload.Ref != fetchRef {
		return fmt.Errorf("gerrit poller backstop payload revision/ref %s/%s, want %s/%s", payload.CurrentRevision, payload.Ref, revision, fetchRef)
	}

	repos := dal.NewSQLRepositories(db)
	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("get gerrit poller backstop run: %w", err)
	}

	if run.TriggerSourceInstance == nil || *run.TriggerSourceInstance != "gerrit-smoke-poller" {
		return fmt.Errorf("gerrit poller backstop run source instance = %+v, want gerrit-smoke-poller", run.TriggerSourceInstance)
	}

	dispatches, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		return fmt.Errorf("list gerrit poller backstop dispatch events: %w", err)
	}

	if len(dispatches) != 2 || dispatches[0].EventType != dal.DispatchEventAttempt || dispatches[1].EventType != dal.DispatchEventSuccess {
		return fmt.Errorf("gerrit poller backstop dispatch events = %+v, want attempt/success", dispatches)
	}

	for _, dispatch := range dispatches {
		if dispatch.Source != dal.DispatchSourceSCMPoller || dispatch.SourceInstance != "gerrit-smoke-poller" {
			return fmt.Errorf("gerrit poller backstop dispatch source = %+v", dispatch)
		}
	}

	return nil
}

func gerritReviewDescriptor() (actionregistry.Descriptor, error) {
	source, err := gerritActionResolver()
	if err != nil {
		return actionregistry.Descriptor{}, err
	}

	return source.ResolveDescriptor("gerrit/review@v1")
}

func gerritActionResolver() (actionregistry.Resolver, error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}

	source, err := actionregistry.NewLocalManifestSource(filepath.Join(root, "extensions", "actions"))
	if err != nil {
		return nil, err
	}

	return source, nil
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

type smokeLogClient struct {
	mu     sync.Mutex
	chunks []string
}

func (c *smokeLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &smokeLogStream{client: c}, nil
}

func (c *smokeLogClient) Close() error { return nil }

func (c *smokeLogClient) add(chunk *api.LogChunk) {
	if chunk == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.chunks = append(c.chunks, string(chunk.GetData()))
}

func (c *smokeLogClient) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.chunks, "")
}

func (c *smokeLogClient) Contains(value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, chunk := range c.chunks {
		if strings.Contains(chunk, value) {
			return true
		}
	}

	return false
}

type smokeLogStream struct {
	client *smokeLogClient
}

func (s *smokeLogStream) Send(chunk *api.LogChunk) error {
	if s != nil && s.client != nil {
		s.client.add(chunk)
	}

	return nil
}

func (s *smokeLogStream) CloseSend() error { return nil }

type pollerSmokeIngress struct {
	mu          sync.Mutex
	submissions []cell.ExecutionSubmission
}

func (i *pollerSmokeIngress) SubmitExecution(_ context.Context, submission cell.ExecutionSubmission) error {
	if err := submission.Validate(); err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()
	i.submissions = append(i.submissions, submission)
	return nil
}

func (i *pollerSmokeIngress) Submissions() []cell.ExecutionSubmission {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]cell.ExecutionSubmission, len(i.submissions))
	copy(out, i.submissions)
	return out
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

func strPtr(value string) *string {
	return &value
}

const gerritEmptyBootstrappedCursor = `{"version":1,"changes":{}}`

func forceSQLiteDatabaseDriver() (func(), error) {
	previous, hadPrevious := os.LookupEnv(database.EnvDatabaseDriver)
	if err := os.Setenv(database.EnvDatabaseDriver, "sqlite3"); err != nil {
		return nil, fmt.Errorf("set %s for gerrit poller smoke: %w", database.EnvDatabaseDriver, err)
	}

	return func() {
		if hadPrevious {
			_ = os.Setenv(database.EnvDatabaseDriver, previous)
			return
		}
		_ = os.Unsetenv(database.EnvDatabaseDriver)
	}, nil
}
