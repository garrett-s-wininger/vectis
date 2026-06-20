package kubernetes

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
)

const (
	DefaultSmokeContext  = "kind-vectis"
	DefaultSmokeJobPath  = "examples/e2e-kubernetes.json"
	DefaultSmokeAPIPort  = 18080
	DefaultSmokeWait     = 180 * time.Second
	smokeAPIServiceName  = "service/vectis-api"
	smokeAPIRemotePort   = 8080
	smokeHTTPClientDelay = 30 * time.Second
)

type SmokeOptions struct {
	Kubectl      string
	Context      string
	Namespace    string
	JobPath      string
	APILocalPort int
	Wait         time.Duration
	APIToken     string
	Stdout       io.Writer
}

type SmokeResult struct {
	Status    string               `json:"status"`
	Context   string               `json:"context,omitempty"`
	Namespace string               `json:"namespace"`
	JobPath   string               `json:"job_path"`
	JobID     string               `json:"job_id,omitempty"`
	RunID     string               `json:"run_id"`
	RunStatus string               `json:"run_status"`
	Artifacts []SmokeArtifactCheck `json:"artifacts"`
}

type SmokeArtifactCheck struct {
	Name  string `json:"name"`
	Bytes int    `json:"bytes"`
}

type smokeJobRunResult struct {
	JobID    string `json:"job_id,omitempty"`
	ID       string `json:"id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	RunIndex int    `json:"run_index,omitempty"`
}

type smokeRunDetail struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

type smokeLogEntry struct {
	Stream int    `json:"stream"`
	Data   string `json:"data"`
}

type smokePortForward struct {
	cancel context.CancelFunc
	done   <-chan error
}

type smokeLineBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func RunSmoke(ctx context.Context, opts SmokeOptions) (SmokeResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	opts = normalizeSmokeOptions(opts)
	if err := validateSmokeOptions(opts); err != nil {
		return SmokeResult{}, err
	}

	out := opts.Stdout
	fmt.Fprintf(out, "Starting API port-forward on 127.0.0.1:%d\n", opts.APILocalPort)
	pf, err := startSmokeAPIPortForward(ctx, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	defer pf.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", opts.APILocalPort)
	client := &http.Client{Timeout: smokeHTTPClientDelay}

	fmt.Fprintf(out, "Submitting %s\n", opts.JobPath)
	run, err := submitSmokeJob(client, baseURL, opts)
	if err != nil {
		return SmokeResult{}, err
	}
	fmt.Fprintf(out, "Submitted job_id=%s run_id=%s\n", smokeJobID(run), run.RunID)

	runCtx, runCancel := context.WithTimeout(ctx, opts.Wait)
	detail, err := waitForSmokeRun(runCtx, client, baseURL, opts.APIToken, run.RunID, out)
	runCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	markers := []string{
		"canonical-control-start",
		"canonical-artifact-written",
		"canonical-fanout-ok",
		"canonical-registry-retry-succeeded",
	}
	fmt.Fprintln(out, "Verifying run logs")
	logCtx, logCancel := context.WithTimeout(ctx, opts.Wait)
	err = waitForSmokeLogMarkers(logCtx, baseURL, opts.APIToken, run.RunID, markers, out)
	logCancel()
	if err != nil {
		return SmokeResult{}, err
	}

	artifactChecks, err := verifySmokeArtifacts(client, baseURL, opts.APIToken, run.RunID)
	if err != nil {
		return SmokeResult{}, err
	}

	result := SmokeResult{
		Status:    "ok",
		Context:   strings.TrimSpace(opts.Context),
		Namespace: opts.Namespace,
		JobPath:   opts.JobPath,
		JobID:     smokeJobID(run),
		RunID:     run.RunID,
		RunStatus: detail.Status,
		Artifacts: artifactChecks,
	}

	fmt.Fprintf(out, "Kubernetes smoke succeeded: run_id=%s\n", run.RunID)
	return result, nil
}

func normalizeSmokeOptions(opts SmokeOptions) SmokeOptions {
	opts.Kubectl = strings.TrimSpace(opts.Kubectl)
	if opts.Kubectl == "" {
		opts.Kubectl = "kubectl"
	}
	opts.Context = strings.TrimSpace(opts.Context)
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}
	opts.JobPath = strings.TrimSpace(opts.JobPath)
	if opts.JobPath == "" {
		opts.JobPath = DefaultSmokeJobPath
	}
	if opts.APILocalPort == 0 {
		opts.APILocalPort = DefaultSmokeAPIPort
	}
	if opts.Wait == 0 {
		opts.Wait = DefaultSmokeWait
	}
	opts.APIToken = strings.TrimSpace(opts.APIToken)
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	return opts
}

func validateSmokeOptions(opts SmokeOptions) error {
	if opts.Kubectl == "" {
		return fmt.Errorf("kubectl command is required")
	}
	if opts.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if opts.JobPath == "" {
		return fmt.Errorf("job path is required")
	}
	if opts.APILocalPort <= 0 || opts.APILocalPort > 65535 {
		return fmt.Errorf("api local port must be between 1 and 65535")
	}
	if opts.Wait <= 0 {
		return fmt.Errorf("wait must be > 0")
	}
	return nil
}

func startSmokeAPIPortForward(ctx context.Context, opts SmokeOptions) (*smokePortForward, error) {
	pfCtx, cancel := context.WithCancel(ctx)
	args := []string{}
	if opts.Context != "" {
		args = append(args, "--context", opts.Context)
	}
	args = append(args,
		"-n", opts.Namespace,
		"port-forward",
		"--address", "127.0.0.1",
		smokeAPIServiceName,
		fmt.Sprintf("%d:%d", opts.APILocalPort, smokeAPIRemotePort),
	)

	cmd := exec.CommandContext(pfCtx, opts.Kubectl, args...) // #nosec G204 -- kubectl path and args are smoke-harness controlled.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}

	lines := make(chan string, 32)
	var output smokeLineBuffer
	scan := func(r io.Reader) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			output.add(line)
			select {
			case lines <- line:
			case <-pfCtx.Done():
				return
			default:
			}
		}
	}

	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	go scan(stdout)
	go scan(stderr)

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	readyCtx, readyCancel := context.WithTimeout(ctx, opts.Wait)
	defer readyCancel()

	for {
		select {
		case line := <-lines:
			fmt.Fprintln(opts.Stdout, line)
			if strings.Contains(line, "Forwarding from") {
				return &smokePortForward{cancel: cancel, done: done}, nil
			}
		case err := <-done:
			cancel()
			if err != nil {
				return nil, fmt.Errorf("API port-forward failed: %w: %s", err, strings.TrimSpace(output.String()))
			}
			return nil, fmt.Errorf("API port-forward exited before becoming ready: %s", strings.TrimSpace(output.String()))
		case <-readyCtx.Done():
			cancel()
			return nil, fmt.Errorf("timed out waiting for API port-forward: %s", strings.TrimSpace(output.String()))
		}
	}
}

func (p *smokePortForward) stop() {
	if p == nil || p.cancel == nil {
		return
	}
	p.cancel()
	if p.done == nil {
		return
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
	}
}

func (b *smokeLineBuffer) add(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.WriteString(line)
	b.buf.WriteByte('\n')
}

func (b *smokeLineBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func submitSmokeJob(client *http.Client, baseURL string, opts SmokeOptions) (smokeJobRunResult, error) {
	body, err := os.ReadFile(opts.JobPath)
	if err != nil {
		return smokeJobRunResult{}, fmt.Errorf("read smoke job %s: %w", opts.JobPath, err)
	}

	req, err := newSmokeAPIRequest(http.MethodPost, baseURL, "/api/v1/jobs/run", opts.APIToken, bytes.NewReader(body))
	if err != nil {
		return smokeJobRunResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return smokeJobRunResult{}, fmt.Errorf("submit smoke job: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return smokeJobRunResult{}, smokeUnexpectedStatus("submit smoke job", resp)
	}

	var result smokeJobRunResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return smokeJobRunResult{}, fmt.Errorf("parse smoke job response: %w", err)
	}
	if strings.TrimSpace(result.RunID) == "" {
		return smokeJobRunResult{}, fmt.Errorf("smoke job response missing run_id")
	}
	return result, nil
}

func waitForSmokeRun(ctx context.Context, client *http.Client, baseURL, token, runID string, out io.Writer) (smokeRunDetail, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		detail, err := fetchSmokeRunDetail(client, baseURL, token, runID)
		if err != nil {
			return smokeRunDetail{}, err
		}
		fmt.Fprintf(out, "run_id=%s status=%s\n", detail.RunID, detail.Status)

		switch strings.ToLower(strings.TrimSpace(detail.Status)) {
		case "succeeded":
			return detail, nil
		case "failed", "orphaned", "cancelled", "abandoned", "aborted":
			return detail, fmt.Errorf("run %s reached terminal status %s", runID, detail.Status)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return smokeRunDetail{}, fmt.Errorf("timed out waiting for run %s: %w", runID, ctx.Err())
		}
	}
}

func fetchSmokeRunDetail(client *http.Client, baseURL, token, runID string) (smokeRunDetail, error) {
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, fmt.Sprintf("/api/v1/runs/%s", url.PathEscape(runID)), token, nil)
	if err != nil {
		return smokeRunDetail{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return smokeRunDetail{}, fmt.Errorf("fetch run %s: %w", runID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeRunDetail{}, smokeUnexpectedStatus("fetch run "+runID, resp)
	}

	var detail smokeRunDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		return smokeRunDetail{}, fmt.Errorf("parse run %s: %w", runID, err)
	}
	return detail, nil
}

func waitForSmokeLogMarkers(ctx context.Context, baseURL, token, runID string, markers []string, out io.Writer) error {
	req, err := newSmokeAPIRequest(http.MethodGet, baseURL, fmt.Sprintf("/api/v1/runs/%s/logs", url.PathEscape(runID)), token, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req = req.WithContext(ctx)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("connect to run log stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return smokeUnexpectedStatus("connect to run log stream", resp)
	}

	seen := map[string]bool{}
	reader := bufio.NewReader(resp.Body)
	var dataBuf strings.Builder

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				return missingSmokeLogMarkers(markers, seen)
			}
			return err
		}

		line = strings.TrimRight(line, "\r\n")
		if line != "" {
			if after, ok := strings.CutPrefix(line, "data:"); ok {
				dataBuf.WriteString(strings.TrimSpace(after))
			}
			continue
		}

		if dataBuf.Len() == 0 {
			continue
		}

		message := []byte(dataBuf.String())
		dataBuf.Reset()

		var entry smokeLogEntry
		if err := json.Unmarshal(message, &entry); err != nil {
			continue
		}

		lineText := smokeLogEntryDisplayText(entry)
		if strings.TrimSpace(lineText) != "" {
			fmt.Fprintln(out, lineText)
		}
		for _, marker := range markers {
			if !strings.HasPrefix(lineText, "$ ") && strings.Contains(lineText, marker) {
				seen[marker] = true
			}
		}
		if len(seen) == len(markers) {
			return nil
		}
	}
}

func smokeLogEntryDisplayText(entry smokeLogEntry) string {
	if entry.Stream == int(api.Stream_STREAM_CONTROL.Number()) {
		var meta struct {
			Event  string `json:"event"`
			Status string `json:"status,omitempty"`
		}
		if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
			return ""
		}
		switch meta.Event {
		case "start":
			return "run started"
		case "completed":
			if meta.Status != "" {
				return "run completed: " + meta.Status
			}
			return "run completed"
		default:
			return ""
		}
	}

	if entry.Stream == int(api.Stream_STREAM_STDERR.Number()) {
		return "[stderr] " + entry.Data
	}
	return entry.Data
}

func missingSmokeLogMarkers(markers []string, seen map[string]bool) error {
	missing := make([]string, 0, len(markers))
	for _, marker := range markers {
		if !seen[marker] {
			missing = append(missing, marker)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return fmt.Errorf("run logs missing markers: %s", strings.Join(missing, ", "))
}

func verifySmokeArtifacts(client *http.Client, baseURL, token, runID string) ([]SmokeArtifactCheck, error) {
	checks := []struct {
		name     string
		contains []string
		exact    string
	}{
		{name: "e2e-smoke-report", exact: "canonical-artifact-ok\n"},
		{name: "e2e-registry-retry", contains: []string{"canonical-registry-retry-attempt=2", "canonical-registry-retry-succeeded"}},
	}

	out := make([]SmokeArtifactCheck, 0, len(checks))
	for _, check := range checks {
		body, err := downloadSmokeArtifact(client, baseURL, token, runID, check.name)
		if err != nil {
			return nil, err
		}
		text := string(body)
		if check.exact != "" && text != check.exact {
			return nil, fmt.Errorf("artifact %s content mismatch", check.name)
		}
		for _, want := range check.contains {
			if !strings.Contains(text, want) {
				return nil, fmt.Errorf("artifact %s missing %q", check.name, want)
			}
		}
		out = append(out, SmokeArtifactCheck{Name: check.name, Bytes: len(body)})
	}

	return out, nil
}

func downloadSmokeArtifact(client *http.Client, baseURL, token, runID, name string) ([]byte, error) {
	req, err := newSmokeAPIRequest(
		http.MethodGet,
		baseURL,
		fmt.Sprintf("/api/v1/runs/%s/artifacts/%s/download", url.PathEscape(runID), url.PathEscape(name)),
		token,
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download artifact %s: %w", name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, smokeUnexpectedStatus("download artifact "+name, resp)
	}

	return io.ReadAll(resp.Body)
}

func newSmokeAPIRequest(method, baseURL, path, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, strings.TrimRight(baseURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	if token = strings.TrimSpace(token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req, nil
}

func smokeUnexpectedStatus(action string, resp *http.Response) error {
	body, _ := io.ReadAll(resp.Body)
	if detail := strings.TrimSpace(string(body)); detail != "" {
		return fmt.Errorf("%s failed: %s: %s", action, resp.Status, detail)
	}
	return fmt.Errorf("%s failed: %s", action, resp.Status)
}

func smokeJobID(run smokeJobRunResult) string {
	if strings.TrimSpace(run.JobID) != "" {
		return strings.TrimSpace(run.JobID)
	}
	return strings.TrimSpace(run.ID)
}
