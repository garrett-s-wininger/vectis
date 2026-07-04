//go:build e2e

package local_test

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
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vectis/internal/database"
)

const (
	envEnvelopeEnabled              = "VECTIS_E2E_ENVELOPE"
	envEnvelopeRuns                 = "VECTIS_E2E_ENVELOPE_RUNS"
	envEnvelopeTriggerClients       = "VECTIS_E2E_ENVELOPE_TRIGGER_CLIENTS"
	envEnvelopeStatusReaders        = "VECTIS_E2E_ENVELOPE_STATUS_READERS"
	envEnvelopeStatusReadsPerRun    = "VECTIS_E2E_ENVELOPE_STATUS_READS_PER_RUN"
	envEnvelopeLogReplayRuns        = "VECTIS_E2E_ENVELOPE_LOG_REPLAY_RUNS"
	envEnvelopeArtifactDownloadRuns = "VECTIS_E2E_ENVELOPE_ARTIFACT_DOWNLOAD_RUNS"
	envEnvelopeMinRunsPerSecond     = "VECTIS_E2E_ENVELOPE_MIN_RUNS_PER_SECOND"

	localEnvelopeBaseURL = "http://localhost:8080"
)

type localEnvelopeConfig struct {
	runs                 int
	triggerClients       int
	statusReaders        int
	statusReadsPerRun    int
	logReplayRuns        int
	artifactDownloadRuns int
	minRunsPerSecond     float64
}

type localEnvelopeRunInfo struct {
	runID        string
	runIndex     int
	triggerStart time.Time
	acceptedAt   time.Time
}

type localEnvelopeHTTPResult struct {
	latency time.Duration
	err     error
}

type localEnvelopeTerminalResult struct {
	info     localEnvelopeRunInfo
	latency  time.Duration
	final    runShowResult
	statuses []string
	err      error
}

func TestE2ELocalEnvelope(t *testing.T) {
	if !truthyEnv(envEnvelopeEnabled) {
		t.Skipf("set %s=true to run the local deployment envelope test", envEnvelopeEnabled)
	}
	if runtime.GOOS == "windows" {
		t.Skip("vectis-local e2e process cleanup uses Unix signals")
	}

	cfg := localEnvelopeConfigFromEnv(t)
	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")

	requireExecutable(t, cli, "vectis-cli")
	requireExecutable(t, local, "vectis-local")
	requireLocalPortsAvailable(t, localSmokePorts)

	dataHome := t.TempDir()
	env := commandEnv(map[string]string{
		"PATH":            filepath.Join(root, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"),
		"XDG_DATA_HOME":   dataHome,
		"XDG_RUNTIME_DIR": shortTempDir(t, "vectis-envelope-runtime-*"),
		"VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES":  "",
		"VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES":     "",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS":         filepath.Join(root, "examples", "actions"),
		"VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS": "false",
		"VECTIS_API_SERVER_HOST":                     "localhost",
		"VECTIS_API_SERVER_PORT":                     "8080",
		"VECTIS_API_TOKEN":                           "",
		"VECTIS_GRPC_TLS_INSECURE":                   "false",
		"VECTIS_LOCAL_GRPC_INSECURE":                 "false",
		"VECTIS_LOCAL_HTTP_TLS":                      "off",
		"VECTIS_LOCAL_DOCS_ENABLED":                  "false",
		database.EnvDatabaseDSN:                      "",
		database.EnvGlobalDatabaseDSN:                "",
		database.EnvCellDatabaseDSN:                  "",
		"VECTIS_WORKER_EXECUTION_BACKEND":            "host",
	})

	proc := startVectisLocal(t, root, env, local)
	if !truthyEnv("VECTIS_E2E_KEEP_LOCAL") {
		t.Cleanup(func() { stopVectisLocal(t, proc) })
	}

	waitForAPIReady(t, proc, localEnvelopeBaseURL+"/health/ready", 2*time.Minute)
	seedLocalSmokeSecret(t, root, env, cli, dataHome)
	createStoredExampleJob(t, root, env, cli, filepath.Join(root, "examples", "e2e-canonical.json"), canonicalJobID)

	client := &http.Client{Timeout: 30 * time.Second}
	statusJobs := make(chan string, cfg.runs*cfg.statusReadsPerRun)
	statusResults := make(chan localEnvelopeHTTPResult, cfg.runs*cfg.statusReadsPerRun)
	waitStatusReaders := startLocalEnvelopeStatusReaders(client, statusJobs, statusResults, cfg.statusReaders)

	workStart := time.Now()
	triggered, err := triggerLocalEnvelopeBurst(t, client, cfg, func(info localEnvelopeRunInfo) {
		for i := 0; i < cfg.statusReadsPerRun; i++ {
			statusJobs <- info.runID
		}
	})

	if err != nil {
		t.Fatalf("trigger local envelope burst: %v\nvectis-local stderr:\n%s", err, proc.stderr.String())
	}

	triggerDone := time.Now()
	close(statusJobs)

	terminalResults := waitForLocalEnvelopeTerminalRuns(t, client, triggered)
	terminalDone := time.Now()
	waitStatusReaders()
	close(statusResults)
	statusLatencies := collectLocalEnvelopeHTTPResults(t, statusResults)

	logLatencies := replayLocalEnvelopeLogs(t, client, triggered, cfg.logReplayRuns)
	artifactLatencies := downloadLocalEnvelopeArtifacts(t, client, triggered, cfg.artifactDownloadRuns)
	workDone := time.Now()

	runLatencies := make([]time.Duration, 0, len(terminalResults))
	for _, result := range terminalResults {
		if result.err != nil {
			t.Fatalf("run %s did not complete successfully: %v statuses=%v final=%+v\nvectis-local stderr:\n%s", result.info.runID, result.err, result.statuses, result.final, proc.stderr.String())
		}

		runLatencies = append(runLatencies, result.latency)
	}

	totalDuration := terminalDone.Sub(workStart)
	runsPerSecond := float64(len(triggered)) / totalDuration.Seconds()
	if cfg.minRunsPerSecond > 0 && runsPerSecond < cfg.minRunsPerSecond {
		t.Fatalf("local envelope throughput %.2f runs/s below %s=%.2f", runsPerSecond, envEnvelopeMinRunsPerSecond, cfg.minRunsPerSecond)
	}

	t.Logf("local envelope: runs=%d trigger_clients=%d status_reads/run=%d status_readers=%d", cfg.runs, cfg.triggerClients, cfg.statusReadsPerRun, cfg.statusReaders)
	t.Logf("local envelope: trigger_burst=%s terminal_duration=%s total_duration=%s runs/s=%.2f accepted/s=%.2f",
		triggerDone.Sub(workStart).Round(time.Millisecond),
		totalDuration.Round(time.Millisecond),
		workDone.Sub(workStart).Round(time.Millisecond),
		runsPerSecond,
		float64(len(triggered))/triggerDone.Sub(workStart).Seconds())

	t.Logf("local envelope: run latency p50=%s p95=%s p99=%s", localEnvelopeQuantile(runLatencies, 0.50), localEnvelopeQuantile(runLatencies, 0.95), localEnvelopeQuantile(runLatencies, 0.99))
	t.Logf("local envelope: status reads=%d p50=%s p95=%s p99=%s", len(statusLatencies), localEnvelopeQuantile(statusLatencies, 0.50), localEnvelopeQuantile(statusLatencies, 0.95), localEnvelopeQuantile(statusLatencies, 0.99))
	t.Logf("local envelope: log replays=%d p50=%s p95=%s p99=%s", len(logLatencies), localEnvelopeQuantile(logLatencies, 0.50), localEnvelopeQuantile(logLatencies, 0.95), localEnvelopeQuantile(logLatencies, 0.99))
	t.Logf("local envelope: artifact downloads=%d p50=%s p95=%s p99=%s", len(artifactLatencies), localEnvelopeQuantile(artifactLatencies, 0.50), localEnvelopeQuantile(artifactLatencies, 0.95), localEnvelopeQuantile(artifactLatencies, 0.99))
}

func localEnvelopeConfigFromEnv(t *testing.T) localEnvelopeConfig {
	t.Helper()

	return localEnvelopeConfig{
		runs:                 localEnvelopeEnvInt(t, envEnvelopeRuns, 20),
		triggerClients:       localEnvelopeEnvInt(t, envEnvelopeTriggerClients, 4),
		statusReaders:        localEnvelopeEnvInt(t, envEnvelopeStatusReaders, 4),
		statusReadsPerRun:    localEnvelopeEnvInt(t, envEnvelopeStatusReadsPerRun, 2),
		logReplayRuns:        localEnvelopeEnvInt(t, envEnvelopeLogReplayRuns, 5),
		artifactDownloadRuns: localEnvelopeEnvInt(t, envEnvelopeArtifactDownloadRuns, 5),
		minRunsPerSecond:     localEnvelopeEnvFloat(t, envEnvelopeMinRunsPerSecond, 0),
	}
}

func localEnvelopeEnvInt(t *testing.T, key string, defaultValue int) int {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		t.Fatalf("%s must be a non-negative integer, got %q", key, raw)
	}

	return value
}

func localEnvelopeEnvFloat(t *testing.T, key string, defaultValue float64) float64 {
	t.Helper()

	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 0 {
		t.Fatalf("%s must be a non-negative number, got %q", key, raw)
	}

	return value
}

func triggerLocalEnvelopeBurst(t *testing.T, client *http.Client, cfg localEnvelopeConfig, onInfo func(localEnvelopeRunInfo)) ([]localEnvelopeRunInfo, error) {
	t.Helper()

	clients := cfg.triggerClients
	if clients <= 0 {
		clients = 1
	}

	infos := make([]localEnvelopeRunInfo, cfg.runs)
	workCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		if err == nil {
			return
		}

		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	var wg sync.WaitGroup
	for clientID := 0; clientID < clients; clientID++ {
		clientID := clientID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := clientID; i < cfg.runs; i += clients {
				if workCtx.Err() != nil {
					return
				}

				info, err := triggerLocalEnvelopeJob(workCtx, client, fmt.Sprintf("envelope-%d-%d", time.Now().UnixNano(), i))
				if err != nil {
					setErr(err)
					return
				}

				infos[i] = info
				if onInfo != nil {
					onInfo(info)
				}
			}
		}()
	}
	wg.Wait()

	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func triggerLocalEnvelopeJob(ctx context.Context, client *http.Client, idempotencyKey string) (localEnvelopeRunInfo, error) {
	triggerStart := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, localEnvelopeBaseURL+"/api/v1/jobs/trigger/"+url.PathEscape(canonicalJobID), nil)
	if err != nil {
		return localEnvelopeRunInfo{}, err
	}

	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := client.Do(req)
	acceptedAt := time.Now()
	if err != nil {
		return localEnvelopeRunInfo{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return localEnvelopeRunInfo{}, err
	}

	if resp.StatusCode != http.StatusAccepted {
		return localEnvelopeRunInfo{}, fmt.Errorf("trigger status=%d body=%s", resp.StatusCode, string(body))
	}

	var out jobRunResult
	if err := json.Unmarshal(body, &out); err != nil {
		return localEnvelopeRunInfo{}, fmt.Errorf("parse trigger response: %w", err)
	}

	if strings.TrimSpace(out.RunID) == "" {
		return localEnvelopeRunInfo{}, fmt.Errorf("trigger response missing run_id: %s", string(body))
	}

	return localEnvelopeRunInfo{
		runID:        out.RunID,
		runIndex:     out.RunIndex,
		triggerStart: triggerStart,
		acceptedAt:   acceptedAt,
	}, nil
}

func waitForLocalEnvelopeTerminalRuns(t *testing.T, client *http.Client, infos []localEnvelopeRunInfo) []localEnvelopeTerminalResult {
	t.Helper()

	results := make([]localEnvelopeTerminalResult, len(infos))
	var wg sync.WaitGroup
	for i, info := range infos {
		i, info := i, info
		wg.Add(1)
		go func() {
			defer wg.Done()
			final, statuses, err := waitForLocalEnvelopeRunTerminal(client, info.runID, 5*time.Minute)
			results[i] = localEnvelopeTerminalResult{
				info:     info,
				latency:  time.Since(info.triggerStart),
				final:    final,
				statuses: statuses,
				err:      err,
			}
		}()
	}
	wg.Wait()

	return results
}

func waitForLocalEnvelopeRunTerminal(client *http.Client, runID string, timeout time.Duration) (runShowResult, []string, error) {
	deadline := time.Now().Add(timeout)
	statuses := make([]string, 0, 4)
	var last runShowResult
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := getLocalEnvelopeRun(client, runID)
		if err == nil {
			last = out
			status := strings.TrimSpace(strings.ToLower(out.Status))
			if status != "" && (len(statuses) == 0 || statuses[len(statuses)-1] != status) {
				statuses = append(statuses, status)
			}

			if runStatusTerminal(status) {
				if status != "succeeded" {
					return out, statuses, fmt.Errorf("run reached terminal status %q", status)
				}

				if !taskCompletionSucceeded(out.TaskCompletion) {
					time.Sleep(200 * time.Millisecond)
					continue
				}

				return out, statuses, nil
			}
		} else {
			lastErr = err
		}

		time.Sleep(200 * time.Millisecond)
	}

	return last, statuses, fmt.Errorf("run did not become terminal within %s; lastErr=%v", timeout, lastErr)
}

func getLocalEnvelopeRun(client *http.Client, runID string) (runShowResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localEnvelopeBaseURL+"/api/v1/runs/"+url.PathEscape(runID), nil)
	if err != nil {
		return runShowResult{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return runShowResult{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return runShowResult{}, err
	}

	if resp.StatusCode != http.StatusOK {
		return runShowResult{}, fmt.Errorf("runs show status=%d body=%s", resp.StatusCode, string(body))
	}

	var out runShowResult
	if err := json.Unmarshal(body, &out); err != nil {
		return runShowResult{}, fmt.Errorf("parse runs show response: %w", err)
	}

	return out, nil
}

func startLocalEnvelopeStatusReaders(client *http.Client, jobs <-chan string, results chan<- localEnvelopeHTTPResult, readers int) func() {
	if readers <= 0 {
		readers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runID := range jobs {
				started := time.Now()
				_, err := getLocalEnvelopeRun(client, runID)
				results <- localEnvelopeHTTPResult{latency: time.Since(started), err: err}
			}
		}()
	}

	return wg.Wait
}

func collectLocalEnvelopeHTTPResults(t *testing.T, ch <-chan localEnvelopeHTTPResult) []time.Duration {
	t.Helper()

	var latencies []time.Duration
	for result := range ch {
		if result.err != nil {
			t.Fatalf("local envelope HTTP read failed: %v", result.err)
		}

		latencies = append(latencies, result.latency)
	}

	return latencies
}

func replayLocalEnvelopeLogs(t *testing.T, client *http.Client, runs []localEnvelopeRunInfo, limit int) []time.Duration {
	t.Helper()

	limit = min(limit, len(runs))
	latencies := make([]time.Duration, 0, limit)
	for i := 0; i < limit; i++ {
		started := time.Now()
		body := getLocalEnvelopeRunLogs(t, client, runs[i].runID)
		if !bytes.Contains(body, []byte("canonical-control-start")) {
			t.Fatalf("run %s replayed logs missing canonical-control-start", runs[i].runID)
		}

		latencies = append(latencies, time.Since(started))
	}

	return latencies
}

func getLocalEnvelopeRunLogs(t *testing.T, client *http.Client, runID string) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localEnvelopeBaseURL+"/api/v1/runs/"+url.PathEscape(runID)+"/logs?tail=200&replay_limit=200", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Accept", "text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("replay logs for run %s: %v", runID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("replay logs for run %s status=%d body=%s", runID, resp.StatusCode, string(body))
	}

	var body bytes.Buffer
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		body.Write(line)
		body.WriteByte('\n')
		if bytes.Contains(body.Bytes(), []byte("canonical-control-start")) {
			return body.Bytes()
		}
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("read replayed logs for run %s: %v", runID, err)
	}

	return body.Bytes()
}

func downloadLocalEnvelopeArtifacts(t *testing.T, client *http.Client, runs []localEnvelopeRunInfo, limit int) []time.Duration {
	t.Helper()

	limit = min(limit, len(runs))
	latencies := make([]time.Duration, 0, limit*2)
	for i := 0; i < limit; i++ {
		for _, artifact := range []struct {
			name string
			want string
		}{
			{name: smokeArtifactName, want: smokeArtifactContent},
			{name: smokeRetryArtifactName, want: smokeRetryArtifactContent},
		} {
			started := time.Now()
			body := downloadLocalEnvelopeArtifact(t, client, runs[i].runID, artifact.name)
			if string(body) != artifact.want {
				t.Fatalf("run %s artifact %s content=%q want=%q", runs[i].runID, artifact.name, string(body), artifact.want)
			}

			latencies = append(latencies, time.Since(started))
		}
	}

	return latencies
}

func downloadLocalEnvelopeArtifact(t *testing.T, client *http.Client, runID, name string) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, localEnvelopeBaseURL+"/api/v1/runs/"+url.PathEscape(runID)+"/artifacts/"+url.PathEscape(name)+"/download", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("download artifact %s for run %s: %v", name, runID, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read artifact %s for run %s: %v", name, runID, err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download artifact %s for run %s status=%d body=%s", name, runID, resp.StatusCode, string(body))
	}

	return body
}

func localEnvelopeQuantile(values []time.Duration, q float64) time.Duration {
	if len(values) == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if len(sorted) == 1 {
		return sorted[0].Round(time.Millisecond)
	}

	pos := q * float64(len(sorted)-1)
	idx := int(pos)
	if idx >= len(sorted)-1 {
		return sorted[len(sorted)-1].Round(time.Millisecond)
	}

	frac := pos - float64(idx)
	value := time.Duration(float64(sorted[idx]) + frac*float64(sorted[idx+1]-sorted[idx]))
	return value.Round(time.Millisecond)
}
