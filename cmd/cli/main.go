package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/term"

	api "vectis/api/gen/go"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"

	_ "vectis/internal/dbdrivers"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Stream    int    `json:"stream"`
	Sequence  int64  `json:"sequence"`
	Data      string `json:"data"`
}

func newAPIRequest(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, config.PublicAPIBaseURL()+path, body)
	if err != nil {
		return nil, err
	}

	if token := effectiveToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

var apiHTTPClient = &http.Client{Timeout: 30 * time.Second}

func doAPIRequest(req *http.Request) (*http.Response, error) {
	return apiHTTPClient.Do(req)
}

func cliTokenFilePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "vectis", "token"), nil
}

func readPersistedToken() string {
	path, err := cliTokenFilePath()
	if err != nil {
		return ""
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(b))
}

func writePersistedToken(token string) error {
	path, err := cliTokenFilePath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		return err
	}

	return os.Chmod(path, 0o600)
}

func deletePersistedToken() error {
	path, err := cliTokenFilePath()
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func effectiveToken() string {
	if token := config.CLIAPIToken(); token != "" {
		return token
	}

	return readPersistedToken()
}

func runLogStream(runID string, filterStdout, filterStderr bool) error {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s/logs", runID), nil)
	if err != nil {
		return fmt.Errorf("failed to create log stream request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)

	resp, err := doAPIRequest(req)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to connect to API for log stream: %w", err)
	}

	defer func() {
		cancel()
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusServiceUnavailable {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error string `json:"error"`
		}

		if json.Unmarshal(body, &apiErr) == nil && apiErr.Error == "log_service_unavailable" {
			return fmt.Errorf("log service temporarily unavailable; logs will be backfilled when service recovers")
		}

		return fmt.Errorf("log stream request failed: %s", resp.Status)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("log stream request failed: %s", resp.Status)
	}

	fmt.Printf("Connected to logs for run %s\n", runID)
	fmt.Println("Streaming logs... (press Ctrl+C to exit)")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	done := make(chan struct{})
	readErr := make(chan error, 1)

	go func() {
		defer close(done)

		reader := bufio.NewReader(resp.Body)
		var dataBuf strings.Builder

		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				if err == io.EOF {
					return
				}

				readErr <- err
				return
			}

			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if dataBuf.Len() == 0 {
					continue
				}

				message := []byte(dataBuf.String())
				dataBuf.Reset()

				var entry LogEntry
				if err := json.Unmarshal(message, &entry); err != nil {
					fmt.Fprintf(os.Stderr, "Error: failed to parse log entry: %v\n", err)
					continue
				}

				if entry.Stream == int(api.Stream_STREAM_CONTROL.Number()) {
					var meta struct {
						Event  string `json:"event"`
						Status string `json:"status,omitempty"`
					}

					if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
						continue
					}

					switch meta.Event {
					case "start":
						fmt.Printf("\n=== Run %s started ===\n", runID)
					case "completed":
						status := meta.Status
						switch status {
						case "success":
							fmt.Printf("Run %s finished successfully.\n", runID)
						case "failure":
							fmt.Printf("Run %s failed.\n", runID)
						default:
							fmt.Printf("Run %s finished (status: %s).\n", runID, status)
						}
					}

					if meta.Event == "completed" {
						return
					}

					continue
				}

				if filterStdout && entry.Stream != int(api.Stream_STREAM_STDOUT.Number()) {
					continue
				}

				if filterStderr && entry.Stream != int(api.Stream_STREAM_STDERR.Number()) {
					continue
				}

				streamPrefix := ""
				if entry.Stream == int(api.Stream_STREAM_STDERR.Number()) {
					streamPrefix = "[stderr] "
				}

				fmt.Printf("%s%s\n", streamPrefix, entry.Data)
				continue
			}

			if after, ok := strings.CutPrefix(line, "data:"); ok {
				data := strings.TrimSpace(after)
				dataBuf.WriteString(data)
			}
		}
	}()

	select {
	case <-done:
		select {
		case err := <-readErr:
			if err != nil {
				return fmt.Errorf("log stream read error: %w", err)
			}
		default:
		}
		return nil
	case <-interrupt:
		fmt.Println("\nDisconnecting...")
		cancel()
		<-done
		return fmt.Errorf("interrupted")
	}
}

func triggerJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/jobs/trigger/%s", jobID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create trigger request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to trigger job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			JobID    string `json:"job_id"`
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintf(os.Stderr, "Error: queue service unavailable\n")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func runContinuousLogs(jobID string, filterStdout, filterStderr bool) error {
	lastIndex := 0
	fmt.Printf("Streaming logs for job %s (Ctrl+C to stop)\n", jobID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	type runEvent struct {
		RunID    string `json:"run_id"`
		RunIndex int    `json:"run_index"`
	}

outer:
	for {
		attemptCtx, attemptCancel := context.WithCancel(context.Background())

		runChan := make(chan runEvent, 32)
		go func() {
			defer close(runChan)
			req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/sse/jobs/%s/runs", jobID), nil)
			if err != nil {
				return
			}
			req = req.WithContext(attemptCtx)
			req.Header.Set("Accept", "text/event-stream")

			resp, err := doAPIRequest(req)
			if err != nil {
				return
			}

			defer func() {
				attemptCancel()
				_ = resp.Body.Close()
			}()

			if resp.StatusCode != http.StatusOK {
				return
			}

			reader := bufio.NewReader(resp.Body)
			var dataBuf strings.Builder

			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					if attemptCtx.Err() != nil {
						return
					}
					if err == io.EOF {
						return
					}
					fmt.Fprintf(os.Stderr, "SSE error: %v\n", err)
					return
				}

				line = strings.TrimRight(line, "\r\n")
				if line == "" {
					if dataBuf.Len() == 0 {
						continue
					}

					message := []byte(dataBuf.String())
					dataBuf.Reset()

					var ev runEvent
					if err := json.Unmarshal(message, &ev); err != nil || ev.RunID == "" {
						continue
					}

					select {
					case runChan <- ev:
					default:
						fmt.Fprintf(os.Stderr, "Dropping run event (buffer full)\n")
					}
					continue
				}

				if after, ok := strings.CutPrefix(line, "data:"); ok {
					data := strings.TrimSpace(after)
					dataBuf.WriteString(data)
				}
			}
		}()

		req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s/runs?since=%d", jobID, lastIndex), nil)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("creating runs request: %w", err)
		}

		resp, err := doAPIRequest(req)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("fetching runs: %w", err)
		}
		var runs []struct {
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
			resp.Body.Close()
			attemptCancel()
			return fmt.Errorf("parsing runs: %w", err)
		}
		resp.Body.Close()
		for _, r := range runs {
			if r.RunIndex > lastIndex {
				lastIndex = r.RunIndex
			}
		}

		for {
			select {
			case <-interrupt:
				attemptCancel()
				fmt.Println("\nStopping.")
				return fmt.Errorf("interrupted")
			case ev, ok := <-runChan:
				if !ok {
					attemptCancel()
					fmt.Fprintf(os.Stderr, "Runs connection closed; reconnecting...\n")
					continue outer
				}

				if ev.RunIndex > lastIndex {
					lastIndex = ev.RunIndex
				}

				if err := runLogStream(ev.RunID, filterStdout, filterStderr); err != nil {
					if err.Error() == "interrupted" {
						attemptCancel()
						return err
					}
					fmt.Fprintf(os.Stderr, "Error streaming run %s: %v\n", ev.RunID, err)
				}
			}
		}
	}
}

func resolveLogIDArg(arg string) (string, error) {
	if arg != "-" {
		return arg, nil
	}

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("reading from stdin: %w", err)
	}

	id := strings.TrimSpace(line)
	if id == "" {
		return "", fmt.Errorf("empty id from stdin")
	}

	return id, nil
}

func runLogsRun(cmd *cobra.Command, args []string) {
	id, err := resolveLogIDArg(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	if err := runLogStream(id, filterStdout, filterStderr); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runLogsJob(cmd *cobra.Command, args []string) {
	jobID, err := resolveLogIDArg(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	if err := runContinuousLogs(jobID, filterStdout, filterStderr); err != nil && err.Error() != "interrupted" {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: path or - is required")
		cmd.Usage()
		os.Exit(1)
	}

	source := args[0]
	var body []byte
	var err error
	if source == "-" {
		body, err = io.ReadAll(os.Stdin)
	} else {
		body, err = os.ReadFile(source)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON: %v\n", err)
		os.Exit(1)
	}

	if job.GetRoot() == nil {
		fmt.Fprintln(os.Stderr, "Error: job must have a root node")
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to submit job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			ID    string `json:"id"`
			RunID string `json:"run_id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition")
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintln(os.Stderr, "Error: queue service unavailable")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func fetchJobDefinitionBody(jobID string) ([]byte, int, error) {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create job definition request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch job definition: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read job definition: %w", readErr)
	}

	return body, resp.StatusCode, nil
}

func formatJobDefinitionBody(body []byte, pretty bool) []byte {
	if !pretty {
		out := body
		if len(out) == 0 {
			return out
		}
		if !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}
		return out
	}

	var indented bytes.Buffer
	if err := json.Indent(&indented, body, "", "  "); err != nil {
		out := body
		if len(out) == 0 {
			return out
		}

		if !bytes.HasSuffix(out, []byte("\n")) {
			out = append(out, '\n')
		}

		return out
	}

	out := indented.Bytes()
	if len(out) == 0 {
		return out
	}

	if !bytes.HasSuffix(out, []byte("\n")) {
		out = append(out, '\n')
	}

	return out
}

func editJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	pretty := formatJobDefinitionBody(body, true)
	tempFile, err := os.CreateTemp("", "vectis-job-*.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create temp file: %v\n", err)
		os.Exit(1)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(pretty); err != nil {
		tempFile.Close()
		fmt.Fprintf(os.Stderr, "Error: failed to write job definition to temp file: %v\n", err)
		os.Exit(1)
	}

	if err := tempFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to close temp file: %v\n", err)
		os.Exit(1)
	}

	editorEnv := os.Getenv("EDITOR")
	if editorEnv == "" {
		editorEnv = "vi"
	}

	editorParts := strings.Fields(editorEnv)
	if len(editorParts) == 0 {
		fmt.Fprintln(os.Stderr, "Error: EDITOR is empty after parsing")
		os.Exit(1)
	}

	editorName := editorParts[0]
	editorArgs := append(append([]string{}, editorParts[1:]...), tempPath)

	editCmd := exec.Command(editorName, editorArgs...)
	editCmd.Stdin = os.Stdin
	editCmd.Stdout = os.Stdout
	editCmd.Stderr = os.Stderr

	if err := editCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 0 {
				os.Exit(exitErr.ExitCode())
			}
		}

		fmt.Fprintf(os.Stderr, "Error: editor failed: %v\n", err)
		os.Exit(1)
	}

	edited, err := os.ReadFile(tempPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read edited job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(edited, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON after edit: %v\n", err)
		os.Exit(1)
	}

	if job.GetRoot() == nil {
		fmt.Fprintln(os.Stderr, "Error: job must have a root node")
		os.Exit(1)
	}

	if job.Id == nil || *job.Id != jobID {
		fmt.Fprintf(os.Stderr, "Error: job id mismatch (expected %q, got %v)\n", jobID, job.Id)
		os.Exit(1)
	}

	// NOTE(garrett): Always re-indent the stored job before updating.
	pretty, err = json.MarshalIndent(&job, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to normalize job JSON: %v\n", err)
		os.Exit(1)
	}
	pretty = append(pretty, '\n')

	req, err := newAPIRequest(http.MethodPut, fmt.Sprintf("/api/v1/jobs/%s", jobID), bytes.NewReader(pretty))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create update request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	updateResp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to update job: %v\n", err)
		os.Exit(1)
	}
	defer updateResp.Body.Close()

	switch updateResp.StatusCode {
	case http.StatusNoContent:
		fmt.Println("Job updated successfully.")
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition or id mismatch")
		os.Exit(1)
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status updating job: %s\n", updateResp.Status)
		os.Exit(1)
	}
}

func getJobDefinition(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	raw, _ := cmd.Flags().GetBool("raw")
	out := formatJobDefinitionBody(body, !raw)
	fmt.Print(string(out))
}

func listJobs(cmd *cobra.Command, args []string) {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/jobs", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create list jobs request: %v\n", err)
		os.Exit(1)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to list jobs: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: unexpected status listing jobs: %s\n", resp.Status)
		os.Exit(1)
	}

	var jobs []struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&jobs); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to parse jobs response: %v\n", err)
		os.Exit(1)
	}

	names := make([]string, 0, len(jobs))
	for _, j := range jobs {
		if j.Name != "" {
			names = append(names, j.Name)
		}
	}

	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}
}

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a stored job",
	Long: `Trigger a stored job by its job-id. The job must exist in the database.
The API records the run and returns immediately (202 with run_id); enqueue to the queue happens in the background, so a down queue does not block this command.`,
	Args: cobra.ExactArgs(1),
	Run:  triggerJob,
}

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Stream logs for runs",
	Long:  `Stream logs via WebSocket. Use "logs run" for a single run (until it completes). Use "logs job" to follow a job (runs triggered after you connect). Use "-" as the id to read from stdin.`,
}

var logsRunCmd = &cobra.Command{
	Use:   "run [run-id]",
	Short: "Stream logs for a single run until it completes",
	Long:  `Connect to the log stream for the given run-id and stream output until the run completes (server closes). Argument is a run-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsRun,
}

var logsJobCmd = &cobra.Command{
	Use:   "job [job-id]",
	Short: "Stream logs for a job (next runs only)",
	Long:  `Subscribe to run events for the job and stream logs for each run triggered after you connect. Does not stream historical runs. Re-subscribes if the runs connection closes. Argument is a job-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsJob,
}

var runCmd = &cobra.Command{
	Use:   "run [path|-]",
	Short: "Submit an ephemeral job",
	Long: `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin.
On success prints the job ID (and run_id with --follow); the API returns after persisting the run, then enqueues in the background.`,
	Args: cobra.ExactArgs(1),
	Run:  runJob,
}

var editCmd = &cobra.Command{
	Use:   "edit [job-id]",
	Short: "Edit a stored job definition using $EDITOR",
	Long:  `Fetch a stored job definition, open it in your $EDITOR, and update the job if you save and exit successfully.`,
	Args:  cobra.ExactArgs(1),
	Run:   editJob,
}

var getCmd = &cobra.Command{
	Use:   "get [job-id]",
	Short: "Get a stored job definition",
	Long:  `Fetch a stored job definition by its job-id and print it as JSON.`,
	Args:  cobra.ExactArgs(1),
	Run:   getJobDefinition,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List stored job ids",
	Long:  `Fetch all stored jobs and print each job id on its own line.`,
	Args:  cobra.NoArgs,
	Run:   listJobs,
}

func runMigrate(cmd *cobra.Command, args []string) {
	dbPath := database.GetDBPath()
	fmt.Printf("Migrating database: %s\n", dbPath)
	if err := database.Migrate(dbPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Migrations applied.")
}

func forceFailRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	reason, _ := cmd.Flags().GetString("reason")

	body := []byte("{}")
	if reason != "" {
		payload, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to encode request body: %v\n", err)
			os.Exit(1)
		}
		body = payload
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-fail", runID), bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("Run %s force-failed.\n", runID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: run '%s' not found\n", runID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func forceRequeueRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-requeue", runID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create request: %v\n", err)
		os.Exit(1)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Printf("Run %s force-requeued.\n", runID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: run '%s' not found\n", runID)
		os.Exit(1)
	case http.StatusConflict:
		fmt.Fprintf(os.Stderr, "Error: run '%s' is already succeeded and cannot be requeued\n", runID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply database migrations (admin / one-shot)",
	Long: `Run embedded SQL migrations against the database selected by VECTIS_DATABASE_DRIVER and VECTIS_DATABASE_DSN (or defaults).

Runtime services only wait for the schema; they do not migrate. Use this command (or CI/deploy automation) before starting the stack.`,
	Args: cobra.NoArgs,
	Run:  runMigrate,
}

var forceFailCmd = &cobra.Command{
	Use:   "force-fail [run-id]",
	Short: "Manually mark a run as failed",
	Long:  `Force a run into failed status in the API/database. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceFailRun,
}

var forceRequeueCmd = &cobra.Command{
	Use:   "force-requeue [run-id]",
	Short: "Manually requeue a run",
	Long:  `Force a run back to queued status for manual retry/recovery. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceRequeueRun,
}

func runLogin(cmd *cobra.Command, args []string) {
	username, _ := cmd.Flags().GetString("username")
	var password string

	if username == "" {
		fmt.Fprint(os.Stderr, "Username: ")
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			username = scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to read username: %v\n", err)
			os.Exit(1)
		}
	}

	if password == "" {
		fmt.Fprint(os.Stderr, "Password: ")
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			// Fallback to plain scanner if not a terminal
			scanner := bufio.NewScanner(os.Stdin)
			if scanner.Scan() {
				password = scanner.Text()
			}
			if scanErr := scanner.Err(); scanErr != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to read password: %v\n", scanErr)
				os.Exit(1)
			}
		} else {
			password = string(b)
			fmt.Fprintln(os.Stderr)
		}
	}

	if username == "" || password == "" {
		fmt.Fprintln(os.Stderr, "Error: username and password are required")
		cmd.Usage()
		os.Exit(1)
	}

	token, err := doLogin(username, password)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if err := writePersistedToken(token); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to save token: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Logged in as %s.\n", username)
}

func doLogin(username, password string) (string, error) {
	body, err := json.Marshal(map[string]string{
		"username": username,
		"password": password,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, config.PublicAPIBaseURL()+"/api/v1/login", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := apiHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("login request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result struct {
			Token     string `json:"token"`
			UserID    int64  `json:"user_id"`
			ExpiresAt string `json:"expires_at"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return "", fmt.Errorf("failed to parse response: %w", err)
		}

		return result.Token, nil
	case http.StatusUnauthorized:
		return "", fmt.Errorf("invalid username or password")
	case http.StatusServiceUnavailable:
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("service unavailable: %s", string(body))
	default:
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with the Vectis API",
	Long:  `Log in to the Vectis API using username and password. The token is persisted to the config directory for subsequent commands.`,
	Run:   runLogin,
}

func runLogout(cmd *cobra.Command, args []string) {
	if err := deletePersistedToken(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to remove token: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Logged out. Token removed.")
}

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Remove the persisted API token",
	Long:  `Remove the locally persisted API token. This does not invalidate the token on the server.`,
	Run:   runLogout,
}

func runTokenList(cmd *cobra.Command, args []string) {
	if err := tokenList(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenList(w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/tokens", nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var tokens []struct {
		ID         int64   `json:"id"`
		Label      string  `json:"label"`
		ExpiresAt  *string `json:"expires_at,omitempty"`
		CreatedAt  string  `json:"created_at"`
		LastUsedAt *string `json:"last_used_at,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokens); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	for _, t := range tokens {
		exp := "never"
		if t.ExpiresAt != nil {
			exp = *t.ExpiresAt
		}
		used := "never"
		if t.LastUsedAt != nil {
			used = *t.LastUsedAt
		}
		fmt.Fprintf(w, "%d\t%s\texpires=%s\tcreated=%s\tlast_used=%s\n", t.ID, t.Label, exp, t.CreatedAt, used)
	}
	return nil
}

func runTokenCreate(cmd *cobra.Command, args []string) {
	label, _ := cmd.Flags().GetString("label")
	expiresIn, _ := cmd.Flags().GetString("expires-in")
	userID, _ := cmd.Flags().GetInt64("user-id")

	if label == "" {
		fmt.Fprintln(os.Stderr, "Error: --label is required")
		cmd.Usage()
		os.Exit(1)
	}

	if err := tokenCreate(label, expiresIn, userID, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenCreate(label, expiresIn string, userID int64, w io.Writer) error {
	reqBody := map[string]any{
		"label":      label,
		"expires_in": expiresIn,
	}

	if userID > 0 {
		reqBody["user_id"] = userID
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var result struct {
			Token     string `json:"token"`
			ID        int64  `json:"id"`
			Label     string `json:"label"`
			ExpiresAt string `json:"expires_at"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		fmt.Fprintf(w, "Token created: %d (%s)\n%s\n", result.ID, result.Label, result.Token)
		if result.ExpiresAt != "" {
			fmt.Fprintf(w, "Expires: %s\n", result.ExpiresAt)
		}
		return nil
	case http.StatusForbidden:
		return fmt.Errorf("permission denied")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runTokenDelete(cmd *cobra.Command, args []string) {
	if err := tokenDelete(args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func tokenDelete(tokenID string) error {
	req, err := newAPIRequest(http.MethodDelete, fmt.Sprintf("/api/v1/tokens/%s", tokenID), nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		fmt.Println("Token deleted.")
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("token not found")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

var tokenCmd = &cobra.Command{
	Use:   "token",
	Short: "Manage API tokens",
	Long:  `List, create, and delete API tokens for the authenticated user.`,
}

var tokenListCmd = &cobra.Command{
	Use:   "list",
	Short: "List API tokens",
	Run:   runTokenList,
}

var tokenCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new API token",
	Run:   runTokenCreate,
}

var tokenDeleteCmd = &cobra.Command{
	Use:   "delete [token-id]",
	Short: "Delete an API token",
	Args:  cobra.ExactArgs(1),
	Run:   runTokenDelete,
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Vectis CLI - Command line interface for Vectis",
	Long:  `Vectis CLI provides commands to interact with the Vectis build system.`,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	logsRunCmd.Flags().Bool("stdout", false, "Only show stdout")
	logsRunCmd.Flags().Bool("stderr", false, "Only show stderr")
	logsJobCmd.Flags().Bool("stdout", false, "Only show stdout")
	logsJobCmd.Flags().Bool("stderr", false, "Only show stderr")

	logsCmd.AddCommand(logsRunCmd)
	logsCmd.AddCommand(logsJobCmd)

	triggerCmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	runCmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs run <run-id>)")

	rootCmd.AddCommand(triggerCmd)
	rootCmd.AddCommand(logsCmd)
	rootCmd.AddCommand(runCmd)
	getCmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
	rootCmd.AddCommand(getCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(editCmd)
	rootCmd.AddCommand(migrateCmd)
	forceFailCmd.Flags().String("reason", "", "Failure reason to record")
	rootCmd.AddCommand(forceFailCmd)
	rootCmd.AddCommand(forceRequeueCmd)

	loginCmd.Flags().StringP("username", "u", "", "Username (optional; prompts if omitted)")
	rootCmd.AddCommand(loginCmd)
	rootCmd.AddCommand(logoutCmd)

	tokenCmd.AddCommand(tokenListCmd)
	tokenCreateCmd.Flags().String("label", "", "Token label (required)")
	tokenCreateCmd.Flags().String("expires-in", "never", "Expiry preset (1w, 1m, 3m, 6m, 1y, never)")
	tokenCreateCmd.Flags().Int64("user-id", 0, "Create token for another user (admin only)")
	tokenCmd.AddCommand(tokenCreateCmd)
	tokenCmd.AddCommand(tokenDeleteCmd)
	rootCmd.AddCommand(tokenCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
