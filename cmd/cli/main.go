package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/database"

	_ "github.com/mattn/go-sqlite3"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Stream    int    `json:"stream"`
	Sequence  int64  `json:"sequence"`
	Data      string `json:"data"`
}

func runLogStream(runID string, filterStdout, filterStderr bool) error {
	sseURL := config.PublicLogSSEURL(runID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create log stream request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to log service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("log stream request failed: %s", resp.Status)
	}

	fmt.Printf("Connected to logs for run %s\n", runID)
	fmt.Println("Streaming logs... (press Ctrl+C to exit)")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

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

			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
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
	apiAddr := config.PublicAPIBaseURL()

	url := fmt.Sprintf("%s/api/v1/jobs/trigger/%s", apiAddr, jobID)
	resp, err := http.Post(url, "application/json", nil)
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
	apiAddr := config.PublicAPIBaseURL()
	lastIndex := 0
	fmt.Printf("Streaming logs for job %s (Ctrl+C to stop)\n", jobID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	type runEvent struct {
		RunID    string `json:"run_id"`
		RunIndex int    `json:"run_index"`
	}

outer:
	for {
		sseURL := fmt.Sprintf("%s/api/v1/sse/jobs/%s/runs", apiAddr, jobID)
		attemptCtx, attemptCancel := context.WithCancel(context.Background())

		runChan := make(chan runEvent, 32)
		go func() {
			defer close(runChan)
			req, err := http.NewRequestWithContext(attemptCtx, http.MethodGet, sseURL, nil)
			if err != nil {
				return
			}
			req.Header.Set("Accept", "text/event-stream")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()

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

				if strings.HasPrefix(line, "data:") {
					data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
					dataBuf.WriteString(data)
				}
			}
		}()

		catchUpURL := fmt.Sprintf("%s/api/v1/jobs/%s/runs?since=%d", apiAddr, jobID, lastIndex)
		resp, err := http.Get(catchUpURL)
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

	apiAddr := config.PublicAPIBaseURL()
	url := apiAddr + "/api/v1/jobs/run"
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
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
	apiAddr := config.PublicAPIBaseURL()
	getURL := fmt.Sprintf("%s/api/v1/jobs/%s", apiAddr, jobID)

	resp, err := http.Get(getURL)
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

	apiAddr := config.PublicAPIBaseURL()
	putURL := fmt.Sprintf("%s/api/v1/jobs/%s", apiAddr, jobID)
	req, err := http.NewRequest(http.MethodPut, putURL, bytes.NewReader(pretty))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create update request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	updateResp, err := http.DefaultClient.Do(req)
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
	apiAddr := config.PublicAPIBaseURL()
	url := apiAddr + "/api/v1/jobs"

	resp, err := http.Get(url)
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
	Long:  `Trigger a stored job by its job-id. The job must exist in the database.`,
	Args:  cobra.ExactArgs(1),
	Run:   triggerJob,
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
	Long:  `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin. On success prints the job ID only; use --follow to stream logs.`,
	Args:  cobra.ExactArgs(1),
	Run:   runJob,
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

var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Apply database migrations",
	Long:  `Create the data directory if needed and run embedded SQL migrations on the Vectis SQLite database.`,
	Args:  cobra.NoArgs,
	Run:   runMigrate,
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Vectis CLI - Command line interface for Vectis",
	Long:  `Vectis CLI provides commands to interact with the Vectis build system.`,
}

func init() {
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
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
