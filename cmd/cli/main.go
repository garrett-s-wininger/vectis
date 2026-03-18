package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gorilla/websocket"
	"github.com/spf13/cobra"

	api "vectis/api/gen/go"
	"vectis/internal/networking"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Stream    int    `json:"stream"`
	Sequence  int64  `json:"sequence"`
	Data      string `json:"data"`
}

func runLogStream(runID string, filterStdout, filterStderr bool) error {
	wsURL := fmt.Sprintf("ws://localhost%s/ws/logs/%s", networking.LogWebSocketPort, runID)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to log service: %w", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to logs for run %s\n", runID)
	fmt.Println("Streaming logs... (press Ctrl+C to exit)")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Fprintf(os.Stderr, "Error: websocket error: %v\n", err)
				}
				return
			}

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
					// NOTE(garrett): Server-side close, next ReadMessage will get the status.
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
		}
	}()

	select {
	case <-done:
		return nil
	case <-interrupt:
		fmt.Println("\nDisconnecting...")
		_ = conn.Close()
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
	apiAddr := "http://localhost:8080"

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
	apiAddr := "http://localhost:8080"
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
		wsURL := strings.Replace(apiAddr, "http://", "ws://", 1)
		wsURL = fmt.Sprintf("%s/api/v1/ws/jobs/%s/runs", wsURL, jobID)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			return fmt.Errorf("connecting to runs WebSocket: %w", err)
		}

		runChan := make(chan runEvent, 32)
		go func(c *websocket.Conn) {
			defer close(runChan)
			for {
				_, message, err := c.ReadMessage()
				if err != nil {
					if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
						return
					}
					fmt.Fprintf(os.Stderr, "WebSocket error: %v\n", err)
					return
				}

				var ev runEvent
				if err := json.Unmarshal(message, &ev); err != nil || ev.RunID == "" {
					continue
				}

				select {
				case runChan <- ev:
				default:
					fmt.Fprintf(os.Stderr, "Dropping run event (buffer full)\n")
				}
			}
		}(conn)

		catchUpURL := fmt.Sprintf("%s/api/v1/jobs/%s/runs?since=%d", apiAddr, jobID, lastIndex)
		resp, err := http.Get(catchUpURL)
		if err != nil {
			conn.Close()
			return fmt.Errorf("fetching runs: %w", err)
		}
		var runs []struct {
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&runs); err != nil {
			resp.Body.Close()
			conn.Close()
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
				conn.Close()
				fmt.Println("\nStopping.")
				return fmt.Errorf("interrupted")
			case ev, ok := <-runChan:
				if !ok {
					conn.Close()
					fmt.Fprintf(os.Stderr, "Runs connection closed; reconnecting...\n")
					continue outer
				}

				if ev.RunIndex > lastIndex {
					lastIndex = ev.RunIndex
				}

				if err := runLogStream(ev.RunID, filterStdout, filterStderr); err != nil {
					if err.Error() == "interrupted" {
						conn.Close()
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

	apiAddr := "http://localhost:8080"
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

func editJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	apiAddr := "http://localhost:8080"

	getURL := fmt.Sprintf("%s/api/v1/jobs/%s", apiAddr, jobID)
	resp, err := http.Get(getURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to fetch job definition: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %s\n", resp.Status)
		os.Exit(1)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read job definition: %v\n", err)
		os.Exit(1)
	}

	var indented bytes.Buffer
	if err := json.Indent(&indented, body, "", "  "); err != nil {
		indented.Reset()
		indented.Write(body)
	}

	tempFile, err := os.CreateTemp("", "vectis-job-*.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create temp file: %v\n", err)
		os.Exit(1)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := indented.WriteTo(tempFile); err != nil {
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
	pretty, err := json.MarshalIndent(&job, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to normalize job JSON: %v\n", err)
		os.Exit(1)
	}
	pretty = append(pretty, '\n')

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
	rootCmd.AddCommand(editCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
