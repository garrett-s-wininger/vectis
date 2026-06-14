package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"vectis/api/gen/go"
)

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Stream    int    `json:"stream"`
	Sequence  int64  `json:"sequence"`
	Data      string `json:"data"`
}

type jobRunEvent struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
}

func runLogStream(runID string, filterStdout, filterStderr bool) error {
	return runLogStreamPath(fmt.Sprintf("/api/v1/runs/%s/logs", url.PathEscape(runID)), runID, filterStdout, filterStderr)
}

func runSourceLogStream(repositoryID, jobID, runID string, filterStdout, filterStderr bool) error {
	path := fmt.Sprintf("/api/v1/source-repositories/%s/jobs/%s/runs/%s/logs",
		url.PathEscape(repositoryID),
		url.PathEscape(jobID),
		url.PathEscape(runID),
	)
	return runLogStreamPath(path, runID, filterStdout, filterStderr)
}

func runLogStreamPath(path, runID string, filterStdout, filterStderr bool) error {
	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create log stream request: %w", err)
	}

	req.Header.Set("Accept", "text/event-stream")
	ctx, cancel := context.WithCancel(context.Background())
	req = req.WithContext(ctx)

	resp, err := doAPIStreamRequest(req)
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

	if !outputIsJSON() {
		fmt.Printf("Connected to logs for run %s\n", runID)
		fmt.Println("Streaming logs... (press Ctrl+C to exit)")
	}

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
					if outputIsJSON() {
						if err := writeCompactJSON(os.Stdout, entry); err != nil {
							fmt.Fprintf(os.Stderr, "Error: failed to write log entry: %v\n", err)
						}
					}

					var meta struct {
						Event  string `json:"event"`
						Status string `json:"status,omitempty"`
					}

					if err := json.Unmarshal([]byte(entry.Data), &meta); err != nil {
						continue
					}

					if !outputIsJSON() {
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

				if outputIsJSON() {
					if err := writeCompactJSON(os.Stdout, entry); err != nil {
						fmt.Fprintf(os.Stderr, "Error: failed to write log entry: %v\n", err)
					}

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
		if !outputIsJSON() {
			fmt.Println("\nDisconnecting...")
		}

		cancel()
		<-done
		return fmt.Errorf("interrupted")
	}
}

func runContinuousLogs(jobID string, filterStdout, filterStderr bool) error {
	return runContinuousLogsForRepository(jobID, "", filterStdout, filterStderr)
}

func runContinuousLogsForRepository(jobID, repositoryID string, filterStdout, filterStderr bool) error {
	label := "job " + jobID
	if strings.TrimSpace(repositoryID) != "" {
		label = "source job " + repositoryID + "/" + jobID
	}

	return runContinuousLogsPath(
		label,
		jobRunsSSEPath(jobID, repositoryID),
		jobRunsPath(jobID, repositoryID),
		func(ev jobRunEvent) error {
			return runLogStream(ev.RunID, filterStdout, filterStderr)
		},
	)
}

func runContinuousSourceLogs(repositoryID, jobID string, filterStdout, filterStderr bool) error {
	return runContinuousLogsPath(
		"source job "+repositoryID+"/"+jobID,
		fmt.Sprintf("/api/v1/sse/source-repositories/%s/jobs/%s/runs", url.PathEscape(repositoryID), url.PathEscape(jobID)),
		fmt.Sprintf("/api/v1/source-repositories/%s/jobs/%s/runs", url.PathEscape(repositoryID), url.PathEscape(jobID)),
		func(ev jobRunEvent) error {
			return runSourceLogStream(repositoryID, jobID, ev.RunID, filterStdout, filterStderr)
		},
	)
}

func jobRunsPath(jobID, repositoryID string) string {
	path := fmt.Sprintf("/api/v1/jobs/%s/runs", url.PathEscape(jobID))
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", repositoryID)
	return appendQueryParams(path, params)
}

func jobRunsSSEPath(jobID, repositoryID string) string {
	path := fmt.Sprintf("/api/v1/sse/jobs/%s/runs", url.PathEscape(jobID))
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", repositoryID)
	return appendQueryParams(path, params)
}

func runContinuousLogsPath(label, ssePath, runsPath string, streamRun func(jobRunEvent) error) error {
	lastIndex := 0
	if !outputIsJSON() {
		fmt.Printf("Streaming logs for %s (Ctrl+C to stop)\n", label)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)

outer:
	for {
		attemptCtx, attemptCancel := context.WithCancel(context.Background())

		runChan := make(chan jobRunEvent, 32)
		go func() {
			defer close(runChan)
			req, err := newAPIRequest(http.MethodGet, ssePath, nil)
			if err != nil {
				return
			}

			req = req.WithContext(attemptCtx)
			req.Header.Set("Accept", "text/event-stream")

			resp, err := doAPIStreamRequest(req)
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

					var ev jobRunEvent
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

		params := url.Values{}
		params.Set("after_index", strconv.Itoa(lastIndex))
		req, err := newAPIRequest(http.MethodGet, appendQueryParams(runsPath, params), nil)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("creating runs request: %w", err)
		}

		resp, err := doAPIRequest(req)
		if err != nil {
			attemptCancel()
			return fmt.Errorf("fetching runs: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			attemptCancel()
			return fmt.Errorf("listing runs failed: %s", resp.Status)
		}

		runs, err := decodeJobRuns(resp.Body)
		if err != nil {
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
				if !outputIsJSON() {
					fmt.Println("\nStopping.")
				}

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

				if err := streamRun(ev); err != nil {
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

func decodeJobRuns(r io.Reader) ([]jobRunEvent, error) {
	var result struct {
		Data []jobRunEvent `json:"data"`
	}

	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return nil, err
	}

	return result.Data, nil
}

func latestRunForJob(jobID string) (jobRunEvent, bool, error) {
	return latestRunForJobInRepository(jobID, "")
}

func latestRunForJobInRepository(jobID, repositoryID string) (jobRunEvent, bool, error) {
	return latestRunForJobPath(jobRunsPath(jobID, repositoryID))
}

func latestRunForSourceJob(repositoryID, jobID string) (jobRunEvent, bool, error) {
	return latestRunForJobPath(fmt.Sprintf("/api/v1/source-repositories/%s/jobs/%s/runs", url.PathEscape(repositoryID), url.PathEscape(jobID)))
}

func latestRunForJobPath(path string) (jobRunEvent, bool, error) {
	var latest jobRunEvent
	cursor := int64(0)

	for {
		params := url.Values{}
		params.Set("limit", "200")
		if cursor > 0 {
			params.Set("cursor", strconv.FormatInt(cursor, 10))
		}

		req, err := newAPIRequest(http.MethodGet, appendQueryParams(path, params), nil)
		if err != nil {
			return jobRunEvent{}, false, err
		}

		resp, err := doAPIRequest(req)
		if err != nil {
			return jobRunEvent{}, false, fmt.Errorf("fetching runs: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			return jobRunEvent{}, false, fmt.Errorf("listing runs failed: %s", resp.Status)
		}

		var result struct {
			Data       []jobRunEvent `json:"data"`
			NextCursor *int64        `json:"next_cursor,omitempty"`
		}

		err = json.NewDecoder(resp.Body).Decode(&result)
		_ = resp.Body.Close()
		if err != nil {
			return jobRunEvent{}, false, fmt.Errorf("parsing runs: %w", err)
		}

		for _, run := range result.Data {
			if run.RunID != "" && run.RunIndex >= latest.RunIndex {
				latest = run
			}
		}

		if result.NextCursor == nil {
			break
		}

		cursor = *result.NextCursor
	}

	return latest, latest.RunID != "", nil
}

func appendQueryParams(path string, params url.Values) string {
	encoded := params.Encode()
	if encoded == "" {
		return path
	}

	if strings.Contains(path, "?") {
		return path + "&" + encoded
	}

	return path + "?" + encoded
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
		runCLIError(err)
	}

	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	runCLIError(runLogStream(id, filterStdout, filterStderr))
}

func runLogsJob(cmd *cobra.Command, args []string) {
	jobID, err := resolveLogIDArg(args[0])
	if err != nil {
		runCLIError(err)
	}

	repositoryID := stringFlagValue(cmd, "repository")
	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	follow, _ := cmd.Flags().GetBool("follow")
	if follow {
		if err := runContinuousLogsForRepository(jobID, repositoryID, filterStdout, filterStderr); err != nil {
			if err.Error() == "interrupted" {
				return
			}

			runCLIError(err)
		}

		return
	}

	run, ok, err := latestRunForJobInRepository(jobID, repositoryID)
	if err != nil {
		runCLIError(err)
	}

	if !ok {
		if repositoryID != "" {
			runCLIError(fmt.Errorf("no runs found for source job %q/%q; use --follow to wait for future runs", repositoryID, jobID))
		}

		runCLIError(fmt.Errorf("no runs found for job %q; use --follow to wait for future runs", jobID))
	}

	if !outputIsJSON() {
		if repositoryID != "" {
			fmt.Printf("Streaming latest run for source job %s/%s: %s\n", repositoryID, jobID, run.RunID)
		} else {
			fmt.Printf("Streaming latest run for job %s: %s\n", jobID, run.RunID)
		}
	}

	runCLIError(runLogStream(run.RunID, filterStdout, filterStderr))
}

var logsCmd = &cobra.Command{
	Use:     "logs",
	Short:   "Stream logs for runs",
	Long:    `Stream logs via Server-Sent Events (SSE). Use "logs run" for a single run. Use "logs job" for the latest run of a job, add --repository for source-backed jobs, or add --follow to wait for future runs. Use "-" as the id to read from stdin.`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
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
	Short: "Stream logs for the latest run of a job",
	Long:  `Stream logs for the latest run of a job. Add --repository for source-backed jobs, and add --follow to wait for each future run triggered after you connect. Argument is a job-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsJob,
}

func configureLogFilterFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("stdout", false, "Only show stdout")
	cmd.Flags().Bool("stderr", false, "Only show stderr")
}

func configureLogsJobFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "Wait for and stream future runs for this job")
	cmd.Flags().String("repository", "", "Source repository ID for source-backed job logs")
}
