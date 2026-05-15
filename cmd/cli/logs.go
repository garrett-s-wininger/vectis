package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"os/signal"
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
	lastIndex := 0
	if !outputIsJSON() {
		fmt.Printf("Streaming logs for job %s (Ctrl+C to stop)\n", jobID)
	}

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

var logsCmd = &cobra.Command{
	Use:     "logs",
	Short:   "Stream logs for runs",
	Long:    `Stream logs via Server-Sent Events (SSE). Use "logs run" for a single run (until it completes). Use "logs job" to follow a job (runs triggered after you connect). Use "-" as the id to read from stdin.`,
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
	Short: "Stream logs for a job (next runs only)",
	Long:  `Subscribe to run events for the job and stream logs for each run triggered after you connect. Does not stream historical runs. Re-subscribes if the runs connection closes. Argument is a job-id; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   runLogsJob,
}

func configureLogFilterFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("stdout", false, "Only show stdout")
	cmd.Flags().Bool("stderr", false, "Only show stderr")
}
