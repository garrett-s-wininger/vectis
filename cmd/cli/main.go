package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

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

func runLogStream(jobID string, filterStdout, filterStderr, continuous bool) error {
	wsURL := fmt.Sprintf("ws://localhost%s/ws/logs/%s", networking.LogWebSocketPort, jobID)
	if continuous {
		// NOTE(garrett): Quick hack to skip historical logs in continuous mode.
		wsURL = wsURL + "?history=0"
	}

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect to log service: %w", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to logs for job %s\n", jobID)
	if continuous {
		fmt.Println("Streaming logs for all executions... (press Ctrl+C to exit)")
	} else {
		fmt.Println("Streaming logs for a single execution... (press Ctrl+C to exit)")
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	var completionStatus *string
	executionCount := 0

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
					executionCount++
					fmt.Printf("\n=== Execution %d started for job %s ===\n", executionCount, jobID)
					completionStatus = nil
				case "completed":
					status := meta.Status
					completionStatus = &status
					switch status {
					case "success":
						fmt.Printf("Execution %d for job %s finished successfully.\n", executionCount, jobID)
					case "failure":
						fmt.Printf("Execution %d for job %s failed.\n", executionCount, jobID)
					default:
						fmt.Printf("Execution %d for job %s finished (status: %s).\n", executionCount, jobID, status)
					}

					if !continuous {
						_ = conn.WriteControl(
							websocket.CloseMessage,
							websocket.FormatCloseMessage(websocket.CloseNormalClosure, "execution completed"),
							time.Now().Add(5*time.Second),
						)

						_ = conn.Close()
						return
					}
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
		if !continuous && completionStatus != nil {
			switch *completionStatus {
			case "success":
				fmt.Printf("Job %s finished successfully.\n", jobID)
			case "failure":
				fmt.Printf("Job %s failed.\n", jobID)
			default:
				fmt.Printf("Job %s finished (status: %s).\n", jobID, *completionStatus)
			}
		}

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
	case http.StatusNoContent:
		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(jobID, false, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(jobID)
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

func streamLogs(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobIDArg := args[0]
	var jobID string
	if jobIDArg == "-" {
		reader := bufio.NewReader(os.Stdin)
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "Error: failed to read job-id from stdin: %v\n", err)
			os.Exit(1)
		}

		jobID = strings.TrimSpace(line)
		if jobID == "" {
			fmt.Fprintln(os.Stderr, "Error: empty job-id from stdin")
			os.Exit(1)
		}
	} else {
		jobID = jobIDArg
	}

	follow, _ := cmd.Flags().GetBool("follow")
	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	continuous, _ := cmd.Flags().GetBool("continuous")

	// NOTE(garrett): Require follow mode for now - we don't have historical log retrieval.
	if !follow {
		fmt.Fprintln(os.Stderr, "Error: --follow flag is required (historical log retrieval not yet implemented)")
		os.Exit(1)
	}

	if err := runLogStream(jobID, filterStdout, filterStderr, continuous); err != nil {
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
			ID string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.ID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.ID, false, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.ID)
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

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a stored job",
	Long:  `Trigger a stored job by its job-id. The job must exist in the database.`,
	Args:  cobra.ExactArgs(1),
	Run:   triggerJob,
}

var logsCmd = &cobra.Command{
	Use:   "logs [job-id]",
	Short: "Stream logs for a job",
	Long:  `Stream logs for a job via WebSocket. Use --follow to keep streaming new logs. Use "-" as job-id to read from stdin (e.g. from trigger or run).`,
	Args:  cobra.ExactArgs(1),
	Run:   streamLogs,
}

var runCmd = &cobra.Command{
	Use:   "run [path|-]",
	Short: "Submit an ephemeral job",
	Long:  `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin. On success prints the job ID only; use --follow to stream logs.`,
	Args:  cobra.ExactArgs(1),
	Run:   runJob,
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Vectis CLI - Command line interface for Vectis",
	Long:  `Vectis CLI provides commands to interact with the Vectis build system.`,
}

func init() {
	logsCmd.Flags().BoolP("follow", "f", false, "Keep streaming logs (don't exit after existing logs)")
	logsCmd.Flags().Bool("stdout", false, "Only show stdout")
	logsCmd.Flags().Bool("stderr", false, "Only show stderr")
	logsCmd.Flags().BoolP("continuous", "c", false, "Continuously follow all executions of the job-id")

	triggerCmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs --follow)")
	runCmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs --follow)")

	rootCmd.AddCommand(triggerCmd)
	rootCmd.AddCommand(logsCmd)
	rootCmd.AddCommand(runCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
