package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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
		fmt.Printf("Successfully triggered job: %s\n", jobID)
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

	jobID := args[0]
	follow, _ := cmd.Flags().GetBool("follow")
	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")

	// NOTE(garrett): Require follow mode for now - we don't have historical log retrieval.
	if !follow {
		fmt.Fprintln(os.Stderr, "Error: --follow flag is required (historical log retrieval not yet implemented)")
		os.Exit(1)
	}

	wsURL := fmt.Sprintf("ws://localhost%s/ws/logs/%s", networking.LogWebSocketPort, jobID)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect to log service: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Printf("Connected to logs for job %s\n", jobID)
	fmt.Println("Streaming logs... (press Ctrl+C to exit)")

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					fmt.Fprintf(os.Stderr, "Error: websocket error: %v\n", err)
				}
				close(done)
				return
			}

			var entry LogEntry
			if err := json.Unmarshal(message, &entry); err != nil {
				fmt.Fprintf(os.Stderr, "Error: failed to parse log entry: %v\n", err)
				continue
			}

			if filterStdout && entry.Stream != int(api.Stream_STREAM_STDOUT.Number()) {
				continue
			}
			if filterStderr && entry.Stream != int(api.Stream_STREAM_STDERR.Number()) {
				continue
			}

			streamPrefix := ""
			if entry.Stream == 1 {
				streamPrefix = "[stderr] "
			}

			fmt.Printf("%s%s\n", streamPrefix, entry.Data)
		}
	}()

	select {
	case <-done:
	case <-interrupt:
		fmt.Println("\nDisconnecting...")
		conn.Close()
		<-done
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
	Long:  `Stream logs for a job via WebSocket. Use --follow to keep streaming new logs.`,
	Args:  cobra.ExactArgs(1),
	Run:   streamLogs,
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

	rootCmd.AddCommand(triggerCmd)
	rootCmd.AddCommand(logsCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
