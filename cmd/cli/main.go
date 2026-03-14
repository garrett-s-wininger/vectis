package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

func triggerJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	// TODO(garrett): Make configurable
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

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a stored job",
	Long:  `Trigger a stored job by its job-id. The job must exist in the database.`,
	Args:  cobra.ExactArgs(1),
	Run:   triggerJob,
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Vectis CLI - Command line interface for Vectis",
	Long:  `Vectis CLI provides commands to interact with the Vectis build system.`,
}

func main() {
	rootCmd.AddCommand(triggerCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
