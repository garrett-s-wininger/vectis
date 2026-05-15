package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func runGetRun(cmd *cobra.Command, args []string) {
	if err := getRun(args[0], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getRun(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s", runID), nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var run struct {
			RunID          string  `json:"run_id"`
			RunIndex       int     `json:"run_index"`
			Status         string  `json:"status"`
			OrphanReason   *string `json:"orphan_reason,omitempty"`
			FailureCode    *string `json:"failure_code,omitempty"`
			StartedAt      *string `json:"started_at,omitempty"`
			FinishedAt     *string `json:"finished_at,omitempty"`
			FailureReason  *string `json:"failure_reason,omitempty"`
			DispatchEvents []struct {
				ID        int64   `json:"id"`
				Source    string  `json:"source"`
				EventType string  `json:"event_type"`
				Message   *string `json:"message,omitempty"`
				CreatedAt int64   `json:"created_at"`
			} `json:"dispatch_events,omitempty"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		fmt.Fprintf(w, "run_id=%s\n", run.RunID)
		fmt.Fprintf(w, "run_index=%d\n", run.RunIndex)
		fmt.Fprintf(w, "status=%s\n", run.Status)
		if run.StartedAt != nil {
			fmt.Fprintf(w, "started_at=%s\n", *run.StartedAt)
		}

		if run.FinishedAt != nil {
			fmt.Fprintf(w, "finished_at=%s\n", *run.FinishedAt)
		}

		if run.FailureCode != nil {
			fmt.Fprintf(w, "failure_code=%s\n", *run.FailureCode)
		}

		if run.FailureReason != nil {
			fmt.Fprintf(w, "failure_reason=%s\n", *run.FailureReason)
		}

		if run.OrphanReason != nil {
			fmt.Fprintf(w, "orphan_reason=%s\n", *run.OrphanReason)
		}

		if len(run.DispatchEvents) > 0 {
			fmt.Fprintln(w, "dispatch_events:")
			for _, ev := range run.DispatchEvents {
				ts := time.Unix(ev.CreatedAt, 0).UTC().Format(time.RFC3339)
				if ev.Message != nil {
					fmt.Fprintf(w, "  [%s] %s/%s: %s\n", ts, ev.Source, ev.EventType, *ev.Message)
				} else {
					fmt.Fprintf(w, "  [%s] %s/%s\n", ts, ev.Source, ev.EventType)
				}
			}
		}

		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runCancelRun(cmd *cobra.Command, args []string) {
	if err := cancelRun(args[0], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func cancelRun(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/cancel", runID), nil)
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
		fmt.Fprintf(w, "Run %s cancel requested.\n", runID)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusConflict:
		return fmt.Errorf("run %q is not cancellable", runID)
	case http.StatusBadGateway:
		return fmt.Errorf("failed to send cancel to worker")
	case http.StatusServiceUnavailable:
		return fmt.Errorf("worker resolution not configured")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runListRuns(cmd *cobra.Command, args []string) {
	jobID := strings.TrimSpace(runListJobID)
	if len(args) > 0 {
		jobID = strings.TrimSpace(args[0])
	}

	if jobID == "" {
		fmt.Fprintln(os.Stderr, "Error: job id is required (pass [job-id] or --job)")
		_ = cmd.Usage()
		os.Exit(1)
	}

	if err := listRuns(jobID, runListLimit, runListSince, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func listRuns(jobID string, limit, since int, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/runs", jobID)
	params := []string{}
	if limit > 0 {
		params = append(params, fmt.Sprintf("limit=%d", limit))
	}

	if since > 0 {
		params = append(params, fmt.Sprintf("since=%d", since))
	}

	if len(params) > 0 {
		path += "?" + strings.Join(params, "&")
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
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

	var result struct {
		Data []struct {
			RunID         string  `json:"run_id"`
			RunIndex      int     `json:"run_index"`
			Status        string  `json:"status"`
			StartedAt     *string `json:"started_at,omitempty"`
			FinishedAt    *string `json:"finished_at,omitempty"`
			FailureCode   *string `json:"failure_code,omitempty"`
			FailureReason *string `json:"failure_reason,omitempty"`
		} `json:"data"`
		NextCursor *int64 `json:"next_cursor,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if len(result.Data) == 0 {
		fmt.Fprintln(w, "No runs found")
		return nil
	}

	fmt.Fprintf(w, "%-20s %-5s %-12s %-24s %-24s\n",
		"RUN ID", "INDEX", "STATUS", "STARTED", "FINISHED")

	for _, r := range result.Data {
		started := "-"
		if r.StartedAt != nil {
			started = *r.StartedAt
		}

		finished := "-"
		if r.FinishedAt != nil {
			finished = *r.FinishedAt
		}

		fmt.Fprintf(w, "%-20s %-5d %-12s %-24s %-24s\n",
			r.RunID, r.RunIndex, r.Status, started, finished)
	}

	if result.NextCursor != nil {
		fmt.Fprintf(w, "\nMore runs available (cursor: %d)\n", *result.NextCursor)
	}

	return nil
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
		fmt.Fprintf(os.Stderr, "Error: run '%s' cannot be requeued from its current status\n", runID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Inspect, cancel, retry, and repair runs",
	Long: `Inspect run status, list a job's run history, cancel executing runs, and perform manual repair actions.

Common flows:
  vectis-cli runs show run-123
  vectis-cli runs list build-main
  vectis-cli runs retry run-123`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var forceFailCmd = &cobra.Command{
	Use:   "fail [run-id]",
	Short: "Manually mark a run as failed",
	Long:  `Force a run into failed status in the API/database. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceFailRun,
}

var forceRequeueCmd = &cobra.Command{
	Use:   "retry [run-id]",
	Short: "Manually retry a queued run",
	Long:  `Force a run back to queued status for manual retry/recovery. Intended for manual intervention flows.`,
	Args:  cobra.ExactArgs(1),
	Run:   forceRequeueRun,
}

var runGetCmd = &cobra.Command{
	Use:   "show [run-id]",
	Short: "Show run status and failure details",
	Long:  `Fetch a run by run-id and print operator-facing status and failure fields.`,
	Args:  cobra.ExactArgs(1),
	Run:   runGetRun,
}

var runCancelCmd = &cobra.Command{
	Use:   "cancel [run-id]",
	Short: "Request cancellation for an executing run",
	Long:  `Request cancellation for an executing run through the worker control path.`,
	Args:  cobra.ExactArgs(1),
	Run:   runCancelRun,
}

var runListCmd = &cobra.Command{
	Use:   "list [job-id]",
	Short: "List runs for a job",
	Long:  `List runs for a stored job, most recent first. Pass the job id as an argument or with --job. Supports pagination via --limit and cursor.`,
	Args:  cobra.MaximumNArgs(1),
	Run:   runListRuns,
}

func configureRunListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&runListJobID, "job", "", "Job ID to list runs for")
	cmd.Flags().IntVar(&runListLimit, "limit", 0, "Max runs to return (default 50)")
	cmd.Flags().IntVar(&runListSince, "since", 0, "Only list runs after this run index")
}

func configureForceFailFlags(cmd *cobra.Command) {
	cmd.Flags().String("reason", "", "Failure reason to record")
}
