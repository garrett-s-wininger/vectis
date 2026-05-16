package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type runDetail struct {
	RunID          string          `json:"run_id"`
	RunIndex       int             `json:"run_index"`
	Status         string          `json:"status"`
	OrphanReason   *string         `json:"orphan_reason,omitempty"`
	FailureCode    *string         `json:"failure_code,omitempty"`
	CreatedAt      *string         `json:"created_at,omitempty"`
	StartedAt      *string         `json:"started_at,omitempty"`
	FinishedAt     *string         `json:"finished_at,omitempty"`
	FailureReason  *string         `json:"failure_reason,omitempty"`
	DispatchEvents []dispatchEvent `json:"dispatch_events,omitempty"`
}

type dispatchEvent struct {
	ID        int64   `json:"id"`
	Source    string  `json:"source"`
	EventType string  `json:"event_type"`
	Message   *string `json:"message,omitempty"`
	CreatedAt int64   `json:"created_at"`
}

type runListResult struct {
	Data []struct {
		RunID         string  `json:"run_id"`
		RunIndex      int     `json:"run_index"`
		Status        string  `json:"status"`
		CreatedAt     *string `json:"created_at,omitempty"`
		StartedAt     *string `json:"started_at,omitempty"`
		FinishedAt    *string `json:"finished_at,omitempty"`
		FailureCode   *string `json:"failure_code,omitempty"`
		FailureReason *string `json:"failure_reason,omitempty"`
	} `json:"data"`
	NextCursor *int64 `json:"next_cursor,omitempty"`
}

type runRepairResult struct {
	Status string `json:"status"`
	RunID  string `json:"run_id"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

func runGetRun(cmd *cobra.Command, args []string) {
	runCLIError(getRun(args[0], os.Stdout))
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
		var run runDetail

		if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if outputIsJSON() {
			return writeJSON(w, run)
		}

		fmt.Fprintf(w, "run_id=%s\n", run.RunID)
		fmt.Fprintf(w, "run_index=%d\n", run.RunIndex)
		fmt.Fprintf(w, "status=%s\n", run.Status)
		if run.CreatedAt != nil {
			fmt.Fprintf(w, "created_at=%s\n", *run.CreatedAt)
		}

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
	runCLIError(cancelRun(args[0], os.Stdout))
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
		if outputIsJSON() {
			return writeJSON(w, map[string]string{"status": "cancel_requested", "run_id": runID})
		}

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
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("job id is required (pass [job-id] or --job)"))
	}

	since, _ := cmd.Flags().GetString("since")
	runCLIError(listRuns(jobID, runListLimit, runListCursor, since, os.Stdout))
}

func listRuns(jobID string, limit, cursor int, since string, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/jobs/%s/runs", jobID)
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", limit))
	}

	if cursor > 0 {
		params.Set("cursor", fmt.Sprintf("%d", cursor))
	}

	if strings.TrimSpace(since) != "" {
		params.Set("since", strings.TrimSpace(since))
	}

	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
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

	var result runListResult

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if outputIsJSON() {
		return writeJSON(w, result)
	}

	if len(result.Data) == 0 {
		fmt.Fprintln(w, "No runs found")
		return nil
	}

	fmt.Fprintf(w, "%-20s %-5s %-12s %-24s %-24s %-24s\n",
		"RUN ID", "INDEX", "STATUS", "CREATED", "STARTED", "FINISHED")

	for _, r := range result.Data {
		created := "-"
		if r.CreatedAt != nil {
			created = *r.CreatedAt
		}

		started := "-"
		if r.StartedAt != nil {
			started = *r.StartedAt
		}

		finished := "-"
		if r.FinishedAt != nil {
			finished = *r.FinishedAt
		}

		fmt.Fprintf(w, "%-20s %-5d %-12s %-24s %-24s %-24s\n",
			r.RunID, r.RunIndex, r.Status, created, started, finished)
	}

	if result.NextCursor != nil {
		fmt.Fprintf(w, "\nMore runs available. Continue with --cursor %d.\n", *result.NextCursor)
	}

	return nil
}

func forceFailRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	reason, _ := cmd.Flags().GetString("reason")
	runCLIError(forceFail(runID, reason, os.Stdout))
}

func forceFail(runID, reason string, w io.Writer) error {
	body := []byte("{}")
	if reason != "" {
		payload, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return fmt.Errorf("failed to encode request body: %w", err)
		}

		body = payload
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-fail", runID), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			return writeJSON(w, map[string]string{"status": "force_failed", "run_id": runID})
		}

		fmt.Fprintf(w, "Run %s force-failed.\n", runID)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func forceRequeueRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	runCLIError(forceRequeue(runID, os.Stdout))
}

func forceRequeue(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/force-requeue", runID), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			return writeJSON(w, map[string]string{"status": "force_requeued", "run_id": runID})
		}

		fmt.Fprintf(w, "Run %s force-requeued.\n", runID)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusConflict:
		return fmt.Errorf("run %q cannot be requeued from its current status", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func repairMarkRun(cmd *cobra.Command, args []string) {
	runID := args[0]
	status := strings.TrimPrefix(cmd.Name(), "mark-")
	reason, _ := cmd.Flags().GetString("reason")
	runCLIError(markRunForRepair(runID, status, reason, os.Stdout))
}

func markRunForRepair(runID, state, reason string, w io.Writer) error {
	body := []byte("{}")
	if reason != "" {
		payload, err := json.Marshal(map[string]string{"reason": reason})
		if err != nil {
			return fmt.Errorf("failed to encode request body: %w", err)
		}

		body = payload
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/repair/mark-%s", runID, state), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			return writeJSON(w, runRepairResult{Status: "marked", RunID: runID, State: state, Reason: reason})
		}

		fmt.Fprintf(w, "Repair recorded: run %s marked %s.\n", runID, state)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusConflict:
		return fmt.Errorf("run %q cannot be marked %s from its current status", runID, state)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

var runsCmd = &cobra.Command{
	Use:   "runs",
	Short: "Inspect, cancel, and repair runs",
	Long: `Inspect run status, list a job's run history, cancel executing runs, and perform manual repair actions.

Common flows:
  vectis-cli runs show run-123
  vectis-cli runs list build-main
  vectis-cli runs repair mark-queued run-123`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var runRepairCmd = &cobra.Command{
	Use:   "repair",
	Short: "Manually reconcile orphaned or stuck run records",
	Long: `Repair commands are operator-only reconciliation tools. Use them when the API's durable run state disagrees with verified external evidence, or when a stuck run needs to be returned to queued for dispatch recovery.

These commands update durable state directly; they do not contact a worker or stream logs.`,
	Run: showCommandHelp,
}

var forceFailCmd = &cobra.Command{
	Use:        "fail [run-id]",
	Short:      "Manually mark a run as failed",
	Long:       `Force a run into failed status in the API/database. Intended for manual intervention flows.`,
	Args:       cobra.ExactArgs(1),
	Run:        forceFailRun,
	Hidden:     true,
	Deprecated: "use 'runs repair mark-failed' instead",
}

var forceRequeueCmd = &cobra.Command{
	Use:        "retry [run-id]",
	Short:      "Manually retry a queued run",
	Long:       `Force a run back to queued status for manual retry/recovery. Intended for manual intervention flows.`,
	Args:       cobra.ExactArgs(1),
	Run:        forceRequeueRun,
	Hidden:     true,
	Deprecated: "use 'runs repair mark-queued' instead",
}

var repairMarkSucceededCmd = &cobra.Command{
	Use:   "mark-succeeded [run-id]",
	Short: "Mark an orphaned run as succeeded",
	Long:  `Record an orphaned run as succeeded when an operator has external evidence that the work completed successfully.`,
	Args:  cobra.ExactArgs(1),
	Run:   repairMarkRun,
}

var repairMarkFailedCmd = &cobra.Command{
	Use:   "mark-failed [run-id]",
	Short: "Mark an orphaned run as failed",
	Long:  `Record an orphaned run as failed when an operator has external evidence the work failed.`,
	Args:  cobra.ExactArgs(1),
	Run:   repairMarkRun,
}

var repairMarkCancelledCmd = &cobra.Command{
	Use:   "mark-cancelled [run-id]",
	Short: "Mark an orphaned run as cancelled",
	Long:  `Record an orphaned run as cancelled when an operator knows execution was intentionally stopped outside the normal RPC path.`,
	Args:  cobra.ExactArgs(1),
	Run:   repairMarkRun,
}

var repairMarkAbandonedCmd = &cobra.Command{
	Use:   "mark-abandoned [run-id]",
	Short: "Mark an orphaned run as abandoned",
	Long:  `Record an orphaned run as abandoned when no authoritative completion result is expected.`,
	Args:  cobra.ExactArgs(1),
	Run:   repairMarkRun,
}

var repairMarkQueuedCmd = &cobra.Command{
	Use:   "mark-queued [run-id]",
	Short: "Mark a stuck run for retry",
	Long:  `Record a stuck run as queued so the reconciler can dispatch it again. This is manual dispatch repair, not retry history.`,
	Args:  cobra.ExactArgs(1),
	Run:   repairMarkRun,
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
	Long: `List runs for a stored job, most recent first. Pass the job id as an argument or with --job.

Use --since to filter to runs created at or after a date. Use --limit to control page size.
When more runs are available, the command prints a cursor; pass that value back with --cursor
to fetch the next page.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runListRuns,
}

func configureRunListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&runListJobID, "job", "", "Job ID to list runs for")
	cmd.Flags().IntVar(&runListLimit, "limit", 0, "Max runs to return (default 50)")
	cmd.Flags().IntVar(&runListCursor, "cursor", 0, "Continue listing after this result cursor")
	cmd.Flags().String("since", "", "Only list runs created at or after this RFC3339 timestamp or YYYY-MM-DD date")
}

func configureForceFailFlags(cmd *cobra.Command) {
	cmd.Flags().String("reason", "", "Failure reason to record")
}

func configureRepairMarkFlags(cmd *cobra.Command) {
	cmd.Flags().String("reason", "", "Operator reason to record")
}
