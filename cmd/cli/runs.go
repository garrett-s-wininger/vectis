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
	"text/tabwriter"
	"time"
)

type runDetail struct {
	RunID           string            `json:"run_id"`
	RunIndex        int               `json:"run_index"`
	Status          string            `json:"status"`
	NextAction      string            `json:"next_action,omitempty"`
	OwningCell      string            `json:"owning_cell,omitempty"`
	OrphanReason    *string           `json:"orphan_reason,omitempty"`
	FailureCode     *string           `json:"failure_code,omitempty"`
	CreatedAt       *string           `json:"created_at,omitempty"`
	StartedAt       *string           `json:"started_at,omitempty"`
	FinishedAt      *string           `json:"finished_at,omitempty"`
	FailureReason   *string           `json:"failure_reason,omitempty"`
	DispatchSummary []dispatchSummary `json:"dispatch_summary,omitempty"`
	DispatchEvents  []dispatchEvent   `json:"dispatch_events,omitempty"`
	TaskCompletion  *taskCompletion   `json:"task_completion,omitempty"`
	TaskDispatch    *taskDispatch     `json:"task_dispatch,omitempty"`
	RunAuditFields
}

type RunAuditFields struct {
	DefinitionVersion    int      `json:"definition_version,omitempty"`
	DefinitionHash       string   `json:"definition_hash,omitempty"`
	ReplayOfRunID        *string  `json:"replay_of_run_id,omitempty"`
	TriggerInvocationID  *string  `json:"trigger_invocation_id,omitempty"`
	TriggerID            *int64   `json:"trigger_id,omitempty"`
	TriggerType          *string  `json:"trigger_type,omitempty"`
	TriggerPayloadHash   *string  `json:"trigger_payload_hash,omitempty"`
	RequestedCells       []string `json:"requested_cells,omitempty"`
	ExecutionPayloadHash string   `json:"execution_payload_hash,omitempty"`
}

type dispatchEvent struct {
	ID        int64   `json:"id"`
	Source    string  `json:"source"`
	EventType string  `json:"event_type"`
	Message   *string `json:"message,omitempty"`
	CreatedAt int64   `json:"created_at"`
}

type dispatchSummary struct {
	Source        string  `json:"source"`
	Accepted      int     `json:"accepted"`
	Attempts      int     `json:"attempts"`
	Successes     int     `json:"successes"`
	Failures      int     `json:"failures"`
	FirstEventAt  int64   `json:"first_event_at"`
	LastEventAt   int64   `json:"last_event_at"`
	LastEventType string  `json:"last_event_type"`
	LastMessage   *string `json:"last_message,omitempty"`
}

type taskCompletion struct {
	Total          int `json:"total"`
	Succeeded      int `json:"succeeded"`
	TerminalFailed int `json:"terminal_failed"`
	Incomplete     int `json:"incomplete"`
}

type taskDispatch struct {
	Total        int                  `json:"total"`
	Pending      int                  `json:"pending"`
	Failed       int                  `json:"failed"`
	Enqueued     int                  `json:"enqueued"`
	UnknownState int                  `json:"unknown_state,omitempty"`
	Truncated    bool                 `json:"truncated"`
	Limit        int                  `json:"limit"`
	Intents      []taskDispatchIntent `json:"intents,omitempty"`
}

type taskDispatchIntent struct {
	ExecutionID          string  `json:"execution_id"`
	TaskID               string  `json:"task_id"`
	TaskAttemptID        string  `json:"task_attempt_id"`
	SourceExecutionID    string  `json:"source_execution_id,omitempty"`
	CellID               string  `json:"cell_id"`
	State                string  `json:"state"`
	EnqueueAttempts      int     `json:"enqueue_attempts"`
	LastEnqueueError     *string `json:"last_enqueue_error,omitempty"`
	EnqueuedAt           *int64  `json:"enqueued_at,omitempty"`
	LastEnqueueAttemptAt *int64  `json:"last_enqueue_attempt_at,omitempty"`
	CreatedAt            int64   `json:"created_at"`
	UpdatedAt            int64   `json:"updated_at"`
}

type runListResult struct {
	Data       []runListRow `json:"data"`
	NextCursor *int64       `json:"next_cursor,omitempty"`
}

type runListRow struct {
	RunID         string  `json:"run_id"`
	RunIndex      int     `json:"run_index"`
	Status        string  `json:"status"`
	CreatedAt     *string `json:"created_at,omitempty"`
	StartedAt     *string `json:"started_at,omitempty"`
	FinishedAt    *string `json:"finished_at,omitempty"`
	FailureCode   *string `json:"failure_code,omitempty"`
	FailureReason *string `json:"failure_reason,omitempty"`
	OwningCell    string  `json:"owning_cell,omitempty"`
	RunAuditFields
}

type runTasksResult struct {
	Data       []runTaskRow `json:"data"`
	NextCursor *int64       `json:"next_cursor,omitempty"`
}

type runArtifactsResult struct {
	Data       []runArtifactRow `json:"data"`
	NextCursor *int64           `json:"next_cursor,omitempty"`
}

type runArtifactsListOptions struct {
	Limit         int
	Cursor        int
	TaskID        string
	TaskAttemptID string
	ExecutionID   string
}

type runArtifactRow struct {
	ID              int64           `json:"id"`
	RunID           string          `json:"run_id"`
	TaskID          *string         `json:"task_id,omitempty"`
	TaskAttemptID   *string         `json:"task_attempt_id,omitempty"`
	ExecutionID     *string         `json:"execution_id,omitempty"`
	CellID          string          `json:"cell_id"`
	Name            string          `json:"name"`
	Path            string          `json:"path"`
	ContentType     string          `json:"content_type,omitempty"`
	BlobKey         string          `json:"blob_key"`
	BlobAlgorithm   string          `json:"blob_algorithm"`
	BlobDigest      string          `json:"blob_digest"`
	SizeBytes       int64           `json:"size_bytes"`
	ArtifactShardID string          `json:"artifact_shard_id"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
	CreatedAt       int64           `json:"created_at"`
	UpdatedAt       int64           `json:"updated_at"`
}

type runTaskRow struct {
	TaskID       string              `json:"task_id"`
	RunID        string              `json:"run_id"`
	ParentTaskID *string             `json:"parent_task_id,omitempty"`
	TaskKey      string              `json:"task_key"`
	Name         string              `json:"name"`
	Status       string              `json:"status"`
	SpecHash     string              `json:"spec_hash,omitempty"`
	CreatedAt    *string             `json:"created_at,omitempty"`
	UpdatedAt    *string             `json:"updated_at,omitempty"`
	Attempts     []runTaskAttemptRow `json:"attempts"`
}

type runTaskAttemptRow struct {
	AttemptID       string  `json:"attempt_id"`
	TaskID          string  `json:"task_id"`
	RunID           string  `json:"run_id"`
	ExecutionID     string  `json:"execution_id,omitempty"`
	ExecutionStatus string  `json:"execution_status,omitempty"`
	CellID          string  `json:"cell_id"`
	LeaseOwner      *string `json:"lease_owner,omitempty"`
	LeaseUntil      *int64  `json:"lease_until,omitempty"`
	Attempt         int     `json:"attempt"`
	Status          string  `json:"status"`
	AcceptedAt      *string `json:"accepted_at,omitempty"`
	StartedAt       *string `json:"started_at,omitempty"`
	FinishedAt      *string `json:"finished_at,omitempty"`
	LastObservedAt  *int64  `json:"last_observed_at,omitempty"`
	EventSequence   int64   `json:"event_sequence"`
	CreatedAt       *string `json:"created_at,omitempty"`
	UpdatedAt       *string `json:"updated_at,omitempty"`
}

type runExecutionPayloadResult struct {
	RunID          string          `json:"run_id"`
	PayloadHash    string          `json:"payload_hash"`
	DefinitionHash string          `json:"definition_hash,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

type runReplayResult struct {
	JobID         string `json:"job_id"`
	RunID         string `json:"run_id"`
	RunIndex      int    `json:"run_index"`
	CellID        string `json:"cell_id,omitempty"`
	ReplayOfRunID string `json:"replay_of_run_id"`
}

type runRepairResult struct {
	Status string `json:"status"`
	RunID  string `json:"run_id"`
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

type runArtifactDownloadResult struct {
	Status      string `json:"status"`
	RunID       string `json:"run_id"`
	Name        string `json:"name"`
	OutputPath  string `json:"output_path"`
	ContentType string `json:"content_type,omitempty"`
	Bytes       int64  `json:"bytes"`
}

func runGetRun(cmd *cobra.Command, args []string) {
	runCLIError(getRun(args[0], os.Stdout))
}

func runGetRunPayload(cmd *cobra.Command, args []string) {
	runCLIError(getRunExecutionPayload(args[0], os.Stdout))
}

func runGetRunTasks(cmd *cobra.Command, args []string) {
	runCLIError(getRunTasks(args[0], runTasksLimit, runTasksCursor, os.Stdout))
}

func runListRunArtifacts(cmd *cobra.Command, args []string) {
	runCLIError(getRunArtifacts(args[0], runArtifactsListOptions{
		Limit:         runArtifactsLimit,
		Cursor:        runArtifactsCursor,
		TaskID:        runArtifactsTaskID,
		TaskAttemptID: runArtifactsAttemptID,
		ExecutionID:   runArtifactsExecID,
	}, os.Stdout))
}

func runDownloadRunArtifact(cmd *cobra.Command, args []string) {
	runCLIError(downloadRunArtifact(args[0], args[1], runArtifactOutput, os.Stdout))
}

func runReplayRun(cmd *cobra.Command, args []string) {
	runCLIError(replayRun(args[0], runReplayCellID, runReplayIdemKey, os.Stdout))
}

func getRun(runID string, w io.Writer) error {
	run, err := fetchRunDetail(runID)
	if err != nil {
		return err
	}

	if outputIsJSON() {
		return writeJSON(w, run)
	}

	fmt.Fprintf(w, "run_id=%s\n", run.RunID)
	fmt.Fprintf(w, "run_index=%d\n", run.RunIndex)
	fmt.Fprintf(w, "status=%s\n", run.Status)

	if strings.TrimSpace(run.NextAction) != "" {
		fmt.Fprintf(w, "next_action=%s\n", run.NextAction)
	}

	if strings.TrimSpace(run.OwningCell) != "" {
		fmt.Fprintf(w, "owning_cell=%s\n", run.OwningCell)
	}

	writeRunAuditFields(w, run.RunAuditFields)
	writeTaskCompletion(w, run.TaskCompletion)

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

	writeTaskDispatch(w, run.TaskDispatch)
	writeDispatchSummary(w, run.DispatchSummary)

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
}

func writeDispatchSummary(w io.Writer, summary []dispatchSummary) {
	if len(summary) == 0 {
		return
	}

	fmt.Fprintln(w, "dispatch_summary:")
	for _, row := range summary {
		lastAt := time.Unix(row.LastEventAt, 0).UTC().Format(time.RFC3339)
		fmt.Fprintf(w, "  %s: accepted=%d attempts=%d successes=%d failures=%d last=%s at %s", row.Source, row.Accepted, row.Attempts, row.Successes, row.Failures, row.LastEventType, lastAt)
		if row.LastMessage != nil {
			fmt.Fprintf(w, ": %s", *row.LastMessage)
		}
		fmt.Fprintln(w)
	}
}

func fetchRunDetail(runID string) (runDetail, error) {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s", runID), nil)
	if err != nil {
		return runDetail{}, err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return runDetail{}, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var run runDetail

		if err := json.NewDecoder(resp.Body).Decode(&run); err != nil {
			return runDetail{}, fmt.Errorf("failed to parse response: %w", err)
		}

		return run, nil
	case http.StatusNotFound:
		return runDetail{}, fmt.Errorf("run %q not found", runID)
	default:
		return runDetail{}, fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func writeTaskCompletion(w io.Writer, tc *taskCompletion) {
	if tc == nil || tc.Total == 0 {
		return
	}

	fmt.Fprintf(w, "task_completion: total=%d succeeded=%d terminal_failed=%d incomplete=%d\n",
		tc.Total, tc.Succeeded, tc.TerminalFailed, tc.Incomplete)
}

func writeTaskDispatch(w io.Writer, td *taskDispatch) {
	if td == nil || td.Total == 0 {
		return
	}

	fmt.Fprintf(w, "task_dispatch: total=%d pending=%d failed=%d enqueued=%d", td.Total, td.Pending, td.Failed, td.Enqueued)
	if td.UnknownState > 0 {
		fmt.Fprintf(w, " unknown=%d", td.UnknownState)
	}

	if td.Truncated {
		fmt.Fprintf(w, " truncated=true limit=%d", td.Limit)
	}

	fmt.Fprintln(w)

	for _, intent := range td.Intents {
		fmt.Fprintf(w, "  %s execution=%s task=%s attempt=%s cell=%s enqueue_attempts=%d",
			intent.State, intent.ExecutionID, intent.TaskID, intent.TaskAttemptID, intent.CellID, intent.EnqueueAttempts)

		if intent.LastEnqueueAttemptAt != nil {
			fmt.Fprintf(w, " last_attempt=%s", formatUnixNano(*intent.LastEnqueueAttemptAt))
		}

		if intent.EnqueuedAt != nil {
			fmt.Fprintf(w, " enqueued_at=%s", formatUnixNano(*intent.EnqueuedAt))
		}

		if intent.LastEnqueueError != nil {
			fmt.Fprintf(w, " error=%q", *intent.LastEnqueueError)
		}

		fmt.Fprintln(w)
	}
}

func formatUnixNano(value int64) string {
	return time.Unix(0, value).UTC().Format(time.RFC3339)
}

func writeRunAuditFields(w io.Writer, audit RunAuditFields) {
	if audit.DefinitionVersion > 0 {
		fmt.Fprintf(w, "definition_version=%d\n", audit.DefinitionVersion)
	}

	if strings.TrimSpace(audit.DefinitionHash) != "" {
		fmt.Fprintf(w, "definition_hash=%s\n", audit.DefinitionHash)
	}

	if audit.ReplayOfRunID != nil {
		fmt.Fprintf(w, "replay_of_run_id=%s\n", *audit.ReplayOfRunID)
	}

	if audit.TriggerInvocationID != nil {
		fmt.Fprintf(w, "trigger_invocation_id=%s\n", *audit.TriggerInvocationID)
	}

	if audit.TriggerID != nil {
		fmt.Fprintf(w, "trigger_id=%d\n", *audit.TriggerID)
	}

	if audit.TriggerType != nil {
		fmt.Fprintf(w, "trigger_type=%s\n", *audit.TriggerType)
	}

	if audit.TriggerPayloadHash != nil {
		fmt.Fprintf(w, "trigger_payload_hash=%s\n", *audit.TriggerPayloadHash)
	}

	if len(audit.RequestedCells) > 0 {
		fmt.Fprintf(w, "requested_cells=%s\n", strings.Join(audit.RequestedCells, ","))
	}

	if strings.TrimSpace(audit.ExecutionPayloadHash) != "" {
		fmt.Fprintf(w, "execution_payload_hash=%s\n", audit.ExecutionPayloadHash)
	}
}

func getRunExecutionPayload(runID string, w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/runs/%s/execution-payload", runID), nil)
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
		var result runExecutionPayloadResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if outputIsJSON() {
			return writeJSON(w, result)
		}

		fmt.Fprintf(w, "run_id=%s\n", result.RunID)
		fmt.Fprintf(w, "payload_hash=%s\n", result.PayloadHash)
		if strings.TrimSpace(result.DefinitionHash) != "" {
			fmt.Fprintf(w, "definition_hash=%s\n", result.DefinitionHash)
		}

		fmt.Fprintln(w, "payload:")
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, result.Payload, "", "  "); err == nil {
			_, err = pretty.WriteTo(w)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(w)
			return err
		}

		_, err := fmt.Fprintln(w, string(result.Payload))
		return err
	case http.StatusNotFound:
		return fmt.Errorf("execution payload for run %q not found", runID)
	case http.StatusForbidden:
		return fmt.Errorf("not authorized to read execution payload for run %q", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func getRunTasks(runID string, limit, cursor int, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/runs/%s/tasks", runID)
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", limit))
	}

	if cursor > 0 {
		params.Set("cursor", fmt.Sprintf("%d", cursor))
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

	switch resp.StatusCode {
	case http.StatusOK:
		var result runTasksResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if outputIsJSON() {
			return writeJSON(w, result)
		}

		if len(result.Data) == 0 {
			fmt.Fprintln(w, "No tasks found")
			return nil
		}

		for _, task := range result.Data {
			writeRunTask(w, task)
		}

		if result.NextCursor != nil {
			fmt.Fprintf(w, "\nMore tasks available. Continue with --cursor %d.\n", *result.NextCursor)
		}

		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusForbidden:
		return fmt.Errorf("not authorized to read tasks for run %q", runID)
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func writeRunTask(w io.Writer, task runTaskRow) {
	parentID := "-"
	if task.ParentTaskID != nil && strings.TrimSpace(*task.ParentTaskID) != "" {
		parentID = *task.ParentTaskID
	}

	taskKey := strings.TrimSpace(task.TaskKey)
	if taskKey == "" {
		taskKey = "-"
	}

	name := strings.TrimSpace(task.Name)
	if name == "" {
		name = "-"
	}

	status := strings.TrimSpace(task.Status)
	if status == "" {
		status = "-"
	}

	fmt.Fprintf(w, "task_id=%s parent=%s key=%s name=%s status=%s attempts=%d",
		task.TaskID, parentID, taskKey, name, status, len(task.Attempts))

	if strings.TrimSpace(task.SpecHash) != "" {
		fmt.Fprintf(w, " spec_hash=%s", task.SpecHash)
	}

	fmt.Fprintln(w)

	for _, attempt := range task.Attempts {
		writeRunTaskAttempt(w, attempt)
	}
}

func writeRunTaskAttempt(w io.Writer, attempt runTaskAttemptRow) {
	cellID := strings.TrimSpace(attempt.CellID)
	if cellID == "" {
		cellID = "-"
	}

	status := strings.TrimSpace(attempt.Status)
	if status == "" {
		status = "-"
	}

	fmt.Fprintf(w, "  attempt=%d id=%s cell=%s status=%s event_sequence=%d",
		attempt.Attempt, attempt.AttemptID, cellID, status, attempt.EventSequence)

	if strings.TrimSpace(attempt.ExecutionID) != "" {
		fmt.Fprintf(w, " execution_id=%s", attempt.ExecutionID)
	}

	if strings.TrimSpace(attempt.ExecutionStatus) != "" {
		fmt.Fprintf(w, " execution_status=%s", attempt.ExecutionStatus)
	}

	if attempt.LeaseOwner != nil && strings.TrimSpace(*attempt.LeaseOwner) != "" {
		fmt.Fprintf(w, " lease_owner=%s", *attempt.LeaseOwner)
	}

	if attempt.LeaseUntil != nil {
		fmt.Fprintf(w, " lease_until=%s", time.Unix(*attempt.LeaseUntil, 0).UTC().Format(time.RFC3339))
	}

	if attempt.AcceptedAt != nil {
		fmt.Fprintf(w, " accepted_at=%s", *attempt.AcceptedAt)
	}

	if attempt.StartedAt != nil {
		fmt.Fprintf(w, " started_at=%s", *attempt.StartedAt)
	}

	if attempt.FinishedAt != nil {
		fmt.Fprintf(w, " finished_at=%s", *attempt.FinishedAt)
	}

	if attempt.LastObservedAt != nil {
		fmt.Fprintf(w, " last_observed_at=%s", formatUnixNano(*attempt.LastObservedAt))
	}

	fmt.Fprintln(w)
}

func getRunArtifacts(runID string, opts runArtifactsListOptions, w io.Writer) error {
	path := fmt.Sprintf("/api/v1/runs/%s/artifacts", url.PathEscape(runID))
	params := url.Values{}
	if opts.Limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", opts.Limit))
	}

	if opts.Cursor > 0 {
		params.Set("cursor", fmt.Sprintf("%d", opts.Cursor))
	}

	setTrimmedQueryParam(params, "task_id", opts.TaskID)
	setTrimmedQueryParam(params, "task_attempt_id", opts.TaskAttemptID)
	setTrimmedQueryParam(params, "execution_id", opts.ExecutionID)
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

	switch resp.StatusCode {
	case http.StatusOK:
		var result runArtifactsResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if outputIsJSON() {
			return writeJSON(w, result)
		}

		if len(result.Data) == 0 {
			fmt.Fprintln(w, "No artifacts found")
			return nil
		}

		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tTASK\tATTEMPT\tEXECUTION\tPATH\tCONTENT TYPE\tSIZE\tSHARD\tDIGEST")
		for _, artifact := range result.Data {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
				textOrDash(artifact.Name),
				textPtrOrDash(artifact.TaskID),
				textPtrOrDash(artifact.TaskAttemptID),
				textPtrOrDash(artifact.ExecutionID),
				textOrDash(artifact.Path),
				textOrDash(artifact.ContentType),
				artifact.SizeBytes,
				textOrDash(artifact.ArtifactShardID),
				textOrDash(artifact.BlobDigest),
			)
		}

		if err := tw.Flush(); err != nil {
			return err
		}

		if result.NextCursor != nil {
			fmt.Fprintf(w, "\nMore artifacts available. Continue with --cursor %d.\n", *result.NextCursor)
		}

		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", runID)
	case http.StatusForbidden:
		return fmt.Errorf("not authorized to read artifacts for run %q", runID)
	case http.StatusServiceUnavailable:
		return fmt.Errorf("artifact repository is not configured")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func setTrimmedQueryParam(params url.Values, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		params.Set(key, value)
	}
}

func downloadRunArtifact(runID, name, outputPath string, w io.Writer) error {
	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "" {
		return fmt.Errorf("--output is required (use --output - to write artifact bytes to stdout)")
	}

	if outputPath == "-" && outputIsJSON() {
		return fmt.Errorf("--format json cannot be used with --output -")
	}

	req, err := newAPIRequest(
		http.MethodGet,
		fmt.Sprintf("/api/v1/runs/%s/artifacts/%s/download", url.PathEscape(runID), url.PathEscape(name)),
		nil,
	)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "*/*")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		contentType := resp.Header.Get("Content-Type")
		written, err := copyArtifactDownload(resp.Body, outputPath, w)
		if err != nil {
			return err
		}

		if outputPath == "-" {
			return nil
		}

		result := runArtifactDownloadResult{
			Status:      "downloaded",
			RunID:       runID,
			Name:        name,
			OutputPath:  outputPath,
			ContentType: contentType,
			Bytes:       written,
		}

		if outputIsJSON() {
			return writeJSON(w, result)
		}

		fmt.Fprintf(w, "Downloaded artifact %s to %s (%d bytes).\n", name, outputPath, written)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("artifact %q for run %q not found", name, runID)
	case http.StatusBadGateway:
		return fmt.Errorf("artifact blob unavailable")
	case http.StatusServiceUnavailable:
		return fmt.Errorf("artifact repository is not configured")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func copyArtifactDownload(r io.Reader, outputPath string, stdout io.Writer) (int64, error) {
	if outputPath == "-" {
		return io.Copy(stdout, r)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}

	written, copyErr := io.Copy(file, r)
	closeErr := file.Close()
	if copyErr != nil {
		return written, fmt.Errorf("write artifact file: %w", copyErr)
	}

	if closeErr != nil {
		return written, fmt.Errorf("close artifact file: %w", closeErr)
	}

	return written, nil
}

func textOrDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}

	return value
}

func textPtrOrDash(value *string) string {
	if value == nil {
		return "-"
	}

	return textOrDash(*value)
}

func replayRun(sourceRunID, cellID, idempotencyKey string, w io.Writer) error {
	var body io.Reader
	if strings.TrimSpace(cellID) != "" {
		payload, err := json.Marshal(map[string]string{"cell_id": strings.TrimSpace(cellID)})
		if err != nil {
			return fmt.Errorf("failed to encode request body: %w", err)
		}
		body = bytes.NewReader(payload)
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/runs/%s/replay", sourceRunID), body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if strings.TrimSpace(idempotencyKey) != "" {
		req.Header.Set("Idempotency-Key", strings.TrimSpace(idempotencyKey))
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result runReplayResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if outputIsJSON() {
			return writeJSON(w, result)
		}

		fmt.Fprintf(w, "replay_of_run_id=%s\n", result.ReplayOfRunID)
		fmt.Fprintf(w, "run_id=%s\n", result.RunID)
		fmt.Fprintf(w, "run_index=%d\n", result.RunIndex)
		if strings.TrimSpace(result.JobID) != "" {
			fmt.Fprintf(w, "job_id=%s\n", result.JobID)
		}

		if strings.TrimSpace(result.CellID) != "" {
			fmt.Fprintf(w, "cell_id=%s\n", result.CellID)
		}

		return nil
	case http.StatusNotFound:
		return fmt.Errorf("run %q not found", sourceRunID)
	case http.StatusConflict:
		return fmt.Errorf("run %q cannot be replayed from its current status", sourceRunID)
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
	case http.StatusNoContent, http.StatusAccepted:
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
	runCLIError(listRuns(jobID, runListLimit, runListCursor, since, runListCellID, os.Stdout))
}

func listRuns(jobID string, limit, cursor int, since string, cellID string, w io.Writer) error {
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

	if strings.TrimSpace(cellID) != "" {
		params.Set("cell_id", strings.TrimSpace(cellID))
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

	fmt.Fprintf(w, "%-20s %-5s %-12s %-12s %-24s %-24s %-24s\n",
		"RUN ID", "INDEX", "CELL", "STATUS", "CREATED", "STARTED", "FINISHED")

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

		owningCell := strings.TrimSpace(r.OwningCell)
		if owningCell == "" {
			owningCell = "-"
		}

		fmt.Fprintf(w, "%-20s %-5d %-12s %-12s %-24s %-24s %-24s\n",
			r.RunID, r.RunIndex, owningCell, r.Status, created, started, finished)
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
  vectis-cli runs tasks run-123
  vectis-cli runs artifacts list run-123
  vectis-cli runs artifacts download run-123 coverage --output coverage.txt
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
	Short: "Show run status, audit metadata, and failure details",
	Long:  `Fetch a run by run-id and print operator-facing status, audit metadata, dispatch events, and failure fields.`,
	Args:  cobra.ExactArgs(1),
	Run:   runGetRun,
}

var runPayloadCmd = &cobra.Command{
	Use:   "payload [run-id]",
	Short: "Show the frozen execution payload for a run",
	Long:  `Fetch the immutable execution payload captured at first durable dispatch for one run. This is an operator-only audit surface.`,
	Args:  cobra.ExactArgs(1),
	Run:   runGetRunPayload,
}

var runTasksCmd = &cobra.Command{
	Use:   "tasks [run-id]",
	Short: "List task graph nodes and attempts for a run",
	Long:  `List the task graph nodes and task attempt state recorded for one run.`,
	Args:  cobra.ExactArgs(1),
	Run:   runGetRunTasks,
}

var runArtifactsCmd = &cobra.Command{
	Use:   "artifacts",
	Short: "List and download run artifacts",
	Long:  `List artifact manifests recorded for a run, or download one artifact by name.`,
	Run:   showCommandHelp,
}

var runArtifactsListCmd = &cobra.Command{
	Use:   "list [run-id]",
	Short: "List artifact manifests for a run",
	Long:  `List artifact manifests recorded for one run, including producer task identity, blob digest, size, content type, and storage shard.`,
	Args:  cobra.ExactArgs(1),
	Run:   runListRunArtifacts,
}

var runArtifactsDownloadCmd = &cobra.Command{
	Use:   "download [run-id] [artifact-name]",
	Short: "Download a run artifact",
	Long:  `Download one artifact blob by run id and artifact name. Pass --output - only when raw artifact bytes should be written to stdout.`,
	Args:  cobra.ExactArgs(2),
	Run:   runDownloadRunArtifact,
}

var runReplayCmd = &cobra.Command{
	Use:   "replay [run-id]",
	Short: "Create a new run from a previous run's captured definition",
	Long:  `Create a fresh queued run from the source run's captured job definition version. Replay creates a new run id and records replay lineage; it does not repair or mutate the source run.`,
	Args:  cobra.ExactArgs(1),
	Run:   runReplayRun,
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
Use --cell to filter to runs owned by one execution cell.
When more runs are available, the command prints a cursor; pass that value back with --cursor
to fetch the next page.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runListRuns,
}

func configureRunListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&runListJobID, "job", "", "Job ID to list runs for")
	cmd.Flags().IntVar(&runListLimit, "limit", 0, "Max runs to return (default 50)")
	cmd.Flags().IntVar(&runListCursor, "cursor", 0, "Continue listing after this result cursor")
	cmd.Flags().StringVar(&runListCellID, "cell", "", "Only list runs owned by this execution cell")
	cmd.Flags().String("since", "", "Only list runs created at or after this RFC3339 timestamp or YYYY-MM-DD date")
}

func configureRunTasksFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(&runTasksLimit, "limit", 0, "Max tasks to return (default 100)")
	cmd.Flags().IntVar(&runTasksCursor, "cursor", 0, "Continue listing after this result cursor")
}

func configureRunArtifactsListFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(&runArtifactsLimit, "limit", 0, "Max artifacts to return (default 50)")
	cmd.Flags().IntVar(&runArtifactsCursor, "cursor", 0, "Continue listing after this result cursor")
	cmd.Flags().StringVar(&runArtifactsTaskID, "task-id", "", "Only list artifacts produced by this task")
	cmd.Flags().StringVar(&runArtifactsAttemptID, "task-attempt-id", "", "Only list artifacts produced by this task attempt")
	cmd.Flags().StringVar(&runArtifactsExecID, "execution-id", "", "Only list artifacts produced by this execution")
}

func configureRunArtifactsDownloadFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&runArtifactOutput, "output", "o", "", "Output file path, or '-' to write raw bytes to stdout")
}

func configureRunReplayFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&runReplayCellID, "cell", "", "Replay into this execution cell instead of the source run's cell")
	cmd.Flags().StringVar(&runReplayIdemKey, "idempotency-key", "", "Idempotency key for safe replay request retries")
}

func configureForceFailFlags(cmd *cobra.Command) {
	cmd.Flags().String("reason", "", "Failure reason to record")
}

func configureRepairMarkFlags(cmd *cobra.Command) {
	cmd.Flags().String("reason", "", "Operator reason to record")
}
