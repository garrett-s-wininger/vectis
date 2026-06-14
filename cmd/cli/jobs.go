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
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"vectis/api/gen/go"
	"vectis/internal/action/actionconfig"
	jobdef "vectis/internal/job"
	jobvalidation "vectis/internal/job/validation"
)

type jobRunResult struct {
	JobID    string             `json:"job_id,omitempty"`
	ID       string             `json:"id,omitempty"`
	RunID    string             `json:"run_id,omitempty"`
	RunIndex int                `json:"run_index,omitempty"`
	Runs     []jobRunCellResult `json:"runs,omitempty"`
}

type jobRunCellResult struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
	CellID   string `json:"cell_id,omitempty"`
}

type jobListOptions struct {
	RepositoryID string
	Ref          string
	Path         string
	Cursor       string
	Limit        int
	Quiet        bool
}

type jobsSourceDefinitionWriteRequest struct {
	RepositoryID string          `json:"repository_id"`
	JobID        string          `json:"job_id,omitempty"`
	Ref          string          `json:"ref,omitempty"`
	Branch       string          `json:"branch,omitempty"`
	Path         string          `json:"path,omitempty"`
	Message      string          `json:"message,omitempty"`
	ExpectedHead string          `json:"expected_head,omitempty"`
	Job          json.RawMessage `json:"job"`
}

func setIdempotencyHeader(req *http.Request, key string) {
	if key = strings.TrimSpace(key); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
}

func stringFlagValue(cmd *cobra.Command, name string) string {
	if cmd == nil {
		return ""
	}

	value, err := cmd.Flags().GetString(name)
	if err != nil {
		return ""
	}

	return strings.TrimSpace(value)
}

func triggerJob(cmd *cobra.Command, args []string) {
	runCLIError(triggerJobWithOutput(cmd, args, os.Stdout))
}

func triggerJobWithOutput(cmd *cobra.Command, args []string, out io.Writer) error {
	if len(args) < 1 {
		_ = cmd.Usage()
		return fmt.Errorf("job-id is required")
	}

	jobID := args[0]
	if repositoryID := stringFlagValue(cmd, "repository"); repositoryID != "" {
		return triggerSourceJobFromJobsFacadeWithOutput(cmd, out, repositoryID, jobID)
	}

	body, err := triggerJobRequestBody(triggerCellIDs)
	if err != nil {
		return err
	}

	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/jobs/trigger/%s", jobID), body)
	if err != nil {
		return fmt.Errorf("failed to create trigger request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, triggerIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to trigger job: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result jobRunResult

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		return writeTriggerJobResult(cmd, out, result)
	case http.StatusNotFound:
		return fmt.Errorf("job %q not found", jobID)
	case http.StatusServiceUnavailable:
		return fmt.Errorf("queue service unavailable")
	default:
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func triggerJobRequestBody(rawCellIDs []string) (io.Reader, error) {
	cellIDs, err := normalizeTriggerCellIDs(rawCellIDs)
	if err != nil {
		return nil, err
	}

	if len(cellIDs) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(struct {
		CellIDs []string `json:"cell_ids"`
	}{CellIDs: cellIDs})
	if err != nil {
		return nil, fmt.Errorf("failed to encode trigger options: %w", err)
	}

	return bytes.NewReader(body), nil
}

func singleSourceTriggerCellIDFromCells(rawCellIDs []string) (string, error) {
	cellIDs, err := normalizeTriggerCellIDs(rawCellIDs)
	if err != nil {
		return "", err
	}

	if len(cellIDs) > 1 {
		return "", fmt.Errorf("--cell may target only one execution cell with --repository")
	}

	if len(cellIDs) == 0 {
		return "", nil
	}

	return cellIDs[0], nil
}

func normalizeTriggerCellIDs(rawCellIDs []string) ([]string, error) {
	out := make([]string, 0, len(rawCellIDs))
	seen := make(map[string]struct{}, len(rawCellIDs))
	for _, raw := range rawCellIDs {
		for value := range strings.SplitSeq(raw, ",") {
			cellID := strings.TrimSpace(value)
			if cellID == "" {
				return nil, fmt.Errorf("--cell cannot be empty")
			}

			if _, ok := seen[cellID]; ok {
				continue
			}

			seen[cellID] = struct{}{}
			out = append(out, cellID)
		}
	}

	return out, nil
}

func writeTriggerJobResult(cmd *cobra.Command, out io.Writer, result jobRunResult) error {
	if len(result.Runs) > 0 {
		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			return fmt.Errorf("--follow is only supported when a trigger creates one run")
		}

		if outputIsJSON() {
			return writeJSON(out, result)
		}

		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "CELL\tRUN ID\tINDEX")
		for _, run := range result.Runs {
			cellID := run.CellID
			if cellID == "" {
				cellID = "-"
			}

			fmt.Fprintf(tw, "%s\t%s\t%d\n", cellID, run.RunID, run.RunIndex)
		}

		return tw.Flush()
	}

	if result.RunID == "" {
		return fmt.Errorf("response missing run_id")
	}

	follow, _ := cmd.Flags().GetBool("follow")
	if follow {
		return runLogStream(result.RunID, false, false)
	}

	if outputIsJSON() {
		return writeJSON(out, result)
	}

	_, err := fmt.Fprintln(out, result.RunID)
	return err
}

func runJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("path or - is required"))
	}

	result, err := submitJobDefinitionSource(args[0], runCellID, runIdemKey, os.Stdin)
	runCLIError(err)

	if result.RunID == "" {
		runCLIError(fmt.Errorf("response missing run_id"))
	}

	follow, _ := cmd.Flags().GetBool("follow")
	if follow {
		runCLIError(runLogStream(result.RunID, false, false))
	} else if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, result))
	} else {
		fmt.Println(result.RunID)
	}
}

func readJobDefinitionSource(source string, stdin io.Reader) ([]byte, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, fmt.Errorf("path or - is required")
	}

	var body []byte
	var err error
	if source == "-" {
		if stdin == nil {
			return nil, fmt.Errorf("stdin is not available for job definition")
		}

		body, err = io.ReadAll(stdin)
	} else {
		body, err = os.ReadFile(source)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read job definition: %w", err)
	}

	return body, nil
}

func validateRunnableJobDefinition(body []byte) error {
	var job api.Job
	if err := jobdef.DecodeDefinitionJSON(body, &job); err != nil {
		return fmt.Errorf("invalid job JSON: %w", err)
	}

	if job.GetRoot() == nil {
		return fmt.Errorf("job must have a root node")
	}

	return nil
}

func submitJobDefinitionSource(source, cellID, idempotencyKey string, stdin io.Reader) (jobRunResult, error) {
	body, err := readJobDefinitionSource(source, stdin)
	if err != nil {
		return jobRunResult{}, err
	}

	return submitJobDefinitionBody(body, cellID, idempotencyKey)
}

func submitJobDefinitionBody(body []byte, cellID, idempotencyKey string) (jobRunResult, error) {
	if err := validateRunnableJobDefinition(body); err != nil {
		return jobRunResult{}, err
	}

	requestBody, err := runJobRequestBody(body, cellID)
	if err != nil {
		return jobRunResult{}, err
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs/run", requestBody)
	if err != nil {
		return jobRunResult{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, idempotencyKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		return jobRunResult{}, fmt.Errorf("failed to submit job: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result jobRunResult

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return jobRunResult{}, fmt.Errorf("failed to parse response: %w", err)
		}

		if result.RunID == "" {
			return jobRunResult{}, fmt.Errorf("response missing run_id")
		}

		return result, nil
	case http.StatusUnsupportedMediaType:
		return jobRunResult{}, fmt.Errorf("content type must be application/json")
	case http.StatusBadRequest:
		return jobRunResult{}, fmt.Errorf("invalid job definition")
	case http.StatusServiceUnavailable:
		return jobRunResult{}, fmt.Errorf("queue service unavailable")
	default:
		return jobRunResult{}, fmt.Errorf("unexpected status: %s", resp.Status)
	}
}

func runJobRequestBody(jobBody []byte, cellID string) (io.Reader, error) {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return bytes.NewReader(jobBody), nil
	}

	body, err := json.Marshal(struct {
		Job    json.RawMessage `json:"job"`
		CellID string          `json:"cell_id"`
	}{
		Job:    json.RawMessage(jobBody),
		CellID: cellID,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to encode run options: %w", err)
	}

	return bytes.NewReader(body), nil
}

func fetchJobDefinitionBody(jobID string) ([]byte, int, error) {
	req, err := newAPIRequest(http.MethodGet, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create job definition request: %w", err)
	}

	resp, err := doAPIRequest(req)
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

func fetchSourceJobDefinitionBodyFromJobsFacade(cmd *cobra.Command, repositoryID, jobID string) ([]byte, int, error) {
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", repositoryID)
	setTrimmedQueryParam(params, "ref", stringFlagValue(cmd, "ref"))
	setTrimmedQueryParam(params, "path", stringFlagValue(cmd, "path"))

	req, err := newAPIRequest(http.MethodGet, appendQueryParams("/api/v1/jobs/"+url.PathEscape(jobID), params), nil)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create source job definition request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to fetch source job definition: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to read source job definition: %w", readErr)
	}

	if resp.StatusCode != http.StatusOK {
		return body, resp.StatusCode, nil
	}

	var result sourceRepositoryJobDefinitionResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("failed to parse source job definition response: %w", err)
	}

	return result.Definition, resp.StatusCode, nil
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
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("job-id is required"))
	}

	jobID := args[0]
	repositoryID := stringFlagValue(cmd, "repository")
	var body []byte
	var statusCode int
	var err error
	if repositoryID != "" {
		body, statusCode, err = fetchSourceJobDefinitionBodyFromJobsFacade(cmd, repositoryID, jobID)
	} else {
		body, statusCode, err = fetchJobDefinitionBody(jobID)
	}
	if err != nil {
		runCLIError(err)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		runCLIError(fmt.Errorf("job %q not found", jobID))
	default:
		runCLIError(fmt.Errorf("unexpected status fetching job: %d", statusCode))
	}

	pretty := formatJobDefinitionBody(body, true)
	tempFile, err := os.CreateTemp("", "vectis-job-*.json")
	if err != nil {
		runCLIError(fmt.Errorf("failed to create temp file: %w", err))
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(pretty); err != nil {
		tempFile.Close()
		runCLIError(fmt.Errorf("failed to write job definition to temp file: %w", err))
	}

	if err := tempFile.Close(); err != nil {
		runCLIError(fmt.Errorf("failed to close temp file: %w", err))
	}

	editorEnv := os.Getenv("EDITOR")
	if editorEnv == "" {
		editorEnv = "vi"
	}

	editorParts := strings.Fields(editorEnv)
	if len(editorParts) == 0 {
		runCLIError(fmt.Errorf("EDITOR is empty after parsing"))
	}

	editorName := editorParts[0]
	editorArgs := append(append([]string{}, editorParts[1:]...), tempPath)

	// NOTE(garrett): editorName/args come from EDITOR (user-controlled editor) plus our temp file path.
	editCmd := exec.Command(editorName, editorArgs...) //#nosec G204
	editCmd.Stdin = os.Stdin
	editCmd.Stdout = os.Stdout
	editCmd.Stderr = os.Stderr

	if err := editCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 0 {
				os.Exit(exitErr.ExitCode())
			}
		}

		runCLIError(fmt.Errorf("editor failed: %w", err))
	}

	edited, err := os.ReadFile(tempPath)
	if err != nil {
		runCLIError(fmt.Errorf("failed to read edited job definition: %w", err))
	}

	var job api.Job
	if err := jobdef.DecodeDefinitionJSON(edited, &job); err != nil {
		runCLIError(fmt.Errorf("invalid job JSON after edit: %w", err))
	}

	if job.GetRoot() == nil {
		runCLIError(fmt.Errorf("job must have a root node"))
	}

	if repositoryID == "" && (job.Id == nil || *job.Id != jobID) {
		runCLIError(fmt.Errorf("job id mismatch (expected %q, got %v)", jobID, job.Id))
	}
	if repositoryID != "" && strings.TrimSpace(job.GetId()) != "" && strings.TrimSpace(job.GetId()) != jobID {
		runCLIError(fmt.Errorf("job id mismatch (expected %q, got %v)", jobID, job.Id))
	}

	// NOTE(garrett): Always re-indent the job before updating.
	pretty, err = json.MarshalIndent(&job, "", "  ")
	if err != nil {
		runCLIError(fmt.Errorf("failed to normalize job JSON: %w", err))
	}
	pretty = append(pretty, '\n')

	if repositoryID != "" {
		runCLIError(updateSourceJobFromJobsFacadeWithOutput(cmd, os.Stdout, repositoryID, jobID, pretty))
		return
	}

	req, err := newAPIRequest(http.MethodPut, fmt.Sprintf("/api/v1/jobs/%s", jobID), bytes.NewReader(pretty))
	if err != nil {
		runCLIError(fmt.Errorf("failed to create update request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")

	updateResp, err := doAPIRequest(req)
	if err != nil {
		runCLIError(fmt.Errorf("failed to update job: %w", err))
	}
	defer updateResp.Body.Close()

	switch updateResp.StatusCode {
	case http.StatusNoContent:
		runCLIError(writeAction(os.Stdout, "Job updated successfully.", cliActionResult{Status: "updated"}))
	case http.StatusBadRequest:
		runCLIError(fmt.Errorf("invalid job definition or id mismatch"))
	case http.StatusUnsupportedMediaType:
		runCLIError(fmt.Errorf("content type must be application/json"))
	case http.StatusNotFound:
		runCLIError(fmt.Errorf("job %q not found", jobID))
	default:
		runCLIError(fmt.Errorf("unexpected status updating job: %s", updateResp.Status))
	}
}

func getJobDefinition(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("job-id is required"))
	}

	jobID := args[0]
	if repositoryID := stringFlagValue(cmd, "repository"); repositoryID != "" {
		runCLIError(showSourceJobFromJobsFacadeWithOutput(cmd, os.Stdout, repositoryID, jobID))
		return
	}

	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		runCLIError(err)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		runCLIError(fmt.Errorf("job %q not found", jobID))
	default:
		runCLIError(fmt.Errorf("unexpected status fetching job: %d", statusCode))
	}

	raw, _ := cmd.Flags().GetBool("raw")
	out := formatJobDefinitionBody(body, !raw)
	fmt.Print(string(out))
}

func listJobs(cmd *cobra.Command, args []string) {
	quiet, _ := cmd.Flags().GetBool("quiet")
	cursor, _ := cmd.Flags().GetString("cursor")
	limit, _ := cmd.Flags().GetInt("limit")
	runCLIError(listJobsWithOutput(os.Stdout, jobListOptions{
		RepositoryID: stringFlagValue(cmd, "repository"),
		Ref:          stringFlagValue(cmd, "ref"),
		Path:         stringFlagValue(cmd, "path"),
		Cursor:       cursor,
		Limit:        limit,
		Quiet:        quiet,
	}))
}

func createJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("path or - is required"))
	}

	body, err := readJobDefinitionSource(args[0], os.Stdin)
	if err != nil {
		runCLIError(err)
	}

	var job api.Job
	if err := jobdef.DecodeDefinitionJSON(body, &job); err != nil {
		runCLIError(fmt.Errorf("invalid job JSON: %w", err))
	}

	actionResolver, err := actionconfig.Resolver()
	if err != nil {
		runCLIError(fmt.Errorf("invalid action registry config: %w", err))
	}

	repositoryID := stringFlagValue(cmd, "repository")
	if repositoryID != "" {
		if namespace := stringFlagValue(cmd, "namespace"); namespace != "" {
			runCLIError(fmt.Errorf("--namespace cannot be used with --repository"))
		}

		if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{Resolver: actionResolver}); err != nil {
			runCLIError(fmt.Errorf("invalid job definition: %w", err))
		}

		jobID := stringFlagValue(cmd, "job-id")
		if jobID == "" {
			jobID = strings.TrimSpace(job.GetId())
		}
		if jobID == "" {
			runCLIError(fmt.Errorf("source job creation requires --job-id or an id field in the job definition"))
		}

		runCLIError(createSourceJobFromJobsFacadeWithOutput(cmd, os.Stdout, repositoryID, jobID, body))
		return
	}

	if job.Id == nil || *job.Id == "" {
		runCLIError(fmt.Errorf("job definition must include an id field"))
	}

	if jobID := stringFlagValue(cmd, "job-id"); jobID != "" {
		runCLIError(fmt.Errorf("--job-id requires --repository"))
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true, Resolver: actionResolver}); err != nil {
		runCLIError(fmt.Errorf("invalid job definition: %w", err))
	}

	namespace, _ := cmd.Flags().GetString("namespace")

	payload, err := json.Marshal(struct {
		Namespace string          `json:"namespace"`
		Job       json.RawMessage `json:"job"`
	}{
		Namespace: namespace,
		Job:       body,
	})

	if err != nil {
		runCLIError(fmt.Errorf("failed to encode request: %w", err))
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		runCLIError(err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		runCLIError(fmt.Errorf("request failed: %w", err))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, map[string]string{"status": "created", "job_id": *job.Id}))
		} else {
			fmt.Printf("Job %q stored.\n", *job.Id)
		}
	case http.StatusConflict:
		runCLIError(fmt.Errorf("job %q already exists", *job.Id))
	case http.StatusBadRequest:
		runCLIError(fmt.Errorf("invalid job definition"))
	case http.StatusUnsupportedMediaType:
		runCLIError(fmt.Errorf("content type must be application/json"))
	case http.StatusServiceUnavailable:
		runCLIError(fmt.Errorf("database unavailable"))
	default:
		runCLIError(fmt.Errorf("unexpected status: %s", resp.Status))
	}
}

func createSourceJobFromJobsFacadeWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string, definition []byte) error {
	payload, err := jobsSourceDefinitionWritePayload(cmd, repositoryID, jobID, definition)
	if err != nil {
		return err
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create source job request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryJobDefinitionResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source job create response: %w", err)
		}
		return writeJobsSourceAuthoringResult(out, result, "stored")
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source job definition")
	case http.StatusConflict:
		return fmt.Errorf("source job definition write conflicted or source authoring is unavailable")
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("source job definition is too large")
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status creating source job: %s", resp.Status)
	}
}

func updateSourceJobFromJobsFacadeWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string, definition []byte) error {
	payload, err := jobsSourceDefinitionWritePayload(cmd, repositoryID, jobID, definition)
	if err != nil {
		return err
	}

	req, err := newAPIRequest(http.MethodPut, "/api/v1/jobs/"+url.PathEscape(jobID), bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create source job update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to update source job: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryJobDefinitionResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source job update response: %w", err)
		}
		return writeJobsSourceAuthoringResult(out, result, "updated")
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source job definition or id mismatch")
	case http.StatusConflict:
		return fmt.Errorf("source job definition write conflicted or source authoring is unavailable")
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or job %q not found", repositoryID, jobID)
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("source job definition is too large")
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status updating source job: %s", resp.Status)
	}
}

func jobsSourceDefinitionWritePayload(cmd *cobra.Command, repositoryID, jobID string, definition []byte) ([]byte, error) {
	payload, err := json.Marshal(jobsSourceDefinitionWriteRequest{
		RepositoryID: strings.TrimSpace(repositoryID),
		JobID:        strings.TrimSpace(jobID),
		Ref:          stringFlagValue(cmd, "ref"),
		Branch:       stringFlagValue(cmd, "branch"),
		Path:         stringFlagValue(cmd, "path"),
		Message:      stringFlagValue(cmd, "message"),
		ExpectedHead: stringFlagValue(cmd, "expected-head"),
		Job:          json.RawMessage(bytes.TrimSpace(definition)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode source job definition request: %w", err)
	}

	return payload, nil
}

func writeJobsSourceAuthoringResult(out io.Writer, result sourceRepositoryJobDefinitionResult, action string) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	fmt.Fprintf(out, "Job %q %s in source.\n", result.JobID, action)
	fmt.Fprintf(out, "commit=%s\n", result.Source.ResolvedCommit)
	fmt.Fprintf(out, "path=%s\n", result.Source.Path)
	if strings.TrimSpace(result.Source.BlobSHA) != "" {
		fmt.Fprintf(out, "blob_sha=%s\n", result.Source.BlobSHA)
	}
	fmt.Fprintf(out, "definition_hash=%s\n", result.DefinitionHash)
	if strings.TrimSpace(result.Source.RequestedRef) != "" {
		fmt.Fprintf(out, "requested_ref=%s\n", result.Source.RequestedRef)
	}

	return nil
}

func deleteJob(cmd *cobra.Command, args []string) {
	jobID := args[0]

	force, _ := cmd.Flags().GetBool("yes")
	if !force {
		runCLIError(fmt.Errorf("delete job %q requires --yes; this removes the definition and prevents future triggers", jobID))
	}

	if repositoryID := stringFlagValue(cmd, "repository"); repositoryID != "" {
		runCLIError(deleteSourceJobFromJobsFacadeWithOutput(cmd, os.Stdout, repositoryID, jobID))
		return
	}

	req, err := newAPIRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		runCLIError(err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		runCLIError(fmt.Errorf("request failed: %w", err))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, map[string]string{"status": "deleted", "job_id": jobID}))
		} else {
			fmt.Printf("Job %q deleted.\n", jobID)
		}
	case http.StatusNotFound:
		runCLIError(fmt.Errorf("job %q not found", jobID))
	default:
		runCLIError(fmt.Errorf("unexpected status: %s", resp.Status))
	}
}

func deleteSourceJobFromJobsFacadeWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", repositoryID)
	setTrimmedQueryParam(params, "ref", stringFlagValue(cmd, "ref"))
	setTrimmedQueryParam(params, "branch", stringFlagValue(cmd, "branch"))
	setTrimmedQueryParam(params, "path", stringFlagValue(cmd, "path"))
	setTrimmedQueryParam(params, "message", stringFlagValue(cmd, "message"))
	setTrimmedQueryParam(params, "expected_head", stringFlagValue(cmd, "expected-head"))

	req, err := newAPIRequest(http.MethodDelete, appendQueryParams("/api/v1/jobs/"+url.PathEscape(jobID), params), nil)
	if err != nil {
		return fmt.Errorf("failed to create source job delete request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		commit := strings.TrimSpace(resp.Header.Get("X-Vectis-Source-Commit"))
		if outputIsJSON() {
			return writeJSON(out, map[string]string{"status": "deleted", "job_id": jobID, "repository_id": repositoryID, "commit": commit})
		}
		if commit != "" {
			_, err = fmt.Fprintf(out, "Job %q deleted from source.\ncommit=%s\n", jobID, commit)
			return err
		}

		_, err = fmt.Fprintf(out, "Job %q deleted from source.\n", jobID)
		return err
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source job delete request")
	case http.StatusConflict:
		return fmt.Errorf("source job delete conflicted or source authoring is unavailable")
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or job %q not found", repositoryID, jobID)
	default:
		return fmt.Errorf("unexpected status deleting source job: %s", resp.Status)
	}
}

func listJobsWithOutput(w io.Writer, opts jobListOptions) error {
	if strings.TrimSpace(opts.RepositoryID) != "" {
		return listSourceJobsFromJobsFacadeWithOutput(w, opts)
	}

	return listJobNames(w, opts.Quiet, opts.Cursor, opts.Limit)
}

func listJobNames(w io.Writer, quiet bool, cursor string, limit int) error {
	path := "/api/v1/jobs"
	params := url.Values{}
	if strings.TrimSpace(cursor) != "" {
		cursorValue, err := strconv.ParseInt(strings.TrimSpace(cursor), 10, 64)
		if err != nil || cursorValue < 0 {
			return fmt.Errorf("--cursor must be a non-negative integer when listing stored jobs")
		}

		if cursorValue > 0 {
			params.Set("cursor", strconv.FormatInt(cursorValue, 10))
		}
	}

	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}

	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create list jobs request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list jobs: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status listing jobs: %s", resp.Status)
	}

	type jobListItem struct {
		Name       string          `json:"name"`
		Namespace  string          `json:"namespace,omitempty"`
		Definition json.RawMessage `json:"definition,omitempty"`
	}

	var jobsResp struct {
		Data       []jobListItem `json:"data"`
		NextCursor *int64        `json:"next_cursor,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&jobsResp); err != nil {
		return fmt.Errorf("failed to parse jobs response: %w", err)
	}

	jobs := make([]jobListItem, 0, len(jobsResp.Data))
	for _, job := range jobsResp.Data {
		if job.Name != "" {
			jobs = append(jobs, job)
		}
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].Name < jobs[j].Name
	})

	if outputIsJSON() {
		jobsResp.Data = jobs
		return writeJSON(w, jobsResp)
	}

	if quiet {
		for _, job := range jobs {
			fmt.Fprintln(w, job.Name)
		}

		return nil
	}

	if len(jobs) == 0 {
		fmt.Fprintln(w, "No jobs found")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tNAMESPACE")
	for _, job := range jobs {
		namespace := job.Namespace
		if namespace == "" {
			namespace = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\n", job.Name, namespace)
	}
	_ = tw.Flush()

	if jobsResp.NextCursor != nil {
		fmt.Fprintf(w, "\nMore jobs available. Continue with --cursor %d.\n", *jobsResp.NextCursor)
	}

	return nil
}

var jobsCmd = &cobra.Command{
	Use:   "jobs",
	Short: "Create, inspect, trigger, and run jobs",
	Long: `Create, inspect, trigger, and run reusable jobs.

Common flows:
  vectis-cli jobs create build.json
  vectis-cli jobs create build.json --repository vectis --branch main
  vectis-cli jobs trigger build-main --follow
  vectis-cli jobs trigger build-main --repository vectis --ref main --follow
  vectis-cli jobs run scratch.json`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a reusable job",
	Long: `Trigger a stored job by its job-id, or pass --repository to trigger a source-backed job through the jobs API facade.
The API records the run and returns immediately (202 with run_id); enqueue to the queue happens in the background, so a down queue does not block this command.
Use --cell repeatedly to fan out a stored job into named execution cells. Source-backed triggers accept at most one --cell.`,
	Args: cobra.ExactArgs(1),
	Run:  triggerJob,
}

var runCmd = &cobra.Command{
	Use:   "run [path|-]",
	Short: "Run a job definition once",
	Long: `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin.
On success prints the run ID. With --follow, streams logs for that run until it completes.
Use --cell to route the one-off run into a named execution cell.`,
	Args: cobra.ExactArgs(1),
	Run:  runJob,
}

var editCmd = &cobra.Command{
	Use:   "edit [job-id]",
	Short: "Edit a job definition using $EDITOR",
	Long:  `Fetch a stored job definition, or pass --repository to edit a source-backed definition, then update it if you save and exit successfully.`,
	Args:  cobra.ExactArgs(1),
	Run:   editJob,
}

var getCmd = &cobra.Command{
	Use:   "show [job-id]",
	Short: "Show a job definition",
	Long:  `Fetch a stored job definition by job-id, or pass --repository to resolve a source-backed job definition by ref and path.`,
	Args:  cobra.ExactArgs(1),
	Run:   getJobDefinition,
}

var createCmd = &cobra.Command{
	Use:   "create [path|-]",
	Short: "Create a reusable job definition",
	Long:  `Create a reusable job definition for later trigger, edit, and delete. Pass --repository to commit the definition through source authoring. Path is a JSON file; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   createJob,
}

var deleteCmd = &cobra.Command{
	Use:   "delete [job-id]",
	Short: "Delete a reusable job definition",
	Long:  `Delete a stored job definition, or pass --repository to commit a source definition deletion. The job must exist. Pass --yes to skip confirmation.`,
	Args:  cobra.ExactArgs(1),
	Run:   deleteJob,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List reusable jobs",
	Long:  `Fetch stored jobs by default, or pass --repository to list source-backed jobs discovered from a repository. Use --quiet to print only job IDs.`,
	Args:  cobra.NoArgs,
	Run:   listJobs,
}

func configureJobTriggerFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringArrayVar(&triggerCellIDs, "cell", nil, "Target execution cell; may be repeated")
	cmd.Flags().StringVar(&triggerIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe trigger retries")
	cmd.Flags().String("repository", "", "Source repository ID for a source-backed job trigger")
	cmd.Flags().String("ref", "", "Git ref for source-backed job trigger (default: repository default_ref or HEAD)")
	cmd.Flags().String("path", "", "Definition file path override for source-backed job trigger")
}

func configureJobRunFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringVar(&runCellID, "cell", "", "Target execution cell")
	cmd.Flags().StringVar(&runIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe ephemeral run retries")
}

func configureJobShowFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
	cmd.Flags().String("repository", "", "Source repository ID for a source-backed job definition")
	cmd.Flags().String("ref", "", "Git ref for source-backed job definition (default: repository default_ref or HEAD)")
	cmd.Flags().String("path", "", "Definition file path override for a source-backed job definition")
}

func configureJobCreateFlags(cmd *cobra.Command) {
	cmd.Flags().String("namespace", "", "Namespace to store the job in (default: /)")
	cmd.Flags().String("repository", "", "Source repository ID for source-backed job creation")
	cmd.Flags().String("job-id", "", "Source job ID when the definition omits id")
	cmd.Flags().String("ref", "", "Git ref to use as the write base (default: repository default_ref or HEAD)")
	cmd.Flags().String("branch", "", "Local branch to update instead of --ref/default")
	cmd.Flags().String("path", "", "Definition file path override")
	cmd.Flags().String("message", "", "Commit message for local source authoring")
	cmd.Flags().String("expected-head", "", "Require the target branch to still point at this commit")
}

func configureJobEditFlags(cmd *cobra.Command) {
	cmd.Flags().String("repository", "", "Source repository ID for source-backed job editing")
	cmd.Flags().String("ref", "", "Git ref to read and use as the write base (default: repository default_ref or HEAD)")
	cmd.Flags().String("branch", "", "Local branch to update instead of --ref/default")
	cmd.Flags().String("path", "", "Definition file path override")
	cmd.Flags().String("message", "", "Commit message for local source authoring")
	cmd.Flags().String("expected-head", "", "Require the target branch to still point at this commit")
}

func configureJobDeleteFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("yes", false, "Skip confirmation prompt")
	cmd.Flags().String("repository", "", "Source repository ID for source-backed job deletion")
	cmd.Flags().String("ref", "", "Git ref to use as the delete base (default: repository default_ref or HEAD)")
	cmd.Flags().String("branch", "", "Local branch to update instead of --ref/default")
	cmd.Flags().String("path", "", "Definition file path override")
	cmd.Flags().String("message", "", "Commit message for local source authoring")
	cmd.Flags().String("expected-head", "", "Require the target branch to still point at this commit")
}

func configureJobListFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("quiet", "q", false, "Print only job IDs")
	cmd.Flags().String("cursor", "", "Continue listing after this result cursor")
	cmd.Flags().Int("limit", 0, "Max jobs to return")
	cmd.Flags().String("repository", "", "Source repository ID for source-backed jobs")
	cmd.Flags().String("ref", "", "Git ref for source-backed jobs (default: repository default_ref or HEAD)")
	cmd.Flags().String("path", "", "Definition directory path for source-backed jobs (default: .vectis/jobs)")
}
