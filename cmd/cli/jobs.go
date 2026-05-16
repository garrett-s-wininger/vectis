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
	jobvalidation "vectis/internal/job/validation"
)

type jobRunResult struct {
	JobID    string `json:"job_id,omitempty"`
	ID       string `json:"id,omitempty"`
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index,omitempty"`
}

func setIdempotencyHeader(req *http.Request, key string) {
	if key = strings.TrimSpace(key); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
}

func triggerJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("job-id is required"))
	}

	jobID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/jobs/trigger/%s", jobID), nil)
	if err != nil {
		runCLIError(fmt.Errorf("failed to create trigger request: %w", err))
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, triggerIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		runCLIError(fmt.Errorf("failed to trigger job: %w", err))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result jobRunResult

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			runCLIError(fmt.Errorf("failed to parse response: %w", err))
		}

		if result.RunID == "" {
			runCLIError(fmt.Errorf("response missing run_id"))
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				runCLIError(err)
			}
		} else if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, result))
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusNotFound:
		runCLIError(fmt.Errorf("job %q not found", jobID))
	case http.StatusServiceUnavailable:
		runCLIError(fmt.Errorf("queue service unavailable"))
	default:
		runCLIError(fmt.Errorf("unexpected status: %s", resp.Status))
	}
}

func runJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("path or - is required"))
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
		runCLIError(fmt.Errorf("failed to read job definition: %w", err))
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		runCLIError(fmt.Errorf("invalid job JSON: %w", err))
	}

	if job.GetRoot() == nil {
		runCLIError(fmt.Errorf("job must have a root node"))
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	if err != nil {
		runCLIError(err)
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, runIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		runCLIError(fmt.Errorf("failed to submit job: %w", err))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result jobRunResult

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			runCLIError(fmt.Errorf("failed to parse response: %w", err))
		}

		if result.RunID == "" {
			runCLIError(fmt.Errorf("response missing run_id"))
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				runCLIError(err)
			}
		} else if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, result))
		} else {
			fmt.Println(result.RunID)
		}
	case http.StatusUnsupportedMediaType:
		runCLIError(fmt.Errorf("content type must be application/json"))
	case http.StatusBadRequest:
		runCLIError(fmt.Errorf("invalid job definition"))
	case http.StatusServiceUnavailable:
		runCLIError(fmt.Errorf("queue service unavailable"))
	default:
		runCLIError(fmt.Errorf("unexpected status: %s", resp.Status))
	}
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
	if err := json.Unmarshal(edited, &job); err != nil {
		runCLIError(fmt.Errorf("invalid job JSON after edit: %w", err))
	}

	if job.GetRoot() == nil {
		runCLIError(fmt.Errorf("job must have a root node"))
	}

	if job.Id == nil || *job.Id != jobID {
		runCLIError(fmt.Errorf("job id mismatch (expected %q, got %v)", jobID, job.Id))
	}

	// NOTE(garrett): Always re-indent the stored job before updating.
	pretty, err = json.MarshalIndent(&job, "", "  ")
	if err != nil {
		runCLIError(fmt.Errorf("failed to normalize job JSON: %w", err))
	}
	pretty = append(pretty, '\n')

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
	cursor, _ := cmd.Flags().GetInt64("cursor")
	limit, _ := cmd.Flags().GetInt("limit")
	runCLIError(listJobNames(os.Stdout, quiet, cursor, limit))
}

func createJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		_ = cmd.Usage()
		runCLIError(fmt.Errorf("path or - is required"))
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
		runCLIError(fmt.Errorf("failed to read job definition: %w", err))
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		runCLIError(fmt.Errorf("invalid job JSON: %w", err))
	}

	if job.Id == nil || *job.Id == "" {
		runCLIError(fmt.Errorf("job definition must include an id field"))
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true}); err != nil {
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

func deleteJob(cmd *cobra.Command, args []string) {
	jobID := args[0]

	force, _ := cmd.Flags().GetBool("yes")
	if !force {
		runCLIError(fmt.Errorf("delete job %q requires --yes; this removes the definition and prevents future triggers", jobID))
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

func listJobNames(w io.Writer, quiet bool, cursor int64, limit int) error {
	path := "/api/v1/jobs"
	params := url.Values{}
	if cursor > 0 {
		params.Set("cursor", strconv.FormatInt(cursor, 10))
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
	Long: `Create stored jobs, trigger stored jobs, and submit one-off job definitions.

Common flows:
  vectis-cli jobs create build.json
  vectis-cli jobs trigger build-main --follow
  vectis-cli jobs run scratch.json`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var triggerCmd = &cobra.Command{
	Use:   "trigger [job-id]",
	Short: "Trigger a stored job",
	Long: `Trigger a stored job by its job-id. The job must exist in the database.
The API records the run and returns immediately (202 with run_id); enqueue to the queue happens in the background, so a down queue does not block this command.`,
	Args: cobra.ExactArgs(1),
	Run:  triggerJob,
}

var runCmd = &cobra.Command{
	Use:   "run [path|-]",
	Short: "Run a job definition once",
	Long: `Submit a job definition to run once (ephemeral). Path is a JSON file; use "-" to read from stdin.
On success prints the run ID. With --follow, streams logs for that run until it completes.`,
	Args: cobra.ExactArgs(1),
	Run:  runJob,
}

var editCmd = &cobra.Command{
	Use:   "edit [job-id]",
	Short: "Edit a stored job definition using $EDITOR",
	Long:  `Fetch a stored job definition, open it in your $EDITOR, and update the job if you save and exit successfully.`,
	Args:  cobra.ExactArgs(1),
	Run:   editJob,
}

var getCmd = &cobra.Command{
	Use:   "show [job-id]",
	Short: "Show a stored job definition",
	Long:  `Fetch a stored job definition by its job-id and print it as JSON.`,
	Args:  cobra.ExactArgs(1),
	Run:   getJobDefinition,
}

var createCmd = &cobra.Command{
	Use:   "create [path|-]",
	Short: "Store a job definition",
	Long:  `Store a job definition for later trigger, edit, and delete. Path is a JSON file; use "-" to read from stdin.`,
	Args:  cobra.ExactArgs(1),
	Run:   createJob,
}

var deleteCmd = &cobra.Command{
	Use:   "delete [job-id]",
	Short: "Delete a stored job",
	Long:  `Delete a stored job definition. The job must exist. Pass --yes to skip confirmation.`,
	Args:  cobra.ExactArgs(1),
	Run:   deleteJob,
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List stored jobs",
	Long:  `Fetch stored jobs and print a compact table. Use --quiet to print only job IDs.`,
	Args:  cobra.NoArgs,
	Run:   listJobs,
}

func configureJobTriggerFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringVar(&triggerIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe trigger retries")
}

func configureJobRunFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After submitting, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringVar(&runIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe ephemeral run retries")
}

func configureJobShowFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
}

func configureJobCreateFlags(cmd *cobra.Command) {
	cmd.Flags().String("namespace", "", "Namespace to store the job in (default: /)")
}

func configureJobDeleteFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("yes", false, "Skip confirmation prompt")
}

func configureJobListFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("quiet", "q", false, "Print only job IDs")
	cmd.Flags().Int64("cursor", 0, "Continue listing after this result cursor")
	cmd.Flags().Int("limit", 0, "Max jobs to return")
}
