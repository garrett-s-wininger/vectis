package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"vectis/api/gen/go"
	jobvalidation "vectis/internal/job/validation"
)

func setIdempotencyHeader(req *http.Request, key string) {
	if key = strings.TrimSpace(key); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
}

func triggerJob(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	req, err := newAPIRequest(http.MethodPost, fmt.Sprintf("/api/v1/jobs/trigger/%s", jobID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create trigger request: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, triggerIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to trigger job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			JobID    string `json:"job_id"`
			RunID    string `json:"run_id"`
			RunIndex int    `json:"run_index"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
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

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs/run", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, runIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to submit job: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result struct {
			ID    string `json:"id"`
			RunID string `json:"run_id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to parse response: %v\n", err)
			os.Exit(1)
		}

		if result.RunID == "" {
			fmt.Fprintln(os.Stderr, "Error: response missing run_id")
			os.Exit(1)
		}

		follow, _ := cmd.Flags().GetBool("follow")
		if follow {
			if err := runLogStream(result.RunID, false, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
		} else {
			fmt.Println(result.RunID)
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
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	pretty := formatJobDefinitionBody(body, true)
	tempFile, err := os.CreateTemp("", "vectis-job-*.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create temp file: %v\n", err)
		os.Exit(1)
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)

	if _, err := tempFile.Write(pretty); err != nil {
		tempFile.Close()
		fmt.Fprintf(os.Stderr, "Error: failed to write job definition to temp file: %v\n", err)
		os.Exit(1)
	}

	if err := tempFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to close temp file: %v\n", err)
		os.Exit(1)
	}

	editorEnv := os.Getenv("EDITOR")
	if editorEnv == "" {
		editorEnv = "vi"
	}

	editorParts := strings.Fields(editorEnv)
	if len(editorParts) == 0 {
		fmt.Fprintln(os.Stderr, "Error: EDITOR is empty after parsing")
		os.Exit(1)
	}

	editorName := editorParts[0]
	editorArgs := append(append([]string{}, editorParts[1:]...), tempPath)

	editCmd := exec.Command(editorName, editorArgs...)
	editCmd.Stdin = os.Stdin
	editCmd.Stdout = os.Stdout
	editCmd.Stderr = os.Stderr

	if err := editCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 0 {
				os.Exit(exitErr.ExitCode())
			}
		}

		fmt.Fprintf(os.Stderr, "Error: editor failed: %v\n", err)
		os.Exit(1)
	}

	edited, err := os.ReadFile(tempPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to read edited job definition: %v\n", err)
		os.Exit(1)
	}

	var job api.Job
	if err := json.Unmarshal(edited, &job); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job JSON after edit: %v\n", err)
		os.Exit(1)
	}

	if job.GetRoot() == nil {
		fmt.Fprintln(os.Stderr, "Error: job must have a root node")
		os.Exit(1)
	}

	if job.Id == nil || *job.Id != jobID {
		fmt.Fprintf(os.Stderr, "Error: job id mismatch (expected %q, got %v)\n", jobID, job.Id)
		os.Exit(1)
	}

	// NOTE(garrett): Always re-indent the stored job before updating.
	pretty, err = json.MarshalIndent(&job, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to normalize job JSON: %v\n", err)
		os.Exit(1)
	}
	pretty = append(pretty, '\n')

	req, err := newAPIRequest(http.MethodPut, fmt.Sprintf("/api/v1/jobs/%s", jobID), bytes.NewReader(pretty))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create update request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	updateResp, err := doAPIRequest(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to update job: %v\n", err)
		os.Exit(1)
	}
	defer updateResp.Body.Close()

	switch updateResp.StatusCode {
	case http.StatusNoContent:
		fmt.Println("Job updated successfully.")
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition or id mismatch")
		os.Exit(1)
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status updating job: %s\n", updateResp.Status)
		os.Exit(1)
	}
}

func getJobDefinition(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "Error: job-id is required")
		cmd.Usage()
		os.Exit(1)
	}

	jobID := args[0]
	body, statusCode, err := fetchJobDefinitionBody(jobID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch statusCode {
	case http.StatusOK:
		// NOTE(garrett): Continue
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job '%s' not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status fetching job: %d\n", statusCode)
		os.Exit(1)
	}

	raw, _ := cmd.Flags().GetBool("raw")
	out := formatJobDefinitionBody(body, !raw)
	fmt.Print(string(out))
}

func listJobs(cmd *cobra.Command, args []string) {
	if err := listJobNames(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func createJob(cmd *cobra.Command, args []string) {
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

	if job.Id == nil || *job.Id == "" {
		fmt.Fprintln(os.Stderr, "Error: job definition must include an id field")
		os.Exit(1)
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid job definition: %v\n", err)
		os.Exit(1)
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
		fmt.Fprintf(os.Stderr, "Error: failed to encode request: %v\n", err)
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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
	case http.StatusCreated:
		fmt.Printf("Job %q stored.\n", *job.Id)
	case http.StatusConflict:
		fmt.Fprintf(os.Stderr, "Error: job %q already exists\n", *job.Id)
		os.Exit(1)
	case http.StatusBadRequest:
		fmt.Fprintln(os.Stderr, "Error: invalid job definition")
		os.Exit(1)
	case http.StatusUnsupportedMediaType:
		fmt.Fprintln(os.Stderr, "Error: content type must be application/json")
		os.Exit(1)
	case http.StatusServiceUnavailable:
		fmt.Fprintln(os.Stderr, "Error: database unavailable")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func deleteJob(cmd *cobra.Command, args []string) {
	jobID := args[0]

	force, _ := cmd.Flags().GetBool("yes")
	if !force {
		fmt.Fprintf(os.Stderr, "Delete job %q? This removes the definition and prevents future triggers.\n", jobID)
		fmt.Fprintf(os.Stderr, "Re-run with --yes to confirm.\n")
		os.Exit(1)
	}

	req, err := newAPIRequest(http.MethodDelete, fmt.Sprintf("/api/v1/jobs/%s", jobID), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
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
		fmt.Printf("Job %q deleted.\n", jobID)
	case http.StatusNotFound:
		fmt.Fprintf(os.Stderr, "Error: job %q not found\n", jobID)
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "Error: unexpected status: %s\n", resp.Status)
		os.Exit(1)
	}
}

func listJobNames(w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/jobs", nil)
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
		Name string `json:"name"`
	}

	var jobsResp struct {
		Data []jobListItem `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&jobsResp); err != nil {
		return fmt.Errorf("failed to parse jobs response: %w", err)
	}

	names := make([]string, 0, len(jobsResp.Data))
	for _, j := range jobsResp.Data {
		if j.Name != "" {
			names = append(names, j.Name)
		}
	}

	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintln(w, name)
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
On success prints the job ID (and run_id with --follow); the API returns after persisting the run, then enqueues in the background.`,
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
	Long:  `Fetch all stored jobs and print each job id on its own line.`,
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
