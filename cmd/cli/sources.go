package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

type sourceRepositorySummary struct {
	RepositoryID  string                   `json:"repository_id"`
	Namespace     string                   `json:"namespace"`
	SourceKind    string                   `json:"source_kind"`
	CheckoutPath  string                   `json:"checkout_path,omitempty"`
	CheckoutMode  string                   `json:"checkout_mode"`
	AuthoringMode string                   `json:"authoring_mode"`
	Authoring     sourceAuthoringSummary   `json:"authoring"`
	CanonicalURL  string                   `json:"canonical_url,omitempty"`
	DefaultRef    string                   `json:"default_ref,omitempty"`
	CredentialRef string                   `json:"credential_ref,omitempty"`
	Enabled       bool                     `json:"enabled"`
	Sync          sourceRepositorySyncInfo `json:"sync"`
}

type sourceAuthoringSummary struct {
	Mode                   string `json:"mode"`
	WriteDefinitions       bool   `json:"write_definitions"`
	LocalCommits           bool   `json:"local_commits"`
	ExternalChangeRequests bool   `json:"external_change_requests"`
	Reason                 string `json:"reason,omitempty"`
}

type sourceRepositorySyncInfo struct {
	Status             string `json:"status"`
	LastStartedAtUnix  int64  `json:"last_started_at_unix,omitempty"`
	LastFinishedAtUnix int64  `json:"last_finished_at_unix,omitempty"`
	Ref                string `json:"ref,omitempty"`
	Commit             string `json:"commit,omitempty"`
	Error              string `json:"error,omitempty"`
}

type sourceRepositoryJobSummary struct {
	JobID     string           `json:"job_id"`
	Path      string           `json:"path"`
	Name      string           `json:"name"`
	BlobSHA   string           `json:"blob_sha"`
	SizeBytes int64            `json:"size_bytes,omitempty"`
	Source    sourceProvenance `json:"source"`
}

type sourceRepositoryJobsResult struct {
	RepositoryID   string                       `json:"repository_id"`
	RequestedRef   string                       `json:"requested_ref"`
	ResolvedCommit string                       `json:"resolved_commit"`
	Path           string                       `json:"path"`
	Limit          int                          `json:"limit"`
	Jobs           []sourceRepositoryJobSummary `json:"jobs"`
}

type sourceProvenance struct {
	RepositoryID   string `json:"repository_id"`
	RequestedRef   string `json:"requested_ref"`
	ResolvedCommit string `json:"resolved_commit"`
	Path           string `json:"path"`
	BlobSHA        string `json:"blob_sha,omitempty"`
}

type sourceTriggerResult struct {
	JobID             string           `json:"job_id"`
	RunID             string           `json:"run_id"`
	RunIndex          int              `json:"run_index"`
	DefinitionVersion int              `json:"definition_version"`
	DefinitionHash    string           `json:"definition_hash"`
	Source            sourceProvenance `json:"source"`
}

func listSources(cmd *cobra.Command, args []string) {
	runCLIError(listSourcesWithOutput(os.Stdout))
}

func listSourcesWithOutput(out io.Writer) error {
	path := "/api/v1/source-repositories"
	params := url.Values{}
	if v := strings.TrimSpace(sourceListNamespace); v != "" {
		params.Set("namespace", v)
	}
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source repository list request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source repositories: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status listing source repositories: %s", resp.Status)
	}

	var repositories []sourceRepositorySummary
	if err := json.NewDecoder(resp.Body).Decode(&repositories); err != nil {
		return fmt.Errorf("failed to parse source repositories response: %w", err)
	}

	if outputIsJSON() {
		return writeJSON(out, repositories)
	}

	if len(repositories) == 0 {
		fmt.Fprintln(out, "No source repositories found")
		return nil
	}

	if sourceListQuiet {
		for _, repo := range repositories {
			fmt.Fprintln(out, repo.RepositoryID)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPOSITORY\tNAMESPACE\tCHECKOUT\tAUTHORING\tENABLED\tSYNC\tDEFAULT REF")
	for _, repo := range repositories {
		defaultRef := repo.DefaultRef
		if defaultRef == "" {
			defaultRef = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%s\t%s\n",
			repo.RepositoryID,
			emptyAsDash(repo.Namespace),
			emptyAsDash(repo.CheckoutMode),
			emptyAsDash(repo.AuthoringMode),
			repo.Enabled,
			emptyAsDash(repo.Sync.Status),
			defaultRef,
		)
	}
	return tw.Flush()
}

func registerSource(cmd *cobra.Command, args []string) {
	runCLIError(registerSourceWithOutput(os.Stdout, args))
}

func registerSourceWithOutput(out io.Writer, args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("repository-id is required")
	}

	checkoutPath := ""
	if len(args) > 1 {
		checkoutPath = args[1]
	}

	payload := map[string]any{
		"repository_id": args[0],
	}
	if v := strings.TrimSpace(sourceRegisterNamespace); v != "" {
		payload["namespace"] = v
	}
	if checkoutPath = strings.TrimSpace(checkoutPath); checkoutPath != "" {
		payload["checkout_path"] = checkoutPath
	}
	if v := strings.TrimSpace(sourceRegisterCheckoutMode); v != "" {
		payload["checkout_mode"] = v
	}
	if v := strings.TrimSpace(sourceRegisterAuthoringMode); v != "" {
		payload["authoring_mode"] = v
	}
	if v := strings.TrimSpace(sourceRegisterCanonicalURL); v != "" {
		payload["canonical_url"] = v
	}
	if v := strings.TrimSpace(sourceRegisterDefaultRef); v != "" {
		payload["default_ref"] = v
	}
	if v := strings.TrimSpace(sourceRegisterCredentialRef); v != "" {
		payload["credential_ref"] = v
	}
	if sourceRegisterDisabled {
		payload["enabled"] = false
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode source repository registration: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/source-repositories", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source repository registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to register source repository: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var repo sourceRepositorySummary
		if err := json.NewDecoder(resp.Body).Decode(&repo); err != nil {
			return fmt.Errorf("failed to parse source repository response: %w", err)
		}
		if outputIsJSON() {
			return writeJSON(out, repo)
		}
		_, err := fmt.Fprintf(out, "Source repository %q registered.\n", repo.RepositoryID)
		return err
	case http.StatusConflict:
		return fmt.Errorf("source repository %q already exists or conflicts with an existing checkout", args[0])
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source repository registration")
	default:
		return fmt.Errorf("unexpected status registering source repository: %s", resp.Status)
	}
}

func syncSource(cmd *cobra.Command, args []string) {
	runCLIError(syncSourceWithOutput(os.Stdout, args[0]))
}

func syncSourceWithOutput(out io.Writer, repositoryID string) error {
	req, err := newAPIRequest(http.MethodPost, "/api/v1/source-repositories/"+url.PathEscape(repositoryID)+"/sync", nil)
	if err != nil {
		return fmt.Errorf("failed to create source repository sync request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to sync source repository: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		var repo sourceRepositorySummary
		if err := json.NewDecoder(resp.Body).Decode(&repo); err != nil {
			return fmt.Errorf("failed to parse source repository sync response: %w", err)
		}
		if outputIsJSON() {
			return writeJSON(out, repo)
		}
		_, err := fmt.Fprintf(out, "Source repository %q sync status: %s\n", repo.RepositoryID, emptyAsDash(repo.Sync.Status))
		return err
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status syncing source repository: %s", resp.Status)
	}
}

func listSourceJobs(cmd *cobra.Command, args []string) {
	runCLIError(listSourceJobsWithOutput(os.Stdout, args[0]))
}

func listSourceJobsWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs"
	params := url.Values{}
	if v := strings.TrimSpace(sourceJobsRef); v != "" {
		params.Set("ref", v)
	}
	if v := strings.TrimSpace(sourceJobsPath); v != "" {
		params.Set("path", v)
	}
	if sourceJobsLimit > 0 {
		params.Set("limit", strconv.Itoa(sourceJobsLimit))
	}
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source jobs request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source jobs: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryJobsResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source jobs response: %w", err)
		}
		if outputIsJSON() {
			return writeJSON(out, result)
		}
		if len(result.Jobs) == 0 {
			fmt.Fprintln(out, "No source jobs found")
			return nil
		}
		if sourceJobsQuiet {
			for _, job := range result.Jobs {
				fmt.Fprintln(out, job.JobID)
			}
			return nil
		}
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "JOB ID\tPATH\tCOMMIT\tBLOB")
		for _, job := range result.Jobs {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
				job.JobID,
				job.Path,
				shortSHA(job.Source.ResolvedCommit),
				shortSHA(job.BlobSHA),
			)
		}
		return tw.Flush()
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status listing source jobs: %s", resp.Status)
	}
}

func triggerSourceJob(cmd *cobra.Command, args []string) {
	runCLIError(triggerSourceJobWithOutput(cmd, os.Stdout, args[0], args[1]))
}

func triggerSourceJobWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	body, err := json.Marshal(sourceTriggerRequest{
		Ref:    strings.TrimSpace(sourceTriggerRef),
		Path:   strings.TrimSpace(sourceTriggerPath),
		CellID: strings.TrimSpace(sourceTriggerCellID),
	})
	if err != nil {
		return fmt.Errorf("failed to encode source trigger request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/source-repositories/"+url.PathEscape(repositoryID)+"/jobs/"+url.PathEscape(jobID)+"/trigger", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source trigger request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, sourceTriggerIdemKey)

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to trigger source job: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusAccepted:
		var result sourceTriggerResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source trigger response: %w", err)
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
	case http.StatusConflict:
		return fmt.Errorf("source repository %q is disabled or unavailable", repositoryID)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or job %q not found", repositoryID, jobID)
	default:
		return fmt.Errorf("unexpected status triggering source job: %s", resp.Status)
	}
}

type sourceTriggerRequest struct {
	Ref    string `json:"ref,omitempty"`
	Path   string `json:"path,omitempty"`
	CellID string `json:"cell_id,omitempty"`
}

func emptyAsDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func shortSHA(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 12 {
		return emptyAsDash(s)
	}
	return s[:12]
}

var sourcesCmd = &cobra.Command{
	Use:     "sources",
	Short:   "Work with source repositories",
	Long:    `Register source repositories, inspect source-defined jobs, and trigger jobs directly from source.`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var sourcesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List source repositories",
	Long:  `Fetch registered source repositories and print their checkout, authoring, and sync state.`,
	Args:  cobra.NoArgs,
	Run:   listSources,
}

var sourcesRegisterCmd = &cobra.Command{
	Use:   "register [repository-id] [checkout-path]",
	Short: "Register a source repository checkout",
	Long:  `Register a source repository. Pass checkout-path for external checkouts; omit it with --checkout-mode managed to let the API derive the managed checkout path.`,
	Args:  cobra.RangeArgs(1, 2),
	Run:   registerSource,
}

var sourcesSyncCmd = &cobra.Command{
	Use:   "sync [repository-id]",
	Short: "Sync a source repository",
	Long:  `Probe or refresh a source repository checkout and persist its latest sync status.`,
	Args:  cobra.ExactArgs(1),
	Run:   syncSource,
}

var sourcesJobsCmd = &cobra.Command{
	Use:   "jobs [repository-id]",
	Short: "List jobs discovered in a source repository",
	Long:  `List triggerable source jobs derived from repository definition paths without importing them as stored jobs.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceJobs,
}

var sourcesTriggerCmd = &cobra.Command{
	Use:   "trigger [repository-id] [job-id]",
	Short: "Trigger a job directly from source",
	Long:  `Trigger a source repository job without creating or requiring a stored job row.`,
	Args:  cobra.ExactArgs(2),
	Run:   triggerSourceJob,
}

func configureSourcesListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceListNamespace, "namespace", "", "Namespace to list source repositories from (default: /)")
	cmd.Flags().BoolVarP(&sourceListQuiet, "quiet", "q", false, "Print only repository IDs")
}

func configureSourcesRegisterFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceRegisterNamespace, "namespace", "", "Namespace to register the repository in (default: /)")
	cmd.Flags().StringVar(&sourceRegisterCheckoutMode, "checkout-mode", "", "Checkout mode: external or managed")
	cmd.Flags().StringVar(&sourceRegisterAuthoringMode, "authoring-mode", "", "Authoring mode: read_only, local_commit, or external_change_request")
	cmd.Flags().StringVar(&sourceRegisterCanonicalURL, "canonical-url", "", "Canonical clone/fetch URL for managed checkouts")
	cmd.Flags().StringVar(&sourceRegisterDefaultRef, "default-ref", "", "Default git ref for source operations")
	cmd.Flags().StringVar(&sourceRegisterCredentialRef, "credential-ref", "", "Credential reference for future source integrations")
	cmd.Flags().BoolVar(&sourceRegisterDisabled, "disabled", false, "Register the repository disabled")
}

func configureSourcesJobsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceJobsRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceJobsPath, "path", "", "Definition directory path (default: .vectis/jobs)")
	cmd.Flags().IntVar(&sourceJobsLimit, "limit", 0, "Max source jobs to return")
	cmd.Flags().BoolVarP(&sourceJobsQuiet, "quiet", "q", false, "Print only job IDs")
}

func configureSourcesTriggerFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringVar(&sourceTriggerRef, "ref", "", "Git ref to trigger (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceTriggerPath, "path", "", "Definition file path override")
	cmd.Flags().StringVar(&sourceTriggerCellID, "cell", "", "Target execution cell")
	cmd.Flags().StringVar(&sourceTriggerIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe trigger retries")
}
