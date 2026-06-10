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

type sourceRepositoryStatusResult struct {
	RepositoryID       string                     `json:"repository_id"`
	Namespace          string                     `json:"namespace"`
	SourceKind         string                     `json:"source_kind"`
	Enabled            bool                       `json:"enabled"`
	Status             string                     `json:"status"`
	CheckoutMode       string                     `json:"checkout_mode"`
	AuthoringMode      string                     `json:"authoring_mode"`
	Authoring          sourceAuthoringSummary     `json:"authoring"`
	CheckoutPath       string                     `json:"checkout_path,omitempty"`
	PathExists         bool                       `json:"path_exists"`
	PathIsDirectory    bool                       `json:"path_is_directory"`
	GitRepository      bool                       `json:"git_repository"`
	WorkTreePath       string                     `json:"work_tree_path,omitempty"`
	HeadRef            string                     `json:"head_ref,omitempty"`
	DefaultRef         string                     `json:"default_ref,omitempty"`
	DefaultRefResolved bool                       `json:"default_ref_resolved"`
	ResolvedCommit     string                     `json:"resolved_commit,omitempty"`
	Sync               sourceRepositorySyncInfo   `json:"sync"`
	Error              *sourceRepositoryErrorInfo `json:"error,omitempty"`
}

type sourceRepositoryErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type sourceRepositoryBranchSummary struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Commit string `json:"commit"`
	Remote string `json:"remote,omitempty"`
}

type sourceRepositoryBranchesResult struct {
	RepositoryID string                          `json:"repository_id"`
	Prefix       string                          `json:"prefix,omitempty"`
	Limit        int                             `json:"limit"`
	Branches     []sourceRepositoryBranchSummary `json:"branches"`
}

type sourceRepositoryTreeEntry struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Mode      string `json:"mode"`
	ObjectSHA string `json:"object_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type sourceRepositoryTreeResult struct {
	RepositoryID   string                      `json:"repository_id"`
	RequestedRef   string                      `json:"requested_ref"`
	ResolvedCommit string                      `json:"resolved_commit"`
	Path           string                      `json:"path,omitempty"`
	Recursive      bool                        `json:"recursive"`
	Limit          int                         `json:"limit"`
	Entries        []sourceRepositoryTreeEntry `json:"entries"`
}

type sourceRepositoryDefinitionFile struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	BlobSHA   string `json:"blob_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type sourceRepositoryDefinitionsResult struct {
	RepositoryID   string                           `json:"repository_id"`
	RequestedRef   string                           `json:"requested_ref"`
	ResolvedCommit string                           `json:"resolved_commit"`
	Path           string                           `json:"path"`
	Limit          int                              `json:"limit"`
	Definitions    []sourceRepositoryDefinitionFile `json:"definitions"`
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

type sourceRepositoryJobDefinitionResult struct {
	JobID          string           `json:"job_id"`
	DefinitionHash string           `json:"definition_hash"`
	Definition     json.RawMessage  `json:"definition"`
	Source         sourceProvenance `json:"source"`
}

type sourceRepositoryJobDefinitionWriteRequest struct {
	Ref          string          `json:"ref,omitempty"`
	Branch       string          `json:"branch,omitempty"`
	Path         string          `json:"path,omitempty"`
	Message      string          `json:"message,omitempty"`
	ExpectedHead string          `json:"expected_head,omitempty"`
	Definition   json.RawMessage `json:"definition"`
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

func showSourceStatus(cmd *cobra.Command, args []string) {
	runCLIError(showSourceStatusWithOutput(os.Stdout, args[0]))
}

func showSourceStatusWithOutput(out io.Writer, repositoryID string) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/source-repositories/"+url.PathEscape(repositoryID)+"/status", nil)
	if err != nil {
		return fmt.Errorf("failed to create source repository status request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to fetch source repository status: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryStatusResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source repository status response: %w", err)
		}
		return writeSourceStatusResult(out, result)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status fetching source repository status: %s", resp.Status)
	}
}

func writeSourceStatusResult(out io.Writer, result sourceRepositoryStatusResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	fmt.Fprintf(out, "repository_id=%s\n", result.RepositoryID)
	fmt.Fprintf(out, "namespace=%s\n", emptyAsDash(result.Namespace))
	fmt.Fprintf(out, "status=%s\n", emptyAsDash(result.Status))
	fmt.Fprintf(out, "enabled=%t\n", result.Enabled)
	fmt.Fprintf(out, "checkout_mode=%s\n", emptyAsDash(result.CheckoutMode))
	fmt.Fprintf(out, "authoring_mode=%s\n", emptyAsDash(result.AuthoringMode))
	fmt.Fprintf(out, "write_definitions=%t\n", result.Authoring.WriteDefinitions)
	if strings.TrimSpace(result.Authoring.Reason) != "" {
		fmt.Fprintf(out, "authoring_reason=%s\n", result.Authoring.Reason)
	}
	if strings.TrimSpace(result.CheckoutPath) != "" {
		fmt.Fprintf(out, "checkout_path=%s\n", result.CheckoutPath)
	}
	fmt.Fprintf(out, "path_exists=%t\n", result.PathExists)
	fmt.Fprintf(out, "path_is_directory=%t\n", result.PathIsDirectory)
	fmt.Fprintf(out, "git_repository=%t\n", result.GitRepository)
	if strings.TrimSpace(result.WorkTreePath) != "" {
		fmt.Fprintf(out, "work_tree_path=%s\n", result.WorkTreePath)
	}
	if strings.TrimSpace(result.HeadRef) != "" {
		fmt.Fprintf(out, "head_ref=%s\n", result.HeadRef)
	}
	if strings.TrimSpace(result.DefaultRef) != "" {
		fmt.Fprintf(out, "default_ref=%s\n", result.DefaultRef)
		fmt.Fprintf(out, "default_ref_resolved=%t\n", result.DefaultRefResolved)
	}
	if strings.TrimSpace(result.ResolvedCommit) != "" {
		fmt.Fprintf(out, "resolved_commit=%s\n", result.ResolvedCommit)
	}
	if strings.TrimSpace(result.Sync.Status) != "" {
		fmt.Fprintf(out, "sync_status=%s\n", result.Sync.Status)
	}
	if strings.TrimSpace(result.Sync.Ref) != "" {
		fmt.Fprintf(out, "sync_ref=%s\n", result.Sync.Ref)
	}
	if strings.TrimSpace(result.Sync.Commit) != "" {
		fmt.Fprintf(out, "sync_commit=%s\n", result.Sync.Commit)
	}
	if strings.TrimSpace(result.Sync.Error) != "" {
		fmt.Fprintf(out, "sync_error=%s\n", result.Sync.Error)
	}
	if result.Error != nil {
		fmt.Fprintf(out, "error_code=%s\n", result.Error.Code)
		fmt.Fprintf(out, "error_message=%s\n", result.Error.Message)
	}

	return nil
}

func listSourceBranches(cmd *cobra.Command, args []string) {
	runCLIError(listSourceBranchesWithOutput(os.Stdout, args[0]))
}

func listSourceBranchesWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/refs/branches"
	params := url.Values{}
	if v := strings.TrimSpace(sourceBranchesPrefix); v != "" {
		params.Set("prefix", v)
	}
	if sourceBranchesLimit > 0 {
		params.Set("limit", strconv.Itoa(sourceBranchesLimit))
	}
	path = appendQueryParams(path, params)

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source branches request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source branches: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryBranchesResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source branches response: %w", err)
		}
		return writeSourceBranchesResult(out, result)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status listing source branches: %s", resp.Status)
	}
}

func writeSourceBranchesResult(out io.Writer, result sourceRepositoryBranchesResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if len(result.Branches) == 0 {
		fmt.Fprintln(out, "No source branches found")
		return nil
	}

	if sourceBranchesQuiet {
		for _, branch := range result.Branches {
			fmt.Fprintln(out, branch.Name)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tREF\tCOMMIT\tREMOTE")
	for _, branch := range result.Branches {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			branch.Name,
			branch.Ref,
			shortSHA(branch.Commit),
			emptyAsDash(branch.Remote),
		)
	}
	return tw.Flush()
}

func listSourceTree(cmd *cobra.Command, args []string) {
	runCLIError(listSourceTreeWithOutput(os.Stdout, args[0]))
}

func listSourceTreeWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/tree"
	params := url.Values{}
	if v := strings.TrimSpace(sourceTreeRef); v != "" {
		params.Set("ref", v)
	}
	if v := strings.TrimSpace(sourceTreePath); v != "" {
		params.Set("path", v)
	}
	if sourceTreeLimit > 0 {
		params.Set("limit", strconv.Itoa(sourceTreeLimit))
	}
	if sourceTreeRecursive {
		params.Set("recursive", "true")
	}
	path = appendQueryParams(path, params)

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source tree request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source tree: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryTreeResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source tree response: %w", err)
		}
		return writeSourceTreeResult(out, result)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or tree path not found", repositoryID)
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source tree request")
	default:
		return fmt.Errorf("unexpected status listing source tree: %s", resp.Status)
	}
}

func writeSourceTreeResult(out io.Writer, result sourceRepositoryTreeResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if len(result.Entries) == 0 {
		fmt.Fprintln(out, "No source tree entries found")
		return nil
	}

	if sourceTreeQuiet {
		for _, entry := range result.Entries {
			fmt.Fprintln(out, entry.Path)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PATH\tTYPE\tOBJECT\tSIZE")
	for _, entry := range result.Entries {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			entry.Path,
			entry.Type,
			shortSHA(entry.ObjectSHA),
			sizeAsDash(entry.SizeBytes),
		)
	}
	return tw.Flush()
}

func listSourceDefinitions(cmd *cobra.Command, args []string) {
	runCLIError(listSourceDefinitionsWithOutput(os.Stdout, args[0]))
}

func listSourceDefinitionsWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/definitions"
	params := url.Values{}
	if v := strings.TrimSpace(sourceDefinitionsRef); v != "" {
		params.Set("ref", v)
	}
	if v := strings.TrimSpace(sourceDefinitionsPath); v != "" {
		params.Set("path", v)
	}
	if sourceDefinitionsLimit > 0 {
		params.Set("limit", strconv.Itoa(sourceDefinitionsLimit))
	}
	path = appendQueryParams(path, params)

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source definitions request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source definitions: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryDefinitionsResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source definitions response: %w", err)
		}
		return writeSourceDefinitionsResult(out, result)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or definitions path not found", repositoryID)
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source definitions request")
	default:
		return fmt.Errorf("unexpected status listing source definitions: %s", resp.Status)
	}
}

func writeSourceDefinitionsResult(out io.Writer, result sourceRepositoryDefinitionsResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if len(result.Definitions) == 0 {
		fmt.Fprintln(out, "No source definitions found")
		return nil
	}

	if sourceDefinitionsQuiet {
		for _, definition := range result.Definitions {
			fmt.Fprintln(out, definition.Path)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PATH\tBLOB\tSIZE")
	for _, definition := range result.Definitions {
		fmt.Fprintf(tw, "%s\t%s\t%s\n",
			definition.Path,
			shortSHA(definition.BlobSHA),
			sizeAsDash(definition.SizeBytes),
		)
	}
	return tw.Flush()
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

func showSourceJob(cmd *cobra.Command, args []string) {
	runCLIError(showSourceJobWithOutput(cmd, os.Stdout, args[0], args[1]))
}

func showSourceJobWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs/" + url.PathEscape(jobID) + "/definition"
	params := url.Values{}
	if v := strings.TrimSpace(sourceShowRef); v != "" {
		params.Set("ref", v)
	}
	if v := strings.TrimSpace(sourceShowPath); v != "" {
		params.Set("path", v)
	}
	if encoded := params.Encode(); encoded != "" {
		path += "?" + encoded
	}

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source job definition request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to fetch source job definition: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryJobDefinitionResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source job definition response: %w", err)
		}
		if outputIsJSON() {
			return writeJSON(out, result)
		}
		raw, _ := cmd.Flags().GetBool("raw")
		_, err := out.Write(formatJobDefinitionBody(result.Definition, !raw))
		return err
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or job %q not found", repositoryID, jobID)
	default:
		return fmt.Errorf("unexpected status fetching source job definition: %s", resp.Status)
	}
}

func writeSourceJob(cmd *cobra.Command, args []string) {
	runCLIError(writeSourceJobWithOutput(os.Stdout, args[0], args[1], args[2]))
}

func writeSourceJobWithOutput(out io.Writer, repositoryID, jobID, source string) error {
	definition, err := readSourceDefinitionBody(source)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(sourceRepositoryJobDefinitionWriteRequest{
		Ref:          strings.TrimSpace(sourceWriteRef),
		Branch:       strings.TrimSpace(sourceWriteBranch),
		Path:         strings.TrimSpace(sourceWritePath),
		Message:      strings.TrimSpace(sourceWriteMessage),
		ExpectedHead: strings.TrimSpace(sourceWriteExpectedHead),
		Definition:   json.RawMessage(definition),
	})
	if err != nil {
		return fmt.Errorf("failed to encode source job definition write request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPut, "/api/v1/source-repositories/"+url.PathEscape(repositoryID)+"/jobs/"+url.PathEscape(jobID)+"/definition", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create source job definition write request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to write source job definition: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceRepositoryJobDefinitionResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source job definition write response: %w", err)
		}
		return writeSourceJobDefinitionWriteResult(out, result)
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
		return fmt.Errorf("unexpected status writing source job definition: %s", resp.Status)
	}
}

func readSourceDefinitionBody(source string) ([]byte, error) {
	var body []byte
	var err error
	if source == "-" {
		body, err = io.ReadAll(os.Stdin)
	} else {
		body, err = os.ReadFile(source)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read source job definition: %w", err)
	}

	body = bytes.TrimSpace(body)
	if !json.Valid(body) {
		return nil, fmt.Errorf("invalid job JSON")
	}

	return body, nil
}

func writeSourceJobDefinitionWriteResult(out io.Writer, result sourceRepositoryJobDefinitionResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if sourceWriteQuiet {
		_, err := fmt.Fprintln(out, result.Source.ResolvedCommit)
		return err
	}

	fmt.Fprintf(out, "job_id=%s\n", result.JobID)
	fmt.Fprintf(out, "commit=%s\n", result.Source.ResolvedCommit)
	fmt.Fprintf(out, "path=%s\n", result.Source.Path)
	fmt.Fprintf(out, "blob_sha=%s\n", result.Source.BlobSHA)
	fmt.Fprintf(out, "definition_hash=%s\n", result.DefinitionHash)
	if strings.TrimSpace(result.Source.RequestedRef) != "" {
		fmt.Fprintf(out, "requested_ref=%s\n", result.Source.RequestedRef)
	}

	return nil
}

func listSourceRuns(cmd *cobra.Command, args []string) {
	since, _ := cmd.Flags().GetString("since")
	runCLIError(listSourceRunsWithOutput(os.Stdout, args[0], args[1], sourceRunsLimit, sourceRunsCursor, since, sourceRunsCellID))
}

func listSourceRunsWithOutput(out io.Writer, repositoryID, jobID string, limit, cursor int, since, cellID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs/" + url.PathEscape(jobID) + "/runs"
	return listRunsPath(path, limit, cursor, since, cellID, out)
}

func runSourceLogs(cmd *cobra.Command, args []string) {
	repositoryID := args[0]
	jobID := args[1]
	filterStdout, _ := cmd.Flags().GetBool("stdout")
	filterStderr, _ := cmd.Flags().GetBool("stderr")
	follow, _ := cmd.Flags().GetBool("follow")

	if follow {
		if len(args) > 2 {
			runCLIError(fmt.Errorf("--follow waits for future runs and cannot be combined with a run-id"))
		}

		if err := runContinuousSourceLogs(repositoryID, jobID, filterStdout, filterStderr); err != nil {
			if err.Error() == "interrupted" {
				return
			}

			runCLIError(err)
		}

		return
	}

	if len(args) > 2 {
		runID, err := resolveLogIDArg(args[2])
		if err != nil {
			runCLIError(err)
		}

		runCLIError(runSourceLogStream(repositoryID, jobID, runID, filterStdout, filterStderr))
		return
	}

	run, ok, err := latestRunForSourceJob(repositoryID, jobID)
	if err != nil {
		runCLIError(err)
	}

	if !ok {
		runCLIError(fmt.Errorf("no runs found for source job %q/%q; use --follow to wait for future runs", repositoryID, jobID))
	}

	if !outputIsJSON() {
		fmt.Printf("Streaming latest run for source job %s/%s: %s\n", repositoryID, jobID, run.RunID)
	}

	runCLIError(runSourceLogStream(repositoryID, jobID, run.RunID, filterStdout, filterStderr))
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

func sizeAsDash(size int64) string {
	if size <= 0 {
		return "-"
	}
	return strconv.FormatInt(size, 10)
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

var sourcesStatusCmd = &cobra.Command{
	Use:   "status [repository-id]",
	Short: "Show source repository checkout status",
	Long:  `Check whether a source repository checkout is usable, whether its default ref resolves, and what authoring capabilities are available.`,
	Args:  cobra.ExactArgs(1),
	Run:   showSourceStatus,
}

var sourcesBranchesCmd = &cobra.Command{
	Use:   "branches [repository-id]",
	Short: "List source repository branches",
	Long:  `List branch refs available from a source repository checkout for ref pickers and manual source operations.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceBranches,
}

var sourcesTreeCmd = &cobra.Command{
	Use:   "tree [repository-id]",
	Short: "List source repository tree entries",
	Long:  `List tree entries at a source repository ref and path without reading file contents.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceTree,
}

var sourcesDefinitionsCmd = &cobra.Command{
	Use:   "definitions [repository-id]",
	Short: "List source definition files",
	Long:  `Discover candidate JSON job definition files in a source repository without loading file contents.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceDefinitions,
}

var sourcesJobsCmd = &cobra.Command{
	Use:   "jobs [repository-id]",
	Short: "List jobs discovered in a source repository",
	Long:  `List triggerable source jobs derived from repository definition paths without importing them as stored jobs.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceJobs,
}

var sourcesShowCmd = &cobra.Command{
	Use:   "show [repository-id] [job-id]",
	Short: "Show a source job definition",
	Long:  `Resolve a source repository job definition at a ref and print the canonical JSON without importing it as a stored job.`,
	Args:  cobra.ExactArgs(2),
	Run:   showSourceJob,
}

var sourcesWriteCmd = &cobra.Command{
	Use:   "write [repository-id] [job-id] [definition-file]",
	Short: "Write a source job definition",
	Long:  `Write a JSON job definition into a source repository checkout without creating a stored job row. Use "-" as definition-file to read from stdin.`,
	Args:  cobra.ExactArgs(3),
	Run:   writeSourceJob,
}

var sourcesRunsCmd = &cobra.Command{
	Use:   "runs [repository-id] [job-id]",
	Short: "List runs for a source job",
	Long:  `List run history scoped to a source repository job's recorded source provenance.`,
	Args:  cobra.ExactArgs(2),
	Run:   listSourceRuns,
}

var sourcesLogsCmd = &cobra.Command{
	Use:   "logs [repository-id] [job-id] [run-id]",
	Short: "Stream logs for a source job run",
	Long:  `Stream logs for a source repository job run using source provenance to disambiguate runs from stored jobs with the same job ID. Omit run-id to stream the latest source run, or add --follow to wait for future source runs.`,
	Args:  cobra.RangeArgs(2, 3),
	Run:   runSourceLogs,
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

func configureSourcesBranchesFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceBranchesPrefix, "prefix", "", "Branch name prefix to filter")
	cmd.Flags().IntVar(&sourceBranchesLimit, "limit", 0, "Max branches to return")
	cmd.Flags().BoolVarP(&sourceBranchesQuiet, "quiet", "q", false, "Print only branch names")
}

func configureSourcesTreeFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceTreeRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceTreePath, "path", "", "Tree path to list")
	cmd.Flags().IntVar(&sourceTreeLimit, "limit", 0, "Max tree entries to return")
	cmd.Flags().BoolVarP(&sourceTreeRecursive, "recursive", "r", false, "List tree entries recursively")
	cmd.Flags().BoolVarP(&sourceTreeQuiet, "quiet", "q", false, "Print only paths")
}

func configureSourcesDefinitionsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceDefinitionsRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceDefinitionsPath, "path", "", "Definition directory path (default: .vectis/jobs)")
	cmd.Flags().IntVar(&sourceDefinitionsLimit, "limit", 0, "Max definition files to return")
	cmd.Flags().BoolVarP(&sourceDefinitionsQuiet, "quiet", "q", false, "Print only definition paths")
}

func configureSourcesJobsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceJobsRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceJobsPath, "path", "", "Definition directory path (default: .vectis/jobs)")
	cmd.Flags().IntVar(&sourceJobsLimit, "limit", 0, "Max source jobs to return")
	cmd.Flags().BoolVarP(&sourceJobsQuiet, "quiet", "q", false, "Print only job IDs")
}

func configureSourcesShowFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceShowRef, "ref", "", "Git ref to resolve (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceShowPath, "path", "", "Definition file path override")
	cmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
}

func configureSourcesWriteFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceWriteRef, "ref", "", "Git ref to use as the write base (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceWriteBranch, "branch", "", "Local branch to update instead of --ref/default")
	cmd.Flags().StringVar(&sourceWritePath, "path", "", "Definition file path override")
	cmd.Flags().StringVar(&sourceWriteMessage, "message", "", "Commit message for local source authoring")
	cmd.Flags().StringVar(&sourceWriteExpectedHead, "expected-head", "", "Require the target branch to still point at this commit")
	cmd.Flags().BoolVarP(&sourceWriteQuiet, "quiet", "q", false, "Print only the resulting commit")
}

func configureSourcesRunsFlags(cmd *cobra.Command) {
	cmd.Flags().IntVar(&sourceRunsLimit, "limit", 0, "Max runs to return")
	cmd.Flags().IntVar(&sourceRunsCursor, "cursor", 0, "Continue listing after this result cursor")
	cmd.Flags().String("since", "", "Only include runs since RFC3339 timestamp or YYYY-MM-DD")
	cmd.Flags().StringVar(&sourceRunsCellID, "cell", "", "Filter by owning execution cell")
}

func configureSourcesLogsFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "Wait for and stream future source job runs")
}

func configureSourcesTriggerFlags(cmd *cobra.Command) {
	cmd.Flags().BoolP("follow", "f", false, "After triggering, stream logs (same as logs run <run-id>)")
	cmd.Flags().StringVar(&sourceTriggerRef, "ref", "", "Git ref to trigger (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceTriggerPath, "path", "", "Definition file path override")
	cmd.Flags().StringVar(&sourceTriggerCellID, "cell", "", "Target execution cell")
	cmd.Flags().StringVar(&sourceTriggerIdemKey, "idempotency-key", "", "Optional Idempotency-Key header for safe trigger retries")
}
