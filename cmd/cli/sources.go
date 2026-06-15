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
	Declared      bool                     `json:"declared"`
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

type sourceOverviewResult struct {
	RepositoriesConfigured bool                         `json:"repositories_configured"`
	SchedulesConfigured    bool                         `json:"schedules_configured"`
	DeclaredRepositories   int                          `json:"declared_repositories"`
	DeclaredSchedules      int                          `json:"declared_schedules"`
	Repositories           sourceOverviewRepositoryInfo `json:"repositories"`
	Schedules              sourceOverviewScheduleInfo   `json:"schedules"`
}

type sourceOverviewRepositoryInfo struct {
	Total         int `json:"total"`
	Enabled       int `json:"enabled"`
	Disabled      int `json:"disabled"`
	Declared      int `json:"declared"`
	StaleEnabled  int `json:"stale_enabled"`
	StaleDisabled int `json:"stale_disabled"`
	SyncSucceeded int `json:"sync_succeeded"`
	SyncFailed    int `json:"sync_failed"`
	SyncRunning   int `json:"sync_running"`
	SyncNever     int `json:"sync_never"`
}

type sourceOverviewScheduleInfo struct {
	Total           int `json:"total"`
	Enabled         int `json:"enabled"`
	Disabled        int `json:"disabled"`
	Declared        int `json:"declared"`
	StaleEnabled    int `json:"stale_enabled"`
	StaleDisabled   int `json:"stale_disabled"`
	ActiveOverrides int `json:"active_overrides"`
}

type sourceRepositoryStatusResult struct {
	RepositoryID       string                     `json:"repository_id"`
	Namespace          string                     `json:"namespace"`
	SourceKind         string                     `json:"source_kind"`
	Declared           bool                       `json:"declared"`
	Enabled            bool                       `json:"enabled"`
	Status             string                     `json:"status"`
	CheckoutMode       string                     `json:"checkout_mode"`
	AuthoringMode      string                     `json:"authoring_mode"`
	Authoring          sourceAuthoringSummary     `json:"authoring"`
	CredentialRef      string                     `json:"credential_ref,omitempty"`
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
	Truncated    bool                            `json:"truncated"`
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
	Truncated      bool                        `json:"truncated"`
	NextCursor     string                      `json:"next_cursor,omitempty"`
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
	Truncated      bool                             `json:"truncated"`
	NextCursor     string                           `json:"next_cursor,omitempty"`
	Definitions    []sourceRepositoryDefinitionFile `json:"definitions"`
}

type sourceDefinitionRequest struct {
	Ref  string `json:"ref,omitempty"`
	Path string `json:"path"`
}

type resolvedSourceDefinitionResult struct {
	RepositoryID   string           `json:"repository_id"`
	DefinitionHash string           `json:"definition_hash"`
	Definition     json.RawMessage  `json:"definition"`
	Source         sourceProvenance `json:"source"`
}

type sourceRepositoryJobSummary struct {
	JobID     string           `json:"job_id"`
	Path      string           `json:"path"`
	Name      string           `json:"name"`
	BlobSHA   string           `json:"blob_sha"`
	SizeBytes int64            `json:"size_bytes,omitempty"`
	Source    sourceProvenance `json:"source"`
}

type invalidSourceRepositoryJobSummary struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	BlobSHA   string `json:"blob_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Error     string `json:"error"`
}

type sourceRepositoryJobsResult struct {
	RepositoryID   string                              `json:"repository_id"`
	RequestedRef   string                              `json:"requested_ref"`
	ResolvedCommit string                              `json:"resolved_commit"`
	Path           string                              `json:"path"`
	Limit          int                                 `json:"limit"`
	Truncated      bool                                `json:"truncated"`
	NextCursor     string                              `json:"next_cursor,omitempty"`
	Jobs           []sourceRepositoryJobSummary        `json:"jobs"`
	Invalid        []invalidSourceRepositoryJobSummary `json:"invalid,omitempty"`
}

type sourceScheduleSummary struct {
	ScheduleID     string                  `json:"schedule_id"`
	RepositoryID   string                  `json:"repository_id"`
	Namespace      string                  `json:"namespace"`
	JobID          string                  `json:"job_id"`
	CronSpec       string                  `json:"cron_spec"`
	NextRunAt      string                  `json:"next_run_at"`
	Ref            string                  `json:"ref,omitempty"`
	Path           string                  `json:"path,omitempty"`
	PathDerived    bool                    `json:"path_derived"`
	ConfiguredRef  string                  `json:"configured_ref"`
	ConfiguredPath string                  `json:"configured_path"`
	Override       *sourceScheduleOverride `json:"override,omitempty"`
	Declared       bool                    `json:"declared"`
	Enabled        bool                    `json:"enabled"`
}

type sourceScheduleOverride struct {
	Ref           string `json:"ref,omitempty"`
	Path          string `json:"path,omitempty"`
	Reason        string `json:"reason,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
}

type sourceSchedulesResult struct {
	Namespace    string                  `json:"namespace"`
	RepositoryID string                  `json:"repository_id,omitempty"`
	Schedules    []sourceScheduleSummary `json:"schedules"`
}

type sourceScheduleOverrideRequest struct {
	Ref    string `json:"ref,omitempty"`
	Path   string `json:"path,omitempty"`
	Reason string `json:"reason,omitempty"`
}

type sourceScheduleUpdateRequest struct {
	Enabled bool `json:"enabled"`
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

func sourceOverview(cmd *cobra.Command, args []string) {
	runCLIError(sourceOverviewWithOutput(os.Stdout))
}

func sourceOverviewWithOutput(out io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/source/status", nil)
	if err != nil {
		return fmt.Errorf("failed to create source overview request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to get source overview: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status getting source overview: %s", resp.Status)
	}

	var result sourceOverviewResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse source overview response: %w", err)
	}

	return writeSourceOverviewResult(out, result)
}

func writeSourceOverviewResult(out io.Writer, result sourceOverviewResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	fmt.Fprintf(out, "repositories_configured=%t\n", result.RepositoriesConfigured)
	fmt.Fprintf(out, "schedules_configured=%t\n", result.SchedulesConfigured)
	fmt.Fprintf(out, "declared_repositories=%d\n", result.DeclaredRepositories)
	fmt.Fprintf(out, "declared_schedules=%d\n", result.DeclaredSchedules)
	fmt.Fprintf(out, "repositories_total=%d\n", result.Repositories.Total)
	fmt.Fprintf(out, "repositories_enabled=%d\n", result.Repositories.Enabled)
	fmt.Fprintf(out, "repositories_disabled=%d\n", result.Repositories.Disabled)
	fmt.Fprintf(out, "repositories_declared=%d\n", result.Repositories.Declared)
	fmt.Fprintf(out, "repositories_stale_enabled=%d\n", result.Repositories.StaleEnabled)
	fmt.Fprintf(out, "repositories_stale_disabled=%d\n", result.Repositories.StaleDisabled)
	fmt.Fprintf(out, "repositories_sync_succeeded=%d\n", result.Repositories.SyncSucceeded)
	fmt.Fprintf(out, "repositories_sync_failed=%d\n", result.Repositories.SyncFailed)
	fmt.Fprintf(out, "repositories_sync_running=%d\n", result.Repositories.SyncRunning)
	fmt.Fprintf(out, "repositories_sync_never=%d\n", result.Repositories.SyncNever)
	fmt.Fprintf(out, "schedules_total=%d\n", result.Schedules.Total)
	fmt.Fprintf(out, "schedules_enabled=%d\n", result.Schedules.Enabled)
	fmt.Fprintf(out, "schedules_disabled=%d\n", result.Schedules.Disabled)
	fmt.Fprintf(out, "schedules_declared=%d\n", result.Schedules.Declared)
	fmt.Fprintf(out, "schedules_stale_enabled=%d\n", result.Schedules.StaleEnabled)
	fmt.Fprintf(out, "schedules_stale_disabled=%d\n", result.Schedules.StaleDisabled)
	fmt.Fprintf(out, "schedules_active_overrides=%d\n", result.Schedules.ActiveOverrides)
	return nil
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

	if sourceListStaleOnly {
		repositories = sourceRepositoriesWithoutDeclaration(repositories)
	}

	if outputIsJSON() {
		return writeJSON(out, repositories)
	}

	if len(repositories) == 0 {
		if sourceListStaleOnly {
			fmt.Fprintln(out, "No stale source repositories found")
		} else {
			fmt.Fprintln(out, "No source repositories found")
		}
		return nil
	}

	if sourceListQuiet {
		for _, repo := range repositories {
			fmt.Fprintln(out, repo.RepositoryID)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "REPOSITORY\tNAMESPACE\tCHECKOUT\tAUTHORING\tDECLARED\tENABLED\tSYNC\tDEFAULT REF")
	for _, repo := range repositories {
		defaultRef := repo.DefaultRef
		if defaultRef == "" {
			defaultRef = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%t\t%s\t%s\n",
			repo.RepositoryID,
			emptyAsDash(repo.Namespace),
			emptyAsDash(repo.CheckoutMode),
			emptyAsDash(repo.AuthoringMode),
			repo.Declared,
			repo.Enabled,
			emptyAsDash(repo.Sync.Status),
			defaultRef,
		)
	}
	return tw.Flush()
}

func sourceRepositoriesWithoutDeclaration(repositories []sourceRepositorySummary) []sourceRepositorySummary {
	filtered := repositories[:0]
	for _, repo := range repositories {
		if !repo.Declared {
			filtered = append(filtered, repo)
		}
	}
	return filtered
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

func getSource(cmd *cobra.Command, args []string) {
	runCLIError(getSourceWithOutput(os.Stdout, args[0]))
}

func getSourceWithOutput(out io.Writer, repositoryID string) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/source-repositories/"+url.PathEscape(repositoryID), nil)
	if err != nil {
		return fmt.Errorf("failed to create source repository get request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to get source repository: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var repo sourceRepositorySummary
		if err := json.NewDecoder(resp.Body).Decode(&repo); err != nil {
			return fmt.Errorf("failed to parse source repository response: %w", err)
		}
		return writeSourceRepositoryDetailResult(out, repo)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status getting source repository: %s", resp.Status)
	}
}

func updateSource(cmd *cobra.Command, args []string) {
	runCLIError(updateSourceWithOutput(cmd, os.Stdout, args[0]))
}

func updateSourceWithOutput(cmd *cobra.Command, out io.Writer, repositoryID string) error {
	payload := map[string]any{}
	flags := cmd.Flags()

	if flags.Changed("source-kind") {
		payload["source_kind"] = strings.TrimSpace(sourceUpdateSourceKind)
	}
	if flags.Changed("checkout-path") {
		payload["checkout_path"] = strings.TrimSpace(sourceUpdateCheckoutPath)
	}
	if flags.Changed("checkout-mode") {
		payload["checkout_mode"] = strings.TrimSpace(sourceUpdateCheckoutMode)
	}
	if flags.Changed("authoring-mode") {
		payload["authoring_mode"] = strings.TrimSpace(sourceUpdateAuthoringMode)
	}
	if flags.Changed("canonical-url") {
		payload["canonical_url"] = strings.TrimSpace(sourceUpdateCanonicalURL)
	}
	if flags.Changed("default-ref") {
		payload["default_ref"] = strings.TrimSpace(sourceUpdateDefaultRef)
	}
	if flags.Changed("credential-ref") {
		payload["credential_ref"] = strings.TrimSpace(sourceUpdateCredentialRef)
	}

	enableChanged := flags.Changed("enable")
	disableChanged := flags.Changed("disable")
	if enableChanged && disableChanged {
		return fmt.Errorf("--enable and --disable cannot be used together")
	}
	if enableChanged {
		payload["enabled"] = true
	}
	if disableChanged {
		payload["enabled"] = false
	}
	if len(payload) == 0 {
		return fmt.Errorf("at least one source repository update flag is required")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode source repository update: %w", err)
	}

	req, err := newAPIRequest(http.MethodPut, "/api/v1/source-repositories/"+url.PathEscape(repositoryID), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source repository update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to update source repository: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var repo sourceRepositorySummary
		if err := json.NewDecoder(resp.Body).Decode(&repo); err != nil {
			return fmt.Errorf("failed to parse source repository update response: %w", err)
		}
		return writeSourceRepositoryDetailResult(out, repo)
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source repository update")
	case http.StatusConflict:
		return fmt.Errorf("source repository %q conflicts with an existing checkout or update", repositoryID)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status updating source repository: %s", resp.Status)
	}
}

func deleteSource(cmd *cobra.Command, args []string) {
	runCLIError(deleteSourceWithOutput(os.Stdout, args[0], sourceDeleteYes))
}

func deleteSourceWithOutput(out io.Writer, repositoryID string, confirmed bool) error {
	if !confirmed {
		return fmt.Errorf("delete source repository %q requires --yes; this removes the registration but preserves checkout files", repositoryID)
	}

	req, err := newAPIRequest(http.MethodDelete, "/api/v1/source-repositories/"+url.PathEscape(repositoryID), nil)
	if err != nil {
		return fmt.Errorf("failed to create source repository delete request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to delete source repository: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			return writeJSON(out, map[string]string{"status": "deleted", "repository_id": repositoryID})
		}
		_, err := fmt.Fprintf(out, "Source repository %q deleted.\n", repositoryID)
		return err
	case http.StatusConflict:
		return fmt.Errorf("source repository %q is still declared, has source schedules, or has recorded source provenance; remove it from current config, disable it, or remove dependent references first", repositoryID)
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q not found", repositoryID)
	default:
		return fmt.Errorf("unexpected status deleting source repository: %s", resp.Status)
	}
}

func writeSourceRepositoryDetailResult(out io.Writer, repo sourceRepositorySummary) error {
	if outputIsJSON() {
		return writeJSON(out, repo)
	}

	fmt.Fprintf(out, "repository_id=%s\n", repo.RepositoryID)
	fmt.Fprintf(out, "namespace=%s\n", emptyAsDash(repo.Namespace))
	fmt.Fprintf(out, "source_kind=%s\n", emptyAsDash(repo.SourceKind))
	fmt.Fprintf(out, "checkout_mode=%s\n", emptyAsDash(repo.CheckoutMode))
	fmt.Fprintf(out, "authoring_mode=%s\n", emptyAsDash(repo.AuthoringMode))
	fmt.Fprintf(out, "declared=%t\n", repo.Declared)
	fmt.Fprintf(out, "write_definitions=%t\n", repo.Authoring.WriteDefinitions)
	fmt.Fprintf(out, "local_commits=%t\n", repo.Authoring.LocalCommits)
	fmt.Fprintf(out, "external_change_requests=%t\n", repo.Authoring.ExternalChangeRequests)
	if strings.TrimSpace(repo.Authoring.Reason) != "" {
		fmt.Fprintf(out, "authoring_reason=%s\n", repo.Authoring.Reason)
	}
	if strings.TrimSpace(repo.CheckoutPath) != "" {
		fmt.Fprintf(out, "checkout_path=%s\n", repo.CheckoutPath)
	}
	if strings.TrimSpace(repo.CanonicalURL) != "" {
		fmt.Fprintf(out, "canonical_url=%s\n", repo.CanonicalURL)
	}
	if strings.TrimSpace(repo.DefaultRef) != "" {
		fmt.Fprintf(out, "default_ref=%s\n", repo.DefaultRef)
	}
	if strings.TrimSpace(repo.CredentialRef) != "" {
		fmt.Fprintf(out, "credential_ref=%s\n", repo.CredentialRef)
	}
	fmt.Fprintf(out, "enabled=%t\n", repo.Enabled)
	if strings.TrimSpace(repo.Sync.Status) != "" {
		fmt.Fprintf(out, "sync_status=%s\n", repo.Sync.Status)
	}
	if strings.TrimSpace(repo.Sync.Ref) != "" {
		fmt.Fprintf(out, "sync_ref=%s\n", repo.Sync.Ref)
	}
	if strings.TrimSpace(repo.Sync.Commit) != "" {
		fmt.Fprintf(out, "sync_commit=%s\n", repo.Sync.Commit)
	}
	if strings.TrimSpace(repo.Sync.Error) != "" {
		fmt.Fprintf(out, "sync_error=%s\n", repo.Sync.Error)
	}

	return nil
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
	fmt.Fprintf(out, "declared=%t\n", result.Declared)
	fmt.Fprintf(out, "enabled=%t\n", result.Enabled)
	fmt.Fprintf(out, "checkout_mode=%s\n", emptyAsDash(result.CheckoutMode))
	fmt.Fprintf(out, "authoring_mode=%s\n", emptyAsDash(result.AuthoringMode))
	fmt.Fprintf(out, "write_definitions=%t\n", result.Authoring.WriteDefinitions)

	if strings.TrimSpace(result.Authoring.Reason) != "" {
		fmt.Fprintf(out, "authoring_reason=%s\n", result.Authoring.Reason)
	}

	if strings.TrimSpace(result.CredentialRef) != "" {
		fmt.Fprintf(out, "credential_ref=%s\n", result.CredentialRef)
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

func listSourceSchedules(cmd *cobra.Command, args []string) {
	repositoryID := ""
	if len(args) > 0 {
		repositoryID = args[0]
	}

	runCLIError(listSourceSchedulesWithOutput(os.Stdout, repositoryID))
}

func listSourceSchedulesWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-schedules"
	if strings.TrimSpace(repositoryID) != "" {
		path = "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/schedules"
	}

	params := url.Values{}
	if strings.TrimSpace(repositoryID) == "" {
		if v := strings.TrimSpace(sourceSchedulesNamespace); v != "" {
			params.Set("namespace", v)
		}
	}
	path = appendQueryParams(path, params)

	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("failed to create source schedules request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to list source schedules: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result sourceSchedulesResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source schedules response: %w", err)
		}
		if sourceSchedulesOverrideOnly {
			result.Schedules = sourceSchedulesWithOverrides(result.Schedules)
		}
		if sourceSchedulesStaleOnly {
			result.Schedules = sourceSchedulesWithoutDeclaration(result.Schedules)
		}
		return writeSourceSchedulesResult(out, result)
	case http.StatusNotFound:
		if strings.TrimSpace(repositoryID) != "" {
			return fmt.Errorf("source repository %q not found", repositoryID)
		}
		return fmt.Errorf("source schedules namespace not found")
	default:
		return fmt.Errorf("unexpected status listing source schedules: %s", resp.Status)
	}
}

func writeSourceSchedulesResult(out io.Writer, result sourceSchedulesResult) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if len(result.Schedules) == 0 {
		if strings.TrimSpace(result.RepositoryID) != "" {
			if sourceSchedulesStaleOnly {
				fmt.Fprintf(out, "No stale source schedules found for repository %s\n", result.RepositoryID)
			} else if sourceSchedulesOverrideOnly {
				fmt.Fprintf(out, "No source schedule overrides found for repository %s\n", result.RepositoryID)
			} else {
				fmt.Fprintf(out, "No source schedules found for repository %s\n", result.RepositoryID)
			}
		} else {
			if sourceSchedulesStaleOnly {
				fmt.Fprintln(out, "No stale source schedules found")
			} else if sourceSchedulesOverrideOnly {
				fmt.Fprintln(out, "No source schedule overrides found")
			} else {
				fmt.Fprintln(out, "No source schedules found")
			}
		}
		return nil
	}

	if sourceSchedulesQuiet {
		for _, schedule := range result.Schedules {
			fmt.Fprintln(out, schedule.ScheduleID)
		}
		return nil
	}

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCHEDULE\tREPOSITORY\tJOB\tCRON\tNEXT RUN\tREF\tPATH\tDECLARED\tOVERRIDE\tENABLED")
	for _, schedule := range result.Schedules {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%t\t%s\t%t\n",
			emptyAsDash(schedule.ScheduleID),
			emptyAsDash(schedule.RepositoryID),
			emptyAsDash(schedule.JobID),
			emptyAsDash(schedule.CronSpec),
			emptyAsDash(schedule.NextRunAt),
			emptyAsDash(schedule.Ref),
			sourceSchedulePathForDisplay(schedule),
			schedule.Declared,
			sourceScheduleOverrideForDisplay(schedule),
			schedule.Enabled,
		)
	}
	return tw.Flush()
}

func sourceSchedulesWithOverrides(schedules []sourceScheduleSummary) []sourceScheduleSummary {
	if len(schedules) == 0 {
		return nil
	}

	out := make([]sourceScheduleSummary, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule.Override != nil {
			out = append(out, schedule)
		}
	}
	return out
}

func sourceSchedulesWithoutDeclaration(schedules []sourceScheduleSummary) []sourceScheduleSummary {
	if len(schedules) == 0 {
		return nil
	}

	out := make([]sourceScheduleSummary, 0, len(schedules))
	for _, schedule := range schedules {
		if !schedule.Declared {
			out = append(out, schedule)
		}
	}
	return out
}

func sourceSchedulePathForDisplay(schedule sourceScheduleSummary) string {
	path := emptyAsDash(schedule.Path)
	if schedule.PathDerived && path != "-" {
		return path + " (derived)"
	}

	return path
}

func sourceScheduleOverrideForDisplay(schedule sourceScheduleSummary) string {
	if schedule.Override == nil {
		return "-"
	}

	parts := make([]string, 0, 3)
	if ref := strings.TrimSpace(schedule.Override.Ref); ref != "" {
		parts = append(parts, "ref="+ref)
	}
	if path := strings.TrimSpace(schedule.Override.Path); path != "" {
		parts = append(parts, "path="+path)
	}
	if reason := strings.TrimSpace(schedule.Override.Reason); reason != "" {
		parts = append(parts, "reason="+reason)
	}
	if len(parts) == 0 {
		return "active"
	}

	return strings.Join(parts, " ")
}

func setSourceScheduleOverride(cmd *cobra.Command, args []string) {
	runCLIError(setSourceScheduleOverrideWithOutput(os.Stdout, args[0]))
}

func setSourceScheduleOverrideWithOutput(out io.Writer, scheduleID string) error {
	payload := sourceScheduleOverrideRequest{
		Ref:    strings.TrimSpace(sourceOverrideRef),
		Path:   strings.TrimSpace(sourceOverridePath),
		Reason: strings.TrimSpace(sourceOverrideReason),
	}
	if payload.Ref == "" && payload.Path == "" {
		return fmt.Errorf("--ref or --path is required")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to encode source schedule override: %w", err)
	}

	req, err := newAPIRequest(http.MethodPut, "/api/v1/source-schedules/"+url.PathEscape(scheduleID)+"/override", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source schedule override request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to set source schedule override: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var schedule sourceScheduleSummary
		if err := json.NewDecoder(resp.Body).Decode(&schedule); err != nil {
			return fmt.Errorf("failed to parse source schedule override response: %w", err)
		}
		return writeSourceScheduleOverrideSetResult(out, schedule)
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source schedule override")
	case http.StatusNotFound:
		return fmt.Errorf("source schedule %q not found", scheduleID)
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status setting source schedule override: %s", resp.Status)
	}
}

func clearSourceScheduleOverride(cmd *cobra.Command, args []string) {
	runCLIError(clearSourceScheduleOverrideWithOutput(os.Stdout, args[0]))
}

func clearSourceScheduleOverrideWithOutput(out io.Writer, scheduleID string) error {
	req, err := newAPIRequest(http.MethodDelete, "/api/v1/source-schedules/"+url.PathEscape(scheduleID)+"/override", nil)
	if err != nil {
		return fmt.Errorf("failed to create source schedule override clear request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to clear source schedule override: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var schedule sourceScheduleSummary
		if err := json.NewDecoder(resp.Body).Decode(&schedule); err != nil {
			return fmt.Errorf("failed to parse source schedule override clear response: %w", err)
		}
		return writeSourceScheduleOverrideClearResult(out, schedule)
	case http.StatusNotFound:
		return fmt.Errorf("source schedule %q not found", scheduleID)
	default:
		return fmt.Errorf("unexpected status clearing source schedule override: %s", resp.Status)
	}
}

func writeSourceScheduleOverrideSetResult(out io.Writer, schedule sourceScheduleSummary) error {
	if outputIsJSON() {
		return writeJSON(out, schedule)
	}

	fmt.Fprintf(out, "Source schedule %q override set.\n", schedule.ScheduleID)
	fmt.Fprintf(out, "ref=%s\n", emptyAsDash(schedule.Ref))
	fmt.Fprintf(out, "path=%s\n", emptyAsDash(schedule.Path))
	if schedule.Override != nil && strings.TrimSpace(schedule.Override.Reason) != "" {
		fmt.Fprintf(out, "reason=%s\n", schedule.Override.Reason)
	}
	return nil
}

func writeSourceScheduleOverrideClearResult(out io.Writer, schedule sourceScheduleSummary) error {
	if outputIsJSON() {
		return writeJSON(out, schedule)
	}

	_, err := fmt.Fprintf(out, "Source schedule %q override cleared.\n", schedule.ScheduleID)
	return err
}

func enableSourceSchedule(cmd *cobra.Command, args []string) {
	runCLIError(setSourceScheduleEnabledWithOutput(os.Stdout, args[0], true))
}

func disableSourceSchedule(cmd *cobra.Command, args []string) {
	runCLIError(setSourceScheduleEnabledWithOutput(os.Stdout, args[0], false))
}

func deleteSourceSchedule(cmd *cobra.Command, args []string) {
	runCLIError(deleteSourceScheduleWithOutput(os.Stdout, args[0], sourceDeleteScheduleYes))
}

func setSourceScheduleEnabledWithOutput(out io.Writer, scheduleID string, enabled bool) error {
	body, err := json.Marshal(sourceScheduleUpdateRequest{Enabled: enabled})
	if err != nil {
		return fmt.Errorf("failed to encode source schedule update: %w", err)
	}

	req, err := newAPIRequest(http.MethodPatch, "/api/v1/source-schedules/"+url.PathEscape(scheduleID), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source schedule update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to update source schedule: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var schedule sourceScheduleSummary
		if err := json.NewDecoder(resp.Body).Decode(&schedule); err != nil {
			return fmt.Errorf("failed to parse source schedule update response: %w", err)
		}
		return writeSourceScheduleEnabledResult(out, schedule, enabled)
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source schedule update")
	case http.StatusNotFound:
		return fmt.Errorf("source schedule %q not found", scheduleID)
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status updating source schedule: %s", resp.Status)
	}
}

func writeSourceScheduleEnabledResult(out io.Writer, schedule sourceScheduleSummary, enabled bool) error {
	if outputIsJSON() {
		return writeJSON(out, schedule)
	}

	state := "disabled"
	if enabled {
		state = "enabled"
	}

	_, err := fmt.Fprintf(out, "Source schedule %q %s.\n", schedule.ScheduleID, state)
	return err
}

func deleteSourceScheduleWithOutput(out io.Writer, scheduleID string, confirmed bool) error {
	if !confirmed {
		return fmt.Errorf("delete source schedule %q requires --yes; only stale disabled schedules without overrides can be deleted", scheduleID)
	}

	req, err := newAPIRequest(http.MethodDelete, "/api/v1/source-schedules/"+url.PathEscape(scheduleID), nil)
	if err != nil {
		return fmt.Errorf("failed to create source schedule delete request: %w", err)
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to delete source schedule: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNoContent:
		if outputIsJSON() {
			return writeJSON(out, map[string]string{"status": "deleted", "schedule_id": scheduleID})
		}
		_, err := fmt.Fprintf(out, "Source schedule %q deleted.\n", scheduleID)
		return err
	case http.StatusConflict:
		return fmt.Errorf("source schedule %q must be stale, disabled, and clear of overrides before deletion", scheduleID)
	case http.StatusNotFound:
		return fmt.Errorf("source schedule %q not found", scheduleID)
	default:
		return fmt.Errorf("unexpected status deleting source schedule: %s", resp.Status)
	}
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
	if err := tw.Flush(); err != nil {
		return err
	}

	return writeSourceTruncatedNotice(out, result.Truncated, result.Limit, "")
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
	if v := strings.TrimSpace(sourceTreeCursor); v != "" {
		params.Set("cursor", v)
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
	if err := tw.Flush(); err != nil {
		return err
	}

	return writeSourceTruncatedNotice(out, result.Truncated, result.Limit, result.NextCursor)
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
	if v := strings.TrimSpace(sourceDefinitionsCursor); v != "" {
		params.Set("cursor", v)
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
	if err := tw.Flush(); err != nil {
		return err
	}

	return writeSourceTruncatedNotice(out, result.Truncated, result.Limit, result.NextCursor)
}

func resolveSourceDefinition(cmd *cobra.Command, args []string) {
	runCLIError(resolveSourceDefinitionWithOutput(cmd, os.Stdout, args[0], args[1]))
}

func resolveSourceDefinitionWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, definitionPath string) error {
	body, err := json.Marshal(sourceDefinitionRequest{
		Ref:  strings.TrimSpace(sourceResolveRef),
		Path: strings.TrimSpace(definitionPath),
	})
	if err != nil {
		return fmt.Errorf("failed to encode source definition resolve request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, "/api/v1/source-repositories/"+url.PathEscape(repositoryID)+"/definitions/resolve", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source definition resolve request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("failed to resolve source definition: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var result resolvedSourceDefinitionResult
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to parse source definition resolve response: %w", err)
		}
		if outputIsJSON() {
			return writeJSON(out, result)
		}
		raw, _ := cmd.Flags().GetBool("raw")
		_, err := out.Write(formatJobDefinitionBody(result.Definition, !raw))
		return err
	case http.StatusBadRequest:
		return fmt.Errorf("invalid source definition resolve request")
	case http.StatusNotFound:
		return fmt.Errorf("source repository %q or definition path %q not found", repositoryID, definitionPath)
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("source definition is too large")
	case http.StatusUnsupportedMediaType:
		return fmt.Errorf("content type must be application/json")
	default:
		return fmt.Errorf("unexpected status resolving source definition: %s", resp.Status)
	}
}

func listSourceJobs(cmd *cobra.Command, args []string) {
	runCLIError(listSourceJobsWithOutput(os.Stdout, args[0]))
}

func listSourceJobsWithOutput(out io.Writer, repositoryID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs"
	params := url.Values{}
	setTrimmedQueryParam(params, "ref", sourceJobsRef)
	setTrimmedQueryParam(params, "path", sourceJobsPath)
	setTrimmedQueryParam(params, "cursor", sourceJobsCursor)
	setPositiveIntQueryParam(params, "limit", sourceJobsLimit)

	return listSourceJobsPathWithOutput(out, appendQueryParams(path, params), sourceJobsQuiet)
}

func listSourceJobsFromJobsFacadeWithOutput(out io.Writer, opts jobListOptions) error {
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", opts.RepositoryID)
	setTrimmedQueryParam(params, "ref", opts.Ref)
	setTrimmedQueryParam(params, "path", opts.Path)
	setTrimmedQueryParam(params, "cursor", opts.Cursor)
	setPositiveIntQueryParam(params, "limit", opts.Limit)

	return listSourceJobsPathWithOutput(out, appendQueryParams("/api/v1/jobs", params), opts.Quiet)
}

func listSourceJobsPathWithOutput(out io.Writer, path string, quiet bool) error {
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
		return writeSourceJobsResult(out, result, quiet)
	case http.StatusNotFound:
		return fmt.Errorf("source repository or source jobs not found")
	default:
		return fmt.Errorf("unexpected status listing source jobs: %s", resp.Status)
	}
}

func writeSourceJobsResult(out io.Writer, result sourceRepositoryJobsResult, quiet bool) error {
	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if quiet {
		for _, job := range result.Jobs {
			fmt.Fprintln(out, job.JobID)
		}
		return nil
	}

	if len(result.Jobs) == 0 && len(result.Invalid) == 0 {
		fmt.Fprintln(out, "No source jobs found")
		return nil
	}

	if len(result.Jobs) > 0 {
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
		if err := tw.Flush(); err != nil {
			return err
		}
	}

	if len(result.Invalid) > 0 {
		if len(result.Jobs) > 0 {
			fmt.Fprintln(out)
		}
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "INVALID PATH\tBLOB\tERROR")
		for _, invalid := range result.Invalid {
			fmt.Fprintf(tw, "%s\t%s\t%s\n",
				emptyAsDash(invalid.Path),
				shortSHA(invalid.BlobSHA),
				emptyAsDash(invalid.Error),
			)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}

	return writeSourceTruncatedNotice(out, result.Truncated, result.Limit, result.NextCursor)
}

func writeSourceTruncatedNotice(out io.Writer, truncated bool, limit int, nextCursor string) error {
	if !truncated {
		return nil
	}

	if nextCursor != "" {
		_, err := fmt.Fprintf(out, "Results truncated at limit=%d. Continue with --cursor %s.\n", limit, nextCursor)
		return err
	}

	_, err := fmt.Fprintf(out, "Results truncated at limit=%d; narrow the path/ref or increase --limit.\n", limit)
	return err
}

func showSourceJob(cmd *cobra.Command, args []string) {
	runCLIError(showSourceJobWithOutput(cmd, os.Stdout, args[0], args[1]))
}

func showSourceJobWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	path := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs/" + url.PathEscape(jobID) + "/definition"
	params := url.Values{}
	setTrimmedQueryParam(params, "ref", sourceShowRef)
	setTrimmedQueryParam(params, "path", sourceShowPath)

	return showSourceJobPathWithOutput(cmd, out, appendQueryParams(path, params), repositoryID, jobID)
}

func showSourceJobFromJobsFacadeWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	params := url.Values{}
	setTrimmedQueryParam(params, "repository_id", repositoryID)
	setTrimmedQueryParam(params, "ref", stringFlagValue(cmd, "ref"))
	setTrimmedQueryParam(params, "path", stringFlagValue(cmd, "path"))

	path := appendQueryParams("/api/v1/jobs/"+url.PathEscape(jobID), params)
	return showSourceJobPathWithOutput(cmd, out, path, repositoryID, jobID)
}

func showSourceJobPathWithOutput(cmd *cobra.Command, out io.Writer, path, repositoryID, jobID string) error {
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
	apiPath := "/api/v1/source-repositories/" + url.PathEscape(repositoryID) + "/jobs/" + url.PathEscape(jobID) + "/trigger"
	return triggerSourceJobPathWithOutput(cmd, out, apiPath, repositoryID, jobID, "", sourceTriggerRef, sourceTriggerPath, sourceTriggerCellID, sourceTriggerIdemKey)
}

func triggerSourceJobFromJobsFacadeWithOutput(cmd *cobra.Command, out io.Writer, repositoryID, jobID string) error {
	cellID, err := singleSourceTriggerCellIDFromCells(triggerCellIDs)
	if err != nil {
		return err
	}

	apiPath := "/api/v1/jobs/trigger/" + url.PathEscape(jobID)
	return triggerSourceJobPathWithOutput(cmd, out, apiPath, repositoryID, jobID, repositoryID, stringFlagValue(cmd, "ref"), stringFlagValue(cmd, "path"), cellID, triggerIdemKey)
}

func triggerSourceJobPathWithOutput(cmd *cobra.Command, out io.Writer, apiPath, repositoryID, jobID, bodyRepositoryID, ref, definitionPath, cellID, idempotencyKey string) error {
	body, err := json.Marshal(sourceTriggerRequest{
		RepositoryID: strings.TrimSpace(bodyRepositoryID),
		Ref:          strings.TrimSpace(ref),
		Path:         strings.TrimSpace(definitionPath),
		CellID:       strings.TrimSpace(cellID),
	})
	if err != nil {
		return fmt.Errorf("failed to encode source trigger request: %w", err)
	}

	req, err := newAPIRequest(http.MethodPost, apiPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create source trigger request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	setIdempotencyHeader(req, idempotencyKey)

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
	RepositoryID string `json:"repository_id,omitempty"`
	Ref          string `json:"ref,omitempty"`
	Path         string `json:"path,omitempty"`
	CellID       string `json:"cell_id,omitempty"`
}

func setPositiveIntQueryParam(params url.Values, key string, value int) {
	if value > 0 {
		params.Set(key, strconv.Itoa(value))
	}
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

func versionAsDash(version int) string {
	if version <= 0 {
		return "-"
	}
	return strconv.Itoa(version)
}

var sourcesCmd = &cobra.Command{
	Use:     "sources",
	Short:   "Work with source repositories",
	Long:    `Register, sync, browse, and administer source repositories. Use jobs --repository for the primary source-backed job workflow.`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var sourcesOverviewCmd = &cobra.Command{
	Use:   "overview",
	Short: "Show source-control readiness",
	Long:  `Show config-as-code source wiring, declaration counts, and persisted repository and schedule summaries.`,
	Args:  cobra.NoArgs,
	Run:   sourceOverview,
}

var sourcesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List source repositories",
	Long:  `Fetch registered source repositories and print their checkout, authoring, declared, enabled, and sync state. Use --stale to show rows missing from current API config.`,
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

var sourcesGetCmd = &cobra.Command{
	Use:   "get [repository-id]",
	Short: "Show a source repository registration",
	Long:  `Fetch one source repository registration and print its checkout, authoring, declared, enabled, and sync metadata.`,
	Args:  cobra.ExactArgs(1),
	Run:   getSource,
}

var sourcesUpdateCmd = &cobra.Command{
	Use:   "update [repository-id]",
	Short: "Update a source repository registration",
	Long:  `Update mutable source repository registration fields such as checkout path, checkout mode, authoring mode, canonical URL, default ref, credential ref, and enabled state.`,
	Args:  cobra.ExactArgs(1),
	Run:   updateSource,
}

var sourcesDeleteCmd = &cobra.Command{
	Use:   "delete [repository-id]",
	Short: "Delete a source repository registration",
	Long:  `Delete a source repository registration. Checkout files are left untouched, and repositories still declared in config or referenced by source schedules or recorded source provenance must be disabled instead of deleted.`,
	Args:  cobra.ExactArgs(1),
	Run:   deleteSource,
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

var sourcesSchedulesCmd = &cobra.Command{
	Use:   "schedules [repository-id]",
	Short: "List source-backed cron schedules",
	Long:  `List source-backed cron schedules reconciled from configuration. Pass a repository ID to show schedules for that repository only.`,
	Args:  cobra.RangeArgs(0, 1),
	Run:   listSourceSchedules,
}

var sourcesOverrideCmd = &cobra.Command{
	Use:   "override [schedule-id]",
	Short: "Set a source schedule hotfix override",
	Long:  `Set a temporary source ref or path override for a source-backed cron schedule. Clear it once the fix lands in the configured repository path.`,
	Args:  cobra.ExactArgs(1),
	Run:   setSourceScheduleOverride,
}

var sourcesClearOverrideCmd = &cobra.Command{
	Use:   "clear-override [schedule-id]",
	Short: "Clear a source schedule hotfix override",
	Long:  `Clear a source-backed cron schedule override so future runs return to the configured ref and path.`,
	Args:  cobra.ExactArgs(1),
	Run:   clearSourceScheduleOverride,
}

var sourcesEnableScheduleCmd = &cobra.Command{
	Use:   "enable-schedule [schedule-id]",
	Short: "Enable a source-backed cron schedule",
	Long:  `Enable a source-backed cron schedule row. Config reconciliation may update this again when the schedule is declared in source schedule config.`,
	Args:  cobra.ExactArgs(1),
	Run:   enableSourceSchedule,
}

var sourcesDisableScheduleCmd = &cobra.Command{
	Use:   "disable-schedule [schedule-id]",
	Short: "Disable a source-backed cron schedule",
	Long:  `Disable a source-backed cron schedule row without deleting it. This is useful when a stale schedule has been removed from source schedule config and should stop firing.`,
	Args:  cobra.ExactArgs(1),
	Run:   disableSourceSchedule,
}

var sourcesDeleteScheduleCmd = &cobra.Command{
	Use:   "delete-schedule [schedule-id]",
	Short: "Delete a stale source-backed cron schedule",
	Long:  `Delete a stale source-backed cron schedule row after it has been removed from source schedule config, disabled, and cleared of overrides.`,
	Args:  cobra.ExactArgs(1),
	Run:   deleteSourceSchedule,
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

var sourcesResolveCmd = &cobra.Command{
	Use:   "resolve [repository-id] [definition-path]",
	Short: "Resolve a source definition file",
	Long:  `Resolve and validate a source definition file at a ref and print the canonical JSON without storing it.`,
	Args:  cobra.ExactArgs(2),
	Run:   resolveSourceDefinition,
}

var sourcesJobsCmd = &cobra.Command{
	Use:   "jobs [repository-id]",
	Short: "List jobs discovered in a source repository",
	Long:  `List triggerable source jobs derived from repository definition paths without creating reusable job rows.`,
	Args:  cobra.ExactArgs(1),
	Run:   listSourceJobs,
}

var sourcesShowCmd = &cobra.Command{
	Use:   "show [repository-id] [job-id]",
	Short: "Show a source job definition",
	Long:  `Resolve a source repository job definition at a ref and print the canonical JSON without creating a reusable job row.`,
	Args:  cobra.ExactArgs(2),
	Run:   showSourceJob,
}

var sourcesWriteCmd = &cobra.Command{
	Use:   "write [repository-id] [job-id] [definition-file]",
	Short: "Write a source job definition",
	Long:  `Write a JSON job definition into a source repository checkout without creating a reusable job row. Use "-" as definition-file to read from stdin.`,
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
	Long:  `Stream logs for a source repository job run using source provenance to disambiguate runs from non-source runs with the same job ID. Omit run-id to stream the latest source run, or add --follow to wait for future source runs.`,
	Args:  cobra.RangeArgs(2, 3),
	Run:   runSourceLogs,
}

var sourcesTriggerCmd = &cobra.Command{
	Use:   "trigger [repository-id] [job-id]",
	Short: "Trigger a job directly from source",
	Long:  `Trigger a source repository job without creating or requiring a reusable job row.`,
	Args:  cobra.ExactArgs(2),
	Run:   triggerSourceJob,
}

func configureSourcesListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceListNamespace, "namespace", "", "Namespace to list source repositories from (default: /)")
	cmd.Flags().BoolVarP(&sourceListQuiet, "quiet", "q", false, "Print only repository IDs")
	cmd.Flags().BoolVar(&sourceListStaleOnly, "stale", false, "Show only source repositories missing from current API config")
}

func configureSourcesSchedulesFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceSchedulesNamespace, "namespace", "", "Namespace to list source schedules from when no repository is specified (default: /)")
	cmd.Flags().BoolVarP(&sourceSchedulesQuiet, "quiet", "q", false, "Print only schedule IDs")
	cmd.Flags().BoolVar(&sourceSchedulesOverrideOnly, "overrides", false, "Show only schedules with active source overrides")
	cmd.Flags().BoolVar(&sourceSchedulesStaleOnly, "stale", false, "Show only source schedules missing from current API config")
}

func configureSourcesOverrideFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceOverrideRef, "ref", "", "Override git ref")
	cmd.Flags().StringVar(&sourceOverridePath, "path", "", "Override definition file path")
	cmd.Flags().StringVar(&sourceOverrideReason, "reason", "", "Reason for the temporary override")
}

func configureSourcesRegisterFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceRegisterNamespace, "namespace", "", "Namespace to register the repository in (default: /)")
	cmd.Flags().StringVar(&sourceRegisterCheckoutMode, "checkout-mode", "", "Checkout mode: external or managed")
	cmd.Flags().StringVar(&sourceRegisterAuthoringMode, "authoring-mode", "", "Authoring mode: read_only, local_commit, or external_change_request")
	cmd.Flags().StringVar(&sourceRegisterCanonicalURL, "canonical-url", "", "Canonical clone/fetch URL for managed checkouts")
	cmd.Flags().StringVar(&sourceRegisterDefaultRef, "default-ref", "", "Default git ref for source operations")
	cmd.Flags().StringVar(&sourceRegisterCredentialRef, "credential-ref", "", "Credential reference for managed Git checkout sync")
	cmd.Flags().BoolVar(&sourceRegisterDisabled, "disabled", false, "Register the repository disabled")
}

func configureSourcesUpdateFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceUpdateSourceKind, "source-kind", "", "Source kind, currently local_checkout")
	cmd.Flags().StringVar(&sourceUpdateCheckoutPath, "checkout-path", "", "Checkout path for external repositories")
	cmd.Flags().StringVar(&sourceUpdateCheckoutMode, "checkout-mode", "", "Checkout mode: external or managed")
	cmd.Flags().StringVar(&sourceUpdateAuthoringMode, "authoring-mode", "", "Authoring mode: read_only, local_commit, or external_change_request")
	cmd.Flags().StringVar(&sourceUpdateCanonicalURL, "canonical-url", "", "Canonical clone/fetch URL for managed checkouts")
	cmd.Flags().StringVar(&sourceUpdateDefaultRef, "default-ref", "", "Default git ref for source operations")
	cmd.Flags().StringVar(&sourceUpdateCredentialRef, "credential-ref", "", "Credential reference for managed Git checkout sync")
	cmd.Flags().BoolVar(&sourceUpdateEnable, "enable", false, "Enable the source repository")
	cmd.Flags().BoolVar(&sourceUpdateDisable, "disable", false, "Disable the source repository")
}

func configureSourcesDeleteFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&sourceDeleteYes, "yes", false, "Skip confirmation prompt")
}

func configureSourcesDeleteScheduleFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&sourceDeleteScheduleYes, "yes", false, "Skip confirmation prompt")
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
	cmd.Flags().StringVar(&sourceTreeCursor, "cursor", "", "Continue after a previous tree entry path")
	cmd.Flags().BoolVarP(&sourceTreeRecursive, "recursive", "r", false, "List tree entries recursively")
	cmd.Flags().BoolVarP(&sourceTreeQuiet, "quiet", "q", false, "Print only paths")
}

func configureSourcesDefinitionsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceDefinitionsRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceDefinitionsPath, "path", "", "Definition directory path (default: .vectis/jobs)")
	cmd.Flags().IntVar(&sourceDefinitionsLimit, "limit", 0, "Max definition files to return")
	cmd.Flags().StringVar(&sourceDefinitionsCursor, "cursor", "", "Continue after a previous definition file path")
	cmd.Flags().BoolVarP(&sourceDefinitionsQuiet, "quiet", "q", false, "Print only definition paths")
}

func configureSourcesResolveFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceResolveRef, "ref", "", "Git ref to resolve (default: repository default_ref or HEAD)")
	cmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
}

func configureSourcesJobsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&sourceJobsRef, "ref", "", "Git ref to inspect (default: repository default_ref or HEAD)")
	cmd.Flags().StringVar(&sourceJobsPath, "path", "", "Definition directory path (default: .vectis/jobs)")
	cmd.Flags().IntVar(&sourceJobsLimit, "limit", 0, "Max source jobs to return")
	cmd.Flags().StringVar(&sourceJobsCursor, "cursor", "", "Continue after a previous definition file path")
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
