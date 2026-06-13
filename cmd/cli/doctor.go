package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"vectis/internal/config"
	"vectis/internal/utils"
)

type doctorStatus string

const (
	doctorOK   doctorStatus = "pass"
	doctorWarn doctorStatus = "warn"
	doctorFail doctorStatus = "fail"
)

type doctorSeverity string

const (
	severityCritical doctorSeverity = "critical"
	severityWarning  doctorSeverity = "warning"
)

type doctorCheck struct {
	ID              string         `json:"id"`
	Title           string         `json:"title"`
	Status          doctorStatus   `json:"status"`
	Severity        doctorSeverity `json:"severity"`
	Summary         string         `json:"summary"`
	Evidence        string         `json:"evidence,omitempty"`
	SuggestedAction string         `json:"action,omitempty"`
	DocLink         string         `json:"doc,omitempty"`
	apiAuthEnabled  bool
}

type doctorReport struct {
	Status   doctorStatus  `json:"status"`
	Passed   int           `json:"passed"`
	Warnings int           `json:"warnings"`
	Failed   int           `json:"failed"`
	Checks   []doctorCheck `json:"checks"`
}

var doctorJSON bool
var doctorStrict bool

const (
	doctorDiskWarnFreeBytes  = 1 << 30
	doctorCertExpiryWarn     = 14 * 24 * time.Hour
	doctorCatalogPendingWarn = 100
)

func runDoctor(cmd *cobra.Command, args []string) {
	doctorJSON, _ = cmd.Flags().GetBool("json")
	doctorJSON = doctorJSON || outputIsJSON()
	doctorStrict, _ = cmd.Flags().GetBool("strict")

	runCLIError(doctor(os.Stdout))
}

func doctor(w io.Writer) error {
	setupStatus := doctorSetupStatus()
	checks := []doctorCheck{
		doctorHTTPStatus("api.live", http.MethodGet, "/health/live", http.StatusOK, "API liveness probe passed", severityCritical, "API liveness", "Check API server process", "website/docs/operating/reliability/runbooks.md"),
		doctorHTTPStatus("api.ready", http.MethodGet, "/health/ready", http.StatusOK, "API readiness probe passed", severityCritical, "API readiness", "Check API server and dependencies (DB, queue)", "website/docs/operating/reliability/runbooks.md"),
		setupStatus,
		doctorCLIToken(setupStatus.apiAuthEnabled),
		doctorSchemaCurrent(),
		doctorReconcilerActive(),
		doctorAuditDrops(),
		doctorDBPool(),
		doctorQueueBacklog(),
		doctorCronSchedules(),
		doctorStuckRuns(),
		doctorCellIngressRoutes(),
		doctorCatalogInbox(),
		doctorLogReachable(),
		doctorAuditFlushFailures(),
		doctorTLSFiles(),
		doctorFilesystemPressure("queue.persistence.filesystem", "Queue persistence filesystem", "queue persistence", envOrDefaultAllowEmpty("VECTIS_QUEUE_PERSISTENCE_DIR", defaultDoctorQueuePersistenceDir())),
		doctorFilesystemPressure("log.storage.filesystem", "Log storage filesystem", "log storage", envOrDefault("VECTIS_LOG_STORAGE_DIR", defaultDoctorLogStorageDir())),
		doctorFilesystemPressure("log.forwarder.spool.filesystem", "Log forwarder spool filesystem", "log-forwarder spool", envOrDefault("VECTIS_LOG_FORWARDER_SPOOL_DIR", defaultDoctorForwarderSpoolDir())),
		doctorFilesystemPressure("artifact.storage.filesystem", "Artifact storage filesystem", "artifact storage", envOrDefault("VECTIS_ARTIFACT_STORAGE_DIR", defaultDoctorArtifactStorageDir())),
	}

	if doctorJSON {
		return writeDoctorJSON(w, checks)
	}

	return writeDoctorText(w, checks)
}

func writeDoctorText(w io.Writer, checks []doctorCheck) error {
	passed, warned, failed := doctorStatusCounts(checks)
	fmt.Fprintln(w, "Vectis health check")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Overall: %s  %d passed, %d warnings, %d failed\n", doctorOverallStatus(failed, warned), passed, warned, failed)

	checkByID := make(map[string]doctorCheck, len(checks))
	for _, check := range checks {
		checkByID[check.ID] = check
	}

	for _, group := range doctorTextGroups {
		wroteHeader := false
		for _, item := range group.Items {
			check, ok := checkByID[item.ID]
			if !ok {
				continue
			}

			if !wroteHeader {
				fmt.Fprintln(w)
				fmt.Fprintln(w, group.Name)
				wroteHeader = true
			}

			fmt.Fprintf(w, "  %-5s %-30s %s\n", doctorDisplayStatus(check.Status), item.Label, check.Summary)
		}
	}

	return evaluateDoctorChecks(checks)
}

type doctorTextGroup struct {
	Name  string
	Items []doctorTextItem
}

type doctorTextItem struct {
	ID    string
	Label string
}

var doctorTextGroups = []doctorTextGroup{
	{Name: "Core", Items: []doctorTextItem{
		{ID: "api.live", Label: "API liveness"},
		{ID: "api.ready", Label: "API readiness"},
		{ID: "setup.status", Label: "Initial setup"},
		{ID: "cli.token", Label: "CLI token"},
	}},
	{Name: "Database", Items: []doctorTextItem{
		{ID: "db.schema.current", Label: "Schema"},
		{ID: "db.connection.pool", Label: "Connection pool"},
	}},
	{Name: "Queue", Items: []doctorTextItem{
		{ID: "queue.backlog.ratio", Label: "Backlog"},
		{ID: "queue.persistence.filesystem", Label: "Persistence filesystem"},
	}},
	{Name: "Cron", Items: []doctorTextItem{
		{ID: "cron.schedules", Label: "Schedules"},
	}},
	{Name: "Reconciler", Items: []doctorTextItem{
		{ID: "reconciler.active", Label: "Recovery activity"},
		{ID: "reconciler.stuck.runs", Label: "Stuck runs"},
	}},
	{Name: "Cells", Items: []doctorTextItem{
		{ID: "cells.ingress", Label: "Ingress routes"},
	}},
	{Name: "Catalog", Items: []doctorTextItem{
		{ID: "catalog.inbox", Label: "Cell event inbox"},
	}},
	{Name: "Logging", Items: []doctorTextItem{
		{ID: "log.reachable", Label: "Log service"},
		{ID: "log.storage.filesystem", Label: "Log storage"},
		{ID: "log.forwarder.spool.filesystem", Label: "Forwarder spool"},
	}},
	{Name: "Artifacts", Items: []doctorTextItem{
		{ID: "artifact.storage.filesystem", Label: "Artifact storage"},
	}},
	{Name: "Audit", Items: []doctorTextItem{
		{ID: "audit.drops.recent", Label: "Recent drops"},
		{ID: "audit.flush.failures", Label: "Flush failures"},
	}},
	{Name: "TLS", Items: []doctorTextItem{
		{ID: "tls.files", Label: "Files"},
	}},
}

func doctorStatusCounts(checks []doctorCheck) (passed, warned, failed int) {
	for _, check := range checks {
		switch check.Status {
		case doctorOK:
			passed++
		case doctorWarn:
			warned++
		case doctorFail:
			failed++
		}
	}

	return passed, warned, failed
}

func doctorOverallStatus(failed, warned int) string {
	if failed > 0 {
		return "FAIL"
	}

	if warned > 0 {
		return "WARN"
	}

	return "PASS"
}

func doctorOverallJSONStatus(failed, warned int) doctorStatus {
	if failed > 0 {
		return doctorFail
	}

	if warned > 0 {
		return doctorWarn
	}

	return doctorOK
}

func doctorDisplayStatus(status doctorStatus) string {
	switch status {
	case doctorOK:
		return "OK"
	case doctorWarn:
		return "WARN"
	case doctorFail:
		return "FAIL"
	default:
		return strings.ToUpper(string(status))
	}
}

func evaluateDoctorChecks(checks []doctorCheck) error {
	failed := false
	warned := false

	for _, check := range checks {
		if check.Status == doctorFail {
			failed = true
		}
		if check.Status == doctorWarn {
			warned = true
		}
	}

	if failed {
		return fmt.Errorf("one or more health checks failed")
	}

	if doctorStrict && warned {
		return fmt.Errorf("one or more health checks reported warnings (--strict)")
	}

	return nil
}

func writeDoctorJSON(w io.Writer, checks []doctorCheck) error {
	passed, warned, failed := doctorStatusCounts(checks)
	report := doctorReport{
		Status:   doctorOverallJSONStatus(failed, warned),
		Passed:   passed,
		Warnings: warned,
		Failed:   failed,
		Checks:   checks,
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		return err
	}

	return evaluateDoctorChecks(checks)
}

func doctorHTTPStatus(id, method, path string, want int, okMessage string, severity doctorSeverity, title, action, doc string) doctorCheck {
	req, err := newAPIRequest(method, path, nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: err.Error(), SuggestedAction: action, DocLink: doc}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: action, DocLink: doc}
	}
	defer resp.Body.Close()

	if resp.StatusCode != want {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severity, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: action, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severity, Summary: okMessage, DocLink: doc}
}

func doctorSetupStatus() doctorCheck {
	const id = "setup.status"
	title := "Setup complete"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/setup/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reliability/runbooks.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reliability/runbooks.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reliability/runbooks.md"}
	}

	var result struct {
		SetupComplete bool  `json:"setup_complete"`
		AuthEnabled   *bool `json:"auth_enabled"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reliability/runbooks.md"}
	}

	authEnabled := config.APIAuthEnabled()
	if result.AuthEnabled != nil {
		authEnabled = *result.AuthEnabled
	}

	if !authEnabled {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "initial setup not required; API auth is disabled", DocLink: "website/docs/operating/reliability/runbooks.md", apiAuthEnabled: false}
	}

	if result.SetupComplete {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "initial setup is complete", DocLink: "website/docs/operating/reliability/runbooks.md", apiAuthEnabled: true}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "initial setup is not complete", SuggestedAction: "Complete setup via the API or CLI", DocLink: "website/docs/operating/reliability/runbooks.md", apiAuthEnabled: true}
}

func doctorCLIToken(apiAuthEnabled bool) doctorCheck {
	const id = "cli.token"
	title := "CLI token present"
	if !apiAuthEnabled {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "CLI API token not required; API auth is disabled"}
	}

	if effectiveToken() == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "no CLI API token configured", SuggestedAction: "Set VECTIS_API_TOKEN or run vectis-cli auth login", DocLink: "website/docs/operating/reliability/repair-runbooks.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "CLI API token is configured"}
}

func doctorSchemaCurrent() doctorCheck {
	const id = "db.schema.current"
	title := "Database schema current"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/schema/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		CurrentVersion int  `json:"current_version"`
		HasSchema      bool `json:"has_schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if !result.HasSchema {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: "no schema found — database may be uninitialized", SuggestedAction: "Run vectis-cli database migrate", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityCritical, Summary: fmt.Sprintf("schema at version %d", result.CurrentVersion), Evidence: fmt.Sprintf("%d", result.CurrentVersion), DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorReconcilerActive() doctorCheck {
	const id = "reconciler.active"
	title := "Reconciler heartbeat recent"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/heartbeat", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Active bool `json:"active"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if !result.Active {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no reconciler recovery activity recorded", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "reconciler recovery activity recorded", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorAuditDrops() doctorCheck {
	const id = "audit.drops.recent"
	title := "No recent audit drops"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/drops", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Dropped int64 `json:"dropped"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.Dropped > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit events dropped", result.Dropped), Evidence: fmt.Sprintf("%d", result.Dropped), SuggestedAction: "Check audit buffer configuration; check DB write capacity", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit events dropped", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorDBPool() doctorCheck {
	const id = "db.connection.pool"
	title := "DB connection pool healthy"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/db/pool-stats", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		OpenConnections int   `json:"open_connections"`
		InUse           int   `json:"in_use"`
		WaitCount       int64 `json:"wait_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.OpenConnections > 0 && result.InUse == result.OpenConnections && result.WaitCount > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("pool exhausted: %d in-use / %d open, %d waits", result.InUse, result.OpenConnections, result.WaitCount), Evidence: fmt.Sprintf("in_use=%d open=%d wait=%d", result.InUse, result.OpenConnections, result.WaitCount), SuggestedAction: "Increase max connections or check slow queries", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("pool healthy: %d open, %d in-use", result.OpenConnections, result.InUse), Evidence: fmt.Sprintf("open=%d in_use=%d", result.OpenConnections, result.InUse), DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorQueueBacklog() doctorCheck {
	const id = "queue.backlog.ratio"
	title := "Queue backlog within threshold"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/queue/backlog", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Queued int64                    `json:"queued"`
		Cells  []doctorQueueBacklogCell `json:"cells"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.Queued > 100 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("backlog high: %d queued", result.Queued), Evidence: formatDoctorQueueBacklogEvidence(result.Queued, result.Cells), SuggestedAction: "Check queue service health and worker count", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("backlog ok: %d queued", result.Queued), Evidence: formatDoctorQueueBacklogEvidence(result.Queued, result.Cells), DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

type doctorQueueBacklogCell struct {
	CellID string `json:"cell_id"`
	Queued int64  `json:"queued"`
}

func formatDoctorQueueBacklogEvidence(queued int64, cells []doctorQueueBacklogCell) string {
	if len(cells) == 0 {
		return fmt.Sprintf("%d", queued)
	}

	parts := []string{fmt.Sprintf("queued=%d", queued)}
	cellParts := make([]string, 0, len(cells))
	for _, cell := range cells {
		if cell.Queued <= 0 {
			continue
		}

		cellParts = append(cellParts, fmt.Sprintf("%s:%d", cell.CellID, cell.Queued))
	}

	if len(cellParts) > 0 {
		parts = append(parts, "cells="+strings.Join(cellParts, ","))
	}

	return strings.Join(parts, " ")
}

func doctorCronSchedules() doctorCheck {
	const id = "cron.schedules"
	title := "Cron schedules current"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/cron/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result doctorCronStatus
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	evidence := formatDoctorCronEvidence(result)
	if result.DueCount > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d cron schedules are due for dispatch", result.DueCount), Evidence: evidence, SuggestedAction: "Check vectis-cron process health, database access, and queue/cell ingress handoff", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.ClaimedCount > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d cron schedules are held by active claims", result.ClaimedCount), Evidence: evidence, SuggestedAction: "Check vectis-cron logs for slow or stuck schedule handoff", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.ScheduleCount == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no enabled cron schedules", Evidence: evidence, DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("cron schedules current: %d enabled", result.ScheduleCount), Evidence: evidence, DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

type doctorCronStatus struct {
	ScheduleCount int64  `json:"schedule_count"`
	DueCount      int64  `json:"due_count"`
	ClaimedCount  int64  `json:"claimed_count"`
	OldestDueUnix *int64 `json:"oldest_due_unix"`
	Active        bool   `json:"active"`
}

func formatDoctorCronEvidence(status doctorCronStatus) string {
	parts := []string{
		fmt.Sprintf("schedules=%d", status.ScheduleCount),
		fmt.Sprintf("due=%d", status.DueCount),
		fmt.Sprintf("claimed=%d", status.ClaimedCount),
	}

	if status.OldestDueUnix != nil {
		parts = append(parts, fmt.Sprintf("oldest_due=%s", time.Unix(*status.OldestDueUnix, 0).UTC().Format(time.RFC3339)))
	}

	return strings.Join(parts, " ")
}

func doctorStuckRuns() doctorCheck {
	const id = "reconciler.stuck.runs"
	title := "No stuck runs or task repair backlog"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Stuck                   int64                    `json:"stuck"`
		Cells                   []doctorStuckRunCell     `json:"cells"`
		TaskFinalizationPending int64                    `json:"task_finalization_pending"`
		TaskFinalizationCells   []doctorPendingCellCount `json:"task_finalization_cells"`
		TaskContinuationPending int64                    `json:"task_continuation_pending"`
		TaskContinuationCells   []doctorPendingCellCount `json:"task_continuation_cells"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.Stuck > 0 || result.TaskFinalizationPending > 0 || result.TaskContinuationPending > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: formatDoctorStuckRunsSummary(result.Stuck, result.TaskFinalizationPending, result.TaskContinuationPending), Evidence: formatDoctorStuckRunsEvidence(result.Stuck, result.Cells, result.TaskFinalizationPending, result.TaskFinalizationCells, result.TaskContinuationPending, result.TaskContinuationCells), SuggestedAction: "Check reconciler; check queued dispatch, task continuation, and task finalization paths", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no stuck runs or task repair backlog", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

type doctorStuckRunCell struct {
	CellID string `json:"cell_id"`
	Stuck  int64  `json:"stuck"`
}

type doctorPendingCellCount struct {
	CellID  string `json:"cell_id"`
	Pending int64  `json:"pending"`
}

func formatDoctorStuckRunsSummary(stuck, taskFinalizationPending, taskContinuationPending int64) string {
	parts := []string{}
	if stuck > 0 {
		parts = append(parts, fmt.Sprintf("%d stuck runs", stuck))
	}

	if taskContinuationPending > 0 {
		parts = append(parts, fmt.Sprintf("%d pending task continuations", taskContinuationPending))
	}

	if taskFinalizationPending > 0 {
		parts = append(parts, fmt.Sprintf("%d pending task finalizations", taskFinalizationPending))
	}

	return strings.Join(parts, " and ") + " detected"
}

func formatDoctorStuckRunsEvidence(stuck int64, cells []doctorStuckRunCell, taskFinalizationPending int64, taskFinalizationCells []doctorPendingCellCount, taskContinuationPending int64, taskContinuationCells []doctorPendingCellCount) string {
	parts := []string{fmt.Sprintf("stuck=%d", stuck)}
	if len(cells) == 0 {
		if stuck > 0 && taskFinalizationPending == 0 && len(taskFinalizationCells) == 0 && taskContinuationPending == 0 && len(taskContinuationCells) == 0 {
			return fmt.Sprintf("%d", stuck)
		}
	} else {
		cellParts := make([]string, 0, len(cells))
		for _, cell := range cells {
			if cell.Stuck <= 0 {
				continue
			}

			cellParts = append(cellParts, fmt.Sprintf("%s:%d", cell.CellID, cell.Stuck))
		}

		if len(cellParts) > 0 {
			parts = append(parts, "cells="+strings.Join(cellParts, ","))
		}
	}

	if taskContinuationPending > 0 || len(taskContinuationCells) > 0 {
		parts = append(parts, fmt.Sprintf("task_continuation_pending=%d", taskContinuationPending))
		taskCellParts := make([]string, 0, len(taskContinuationCells))
		for _, cell := range taskContinuationCells {
			if cell.Pending <= 0 {
				continue
			}

			taskCellParts = append(taskCellParts, fmt.Sprintf("%s:%d", cell.CellID, cell.Pending))
		}

		if len(taskCellParts) > 0 {
			parts = append(parts, "task_continuation_cells="+strings.Join(taskCellParts, ","))
		}
	}

	if taskFinalizationPending > 0 || len(taskFinalizationCells) > 0 {
		parts = append(parts, fmt.Sprintf("task_finalization_pending=%d", taskFinalizationPending))
		taskCellParts := make([]string, 0, len(taskFinalizationCells))
		for _, cell := range taskFinalizationCells {
			if cell.Pending <= 0 {
				continue
			}

			taskCellParts = append(taskCellParts, fmt.Sprintf("%s:%d", cell.CellID, cell.Pending))
		}

		if len(taskCellParts) > 0 {
			parts = append(parts, "task_finalization_cells="+strings.Join(taskCellParts, ","))
		}
	}

	return strings.Join(parts, " ")
}

func doctorCellIngressRoutes() doctorCheck {
	const id = "cells.ingress"
	title := "Cell ingress routes reachable"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/cells/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Cells []doctorCellIngressStatus `json:"cells"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if len(result.Cells) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no cell ingress routes configured", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	ready := 0
	required := 0
	unhealthy := 0
	for _, cell := range result.Cells {
		routeRequired := cell.IngressRequired || cell.IngressConfigured
		if !routeRequired {
			continue
		}

		required++
		if cell.Status == "ready" && cell.IngressReachable {
			ready++
			continue
		}

		unhealthy++
	}

	evidence := formatDoctorCellIngressEvidence(result.Cells)
	if unhealthy > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d cell ingress routes unhealthy", unhealthy), Evidence: evidence, SuggestedAction: "Check cell ingress processes, route map, and network path", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if required == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no cell ingress routes required", Evidence: evidence, DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("%d cell ingress routes ready", ready), Evidence: evidence, DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

type doctorCellIngressStatus struct {
	CellID            string `json:"cell_id"`
	IngressRequired   bool   `json:"ingress_required"`
	IngressConfigured bool   `json:"ingress_configured"`
	IngressReachable  bool   `json:"ingress_reachable"`
	Status            string `json:"status"`
}

func formatDoctorCellIngressEvidence(cells []doctorCellIngressStatus) string {
	parts := make([]string, 0, len(cells))
	for _, cell := range cells {
		status := strings.TrimSpace(cell.Status)
		if status == "" {
			status = "unknown"
		}

		parts = append(parts, fmt.Sprintf("%s:%s", cell.CellID, status))
	}

	return strings.Join(parts, ",")
}

func doctorCatalogInbox() doctorCheck {
	const id = "catalog.inbox"
	title := "Catalog inbox healthy"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/catalog/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result doctorCatalogStatus

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	evidence := formatDoctorCatalogInboxEvidence(result)
	if result.Failed > 0 {
		label := "events"
		if result.Failed == 1 {
			label = "event"
		}

		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d catalog %s failed", result.Failed, label), Evidence: evidence, SuggestedAction: "Inspect vectis-catalog logs and failed cell catalog events", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.Pending > doctorCatalogPendingWarn {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("catalog inbox backlog high: %d pending", result.Pending), Evidence: evidence, SuggestedAction: "Check vectis-catalog process health and database write latency", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("catalog inbox ok: %d pending", result.Pending), Evidence: evidence, DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

type doctorCatalogStatus struct {
	Pending int64                       `json:"pending"`
	Applied int64                       `json:"applied"`
	Failed  int64                       `json:"failed"`
	Total   int64                       `json:"total"`
	Sources []doctorCatalogSourceStatus `json:"sources"`
}

type doctorCatalogSourceStatus struct {
	SourceCell string `json:"source_cell"`
	Pending    int64  `json:"pending"`
	Applied    int64  `json:"applied"`
	Failed     int64  `json:"failed"`
	Total      int64  `json:"total"`
}

func formatDoctorCatalogInboxEvidence(status doctorCatalogStatus) string {
	base := fmt.Sprintf("pending=%d applied=%d failed=%d total=%d", status.Pending, status.Applied, status.Failed, status.Total)
	if len(status.Sources) == 0 {
		return base
	}

	parts := make([]string, 0, len(status.Sources))
	for _, source := range status.Sources {
		if source.Pending == 0 && source.Failed == 0 {
			continue
		}

		parts = append(parts, fmt.Sprintf("%s:p=%d/f=%d", source.SourceCell, source.Pending, source.Failed))
	}

	if len(parts) == 0 {
		return base
	}

	return fmt.Sprintf("%s sources=%s", base, strings.Join(parts, ","))
}

func doctorLogReachable() doctorCheck {
	const id = "log.reachable"
	title := "Log service reachable"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/log/reachable", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		Reachable bool `json:"reachable"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if !result.Reachable {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "log service is not reachable", SuggestedAction: "Check log service connectivity; check log DB", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "log service is reachable", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorAuditFlushFailures() doctorCheck {
	const id = "audit.flush.failures"
	title := "No recent audit flush failures"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/flush-failures", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	var result struct {
		FlushFailures int64 `json:"flush_failures"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	if result.FlushFailures > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit flush failures", result.FlushFailures), Evidence: fmt.Sprintf("%d", result.FlushFailures), SuggestedAction: "Check audit persistence; check DB write capacity", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit flush failures", DocLink: "website/docs/operating/reference/health-check-catalog.md"}
}

func doctorTLSFiles() doctorCheck {
	const id = "tls.files"
	title := "TLS files valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"
	action := "Check VECTIS_GRPC_TLS_* and VECTIS_METRICS_TLS_* paths"

	checks := []tlsFileCheck{}
	if !envBoolDefault("VECTIS_GRPC_TLS_INSECURE", true) || anyEnvSet("VECTIS_GRPC_TLS_CA_FILE", "VECTIS_GRPC_TLS_CERT_FILE", "VECTIS_GRPC_TLS_KEY_FILE", "VECTIS_GRPC_TLS_CLIENT_CA_FILE", "VECTIS_GRPC_TLS_CLIENT_CERT_FILE", "VECTIS_GRPC_TLS_CLIENT_KEY_FILE") {
		checks = append(checks,
			tlsFileCheck{label: "gRPC CA", path: os.Getenv("VECTIS_GRPC_TLS_CA_FILE"), kind: tlsFileCA, required: !envBoolDefault("VECTIS_GRPC_TLS_INSECURE", true)},
			tlsFileCheck{label: "gRPC server certificate", path: os.Getenv("VECTIS_GRPC_TLS_CERT_FILE"), kind: tlsFileCert},
			tlsFileCheck{label: "gRPC server key", path: os.Getenv("VECTIS_GRPC_TLS_KEY_FILE"), kind: tlsFileKey},
			tlsFileCheck{label: "gRPC client CA", path: os.Getenv("VECTIS_GRPC_TLS_CLIENT_CA_FILE"), kind: tlsFileCA},
			tlsFileCheck{label: "gRPC client certificate", path: os.Getenv("VECTIS_GRPC_TLS_CLIENT_CERT_FILE"), kind: tlsFileCert},
			tlsFileCheck{label: "gRPC client key", path: os.Getenv("VECTIS_GRPC_TLS_CLIENT_KEY_FILE"), kind: tlsFileKey},
		)
	}

	if !envBoolDefault("VECTIS_METRICS_TLS_INSECURE", true) || anyEnvSet("VECTIS_METRICS_TLS_CERT_FILE", "VECTIS_METRICS_TLS_KEY_FILE") {
		checks = append(checks,
			tlsFileCheck{label: "metrics certificate", path: os.Getenv("VECTIS_METRICS_TLS_CERT_FILE"), kind: tlsFileCert, required: !envBoolDefault("VECTIS_METRICS_TLS_INSECURE", true)},
			tlsFileCheck{label: "metrics key", path: os.Getenv("VECTIS_METRICS_TLS_KEY_FILE"), kind: tlsFileKey, required: !envBoolDefault("VECTIS_METRICS_TLS_INSECURE", true)},
		)
	}

	if len(checks) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "TLS file validation skipped; TLS is disabled", DocLink: doc}
	}

	var problems []string
	var warnings []string
	for _, check := range checks {
		if check.path == "" {
			if check.required {
				problems = append(problems, check.label+" path is not configured")
			}
			continue
		}

		if err := validateTLSFile(check); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", check.label, err))
			continue
		}

		if warn := tlsExpiryWarning(check); warn != "" {
			warnings = append(warnings, fmt.Sprintf("%s: %s", check.label, warn))
		}
	}

	if err := validateTLSKeyPair("gRPC server", os.Getenv("VECTIS_GRPC_TLS_CERT_FILE"), os.Getenv("VECTIS_GRPC_TLS_KEY_FILE")); err != nil {
		problems = append(problems, err.Error())
	}

	if err := validateTLSKeyPair("gRPC client", os.Getenv("VECTIS_GRPC_TLS_CLIENT_CERT_FILE"), os.Getenv("VECTIS_GRPC_TLS_CLIENT_KEY_FILE")); err != nil {
		problems = append(problems, err.Error())
	}

	if err := validateTLSKeyPair("metrics", os.Getenv("VECTIS_METRICS_TLS_CERT_FILE"), os.Getenv("VECTIS_METRICS_TLS_KEY_FILE")); err != nil {
		problems = append(problems, err.Error())
	}

	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: strings.Join(problems, "; "), SuggestedAction: action, DocLink: doc}
	}

	if len(warnings) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(warnings, "; "), SuggestedAction: action, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "configured TLS files are readable and valid", DocLink: doc}
}

type tlsFileKind int

const (
	tlsFileCA tlsFileKind = iota
	tlsFileCert
	tlsFileKey
)

type tlsFileCheck struct {
	label    string
	path     string
	kind     tlsFileKind
	required bool
}

func validateTLSFile(check tlsFileCheck) error {
	b, err := os.ReadFile(check.path)
	if err != nil {
		return err
	}

	if len(b) == 0 {
		return fmt.Errorf("empty file")
	}

	switch check.kind {
	case tlsFileCA:
		if pool := x509.NewCertPool(); !pool.AppendCertsFromPEM(b) {
			return fmt.Errorf("no PEM certificates found")
		}
	case tlsFileCert:
		if _, err := parsePEMCertificates(b); err != nil {
			return err
		}
	case tlsFileKey:
		if !hasPEMBlockType(b, "PRIVATE KEY") && !hasPEMBlockType(b, "RSA PRIVATE KEY") && !hasPEMBlockType(b, "EC PRIVATE KEY") {
			return fmt.Errorf("no PEM private key found")
		}
	}

	return nil
}

func tlsExpiryWarning(check tlsFileCheck) string {
	if check.kind != tlsFileCA && check.kind != tlsFileCert {
		return ""
	}

	b, err := os.ReadFile(check.path)
	if err != nil {
		return ""
	}

	certs, err := parsePEMCertificates(b)
	if err != nil {
		return ""
	}

	now := time.Now()
	for _, cert := range certs {
		if now.After(cert.NotAfter) {
			return fmt.Sprintf("certificate expired at %s", cert.NotAfter.Format(time.RFC3339))
		}

		if cert.NotAfter.Sub(now) < doctorCertExpiryWarn {
			return fmt.Sprintf("certificate expires at %s", cert.NotAfter.Format(time.RFC3339))
		}
	}

	return ""
}

func parsePEMCertificates(b []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			break
		}

		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse certificate: %w", err)
		}

		certs = append(certs, cert)
	}

	if len(certs) == 0 {
		return nil, fmt.Errorf("no PEM certificates found")
	}

	return certs, nil
}

func hasPEMBlockType(b []byte, want string) bool {
	for {
		var block *pem.Block
		block, b = pem.Decode(b)
		if block == nil {
			return false
		}

		if block.Type == want {
			return true
		}
	}
}

func validateTLSKeyPair(label, certFile, keyFile string) error {
	if certFile == "" && keyFile == "" {
		return nil
	}

	if certFile == "" || keyFile == "" {
		return fmt.Errorf("%s certificate and key must be configured together", label)
	}

	if _, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return fmt.Errorf("%s certificate/key mismatch: %w", label, err)
	}

	return nil
}

func doctorFilesystemPressure(id, title, label, path string) doctorCheck {
	doc := "website/docs/operating/reference/health-check-catalog.md"
	if path == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%s path is not configured", label), SuggestedAction: "Configure the deploy path or run doctor on the host that owns it", DocLink: doc}
	}

	statPath, exists, err := existingPathForStat(path)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%s path is not usable: %v", label, err), SuggestedAction: "Check directory ownership and parent path", DocLink: doc}
	}

	if exists {
		if err := directoryUsable(path); err != nil {
			return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%s path is not writable: %v", label, err), Evidence: path, SuggestedAction: "Check directory ownership and permissions", DocLink: doc}
		}
	}

	stats, err := filesystemStats(statPath)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("cannot inspect filesystem for %s: %v", label, err), Evidence: statPath, SuggestedAction: "Run doctor on the host that owns the path", DocLink: doc}
	}

	evidence := fmt.Sprintf("path=%s stat_path=%s free_bytes=%d free_percent=%d free_inodes=%d", path, statPath, stats.freeBytes, stats.freePercent, stats.freeInodes)

	if stats.freeBytes < doctorDiskWarnFreeBytes || stats.freeInodes == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("filesystem pressure: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent), Evidence: evidence, SuggestedAction: "Free disk space or move the path to a larger volume", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("filesystem ok: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent), Evidence: evidence, DocLink: doc}
}

type doctorFSStats struct {
	freeBytes   uint64
	freePercent int
	freeInodes  uint64
}

func filesystemStats(path string) (doctorFSStats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return doctorFSStats{}, err
	}

	free := st.Bavail * uint64(st.Bsize)
	total := st.Blocks * uint64(st.Bsize)
	percent := 0
	if total > 0 {
		percent = int((free * 100) / total)
	}

	return doctorFSStats{freeBytes: free, freePercent: percent, freeInodes: st.Ffree}, nil
}

func existingPathForStat(path string) (string, bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return "", false, fmt.Errorf("%s is not a directory", path)
		}
		return path, true, nil
	}

	if !os.IsNotExist(err) {
		return "", false, err
	}

	parent := filepath.Dir(path)
	for parent != "." && parent != "/" {
		info, err = os.Stat(parent)
		if err == nil {
			if !info.IsDir() {
				return "", false, fmt.Errorf("%s is not a directory", parent)
			}

			return parent, false, nil
		}

		if !os.IsNotExist(err) {
			return "", false, err
		}

		parent = filepath.Dir(parent)
	}

	return parent, false, nil
}

func directoryUsable(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("not a directory")
	}

	probe, err := os.CreateTemp(path, ".vectis-doctor-*")
	if err != nil {
		return err
	}

	name := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}

	return os.Remove(name)
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}

func envOrDefaultAllowEmpty(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok {
		return v
	}

	return fallback
}

func envBoolDefault(name string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if v == "" {
		return fallback
	}

	switch v {
	case "1", "t", "true", "y", "yes", "on":
		return true
	case "0", "f", "false", "n", "no", "off":
		return false
	default:
		return fallback
	}
}

func anyEnvSet(names ...string) bool {
	for _, name := range names {
		if os.Getenv(name) != "" {
			return true
		}
	}

	return false
}

func defaultDoctorForwarderSpoolDir() string {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, _ := os.UserHomeDir()
		if home != "" {
			dataHome = filepath.Join(home, ".local", "share")
		} else {
			dataHome = os.TempDir()
		}
	}

	return filepath.Join(dataHome, "vectis", "log-forwarder", "spool")
}

func defaultDoctorLogStorageDir() string {
	return filepath.Join(utils.DataHome(), "vectis", "log")
}

func defaultDoctorArtifactStorageDir() string {
	return filepath.Join(utils.DataHome(), "vectis", "artifact")
}

func defaultDoctorQueuePersistenceDir() string {
	pool := os.Getenv("VECTIS_QUEUE_POOL")
	if pool == "" {
		pool = "default"
	}

	instanceID := os.Getenv("VECTIS_QUEUE_INSTANCE_ID")
	if instanceID == "" {
		port := config.QueuePort()
		if raw := os.Getenv("VECTIS_QUEUE_PORT"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
				port = parsed
			}
		}

		hostname, err := os.Hostname()
		if err != nil {
			hostname = "localhost"
		}

		if strings.TrimSpace(hostname) == "" {
			hostname = "localhost"
		}

		instanceID = fmt.Sprintf("%s-%d", sanitizeDoctorQueuePathComponent(hostname), port)
	}

	return filepath.Join(utils.DataHome(), "vectis", "queue", sanitizeDoctorQueuePathComponent(pool), sanitizeDoctorQueuePathComponent(instanceID))
}

func sanitizeDoctorQueuePathComponent(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))

	var b strings.Builder
	lastDash := false
	for _, r := range value {
		valid := (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.'

		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	cleaned := strings.Trim(b.String(), "-.")
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		return "queue"
	}

	return cleaned
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}

	value := float64(n)
	for _, suffix := range []string{"KiB", "MiB", "GiB", "TiB", "PiB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f %s", value, suffix)
		}
	}

	return fmt.Sprintf("%.1f EiB", value/unit)
}

var healthCmd = &cobra.Command{
	Use:     "health",
	Short:   "Check API and deployment health",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var doctorCmd = &cobra.Command{
	Use:   "check",
	Short: "Run operational diagnostics",
	Long: `Run a stable, versioned set of operational checks against the configured Vectis API.

Check IDs are frozen between releases (see website/docs/operating/reference/health-check-catalog.md for the catalog).

Text output groups checks by subsystem and starts with an overall status.
  --format json emits a summary object and the full check model.
  --strict treats warnings as exit-nonzero (for CI).

Failed checks always exit non-zero.`,
	Args: cobra.NoArgs,
	Run:  runDoctor,
}

func configureDoctorFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("json", false, "Emit JSON output (deprecated; use --format json)")
	_ = cmd.Flags().MarkDeprecated("json", "use --format json")
	cmd.Flags().Bool("strict", false, "Exit non-zero on warnings")
}
