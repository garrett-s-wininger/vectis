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

var doctorJSON bool
var doctorStrict bool

const (
	doctorDiskWarnFreeBytes = 1 << 30
	doctorCertExpiryWarn    = 14 * 24 * time.Hour
)

func runDoctor(cmd *cobra.Command, args []string) {
	doctorJSON, _ = cmd.Flags().GetBool("json")
	doctorStrict, _ = cmd.Flags().GetBool("strict")

	if err := doctor(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func doctor(w io.Writer) error {
	setupStatus := doctorSetupStatus()
	checks := []doctorCheck{
		doctorHTTPStatus("api.live", http.MethodGet, "/health/live", http.StatusOK, "API liveness probe passed", severityCritical, "API liveness", "Check API server process", "website/docs/operator/runbooks.md"),
		doctorHTTPStatus("api.ready", http.MethodGet, "/health/ready", http.StatusOK, "API readiness probe passed", severityCritical, "API readiness", "Check API server and dependencies (DB, queue)", "website/docs/operator/runbooks.md"),
		setupStatus,
		doctorCLIToken(setupStatus.apiAuthEnabled),
		doctorSchemaCurrent(),
		doctorReconcilerActive(),
		doctorAuditDrops(),
		doctorDBPool(),
		doctorQueueBacklog(),
		doctorStuckRuns(),
		doctorLogReachable(),
		doctorAuditFlushFailures(),
		doctorTLSFiles(),
		doctorFilesystemPressure("queue.persistence.filesystem", "Queue persistence filesystem", "queue persistence", envOrDefault("VECTIS_QUEUE_PERSISTENCE_DIR", filepath.Join(utils.DataHome(), "vectis", "queue"))),
		doctorFilesystemPressure("log.storage.filesystem", "Log storage filesystem", "log storage", envOrDefault("VECTIS_LOG_STORAGE_DIR", filepath.Join(utils.DataHome(), "vectis", "jobs"))),
		doctorFilesystemPressure("log.forwarder.spool.filesystem", "Log forwarder spool filesystem", "log-forwarder spool", envOrDefault("VECTIS_LOG_FORWARDER_SPOOL_DIR", defaultDoctorForwarderSpoolDir())),
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
	{Name: "Reconciler", Items: []doctorTextItem{
		{ID: "reconciler.active", Label: "Recovery activity"},
		{ID: "reconciler.stuck.runs", Label: "Stuck runs"},
	}},
	{Name: "Logging", Items: []doctorTextItem{
		{ID: "log.reachable", Label: "Log service"},
		{ID: "log.storage.filesystem", Label: "Log storage"},
		{ID: "log.forwarder.spool.filesystem", Label: "Forwarder spool"},
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
		return fmt.Errorf("one or more doctor checks failed")
	}

	if doctorStrict && warned {
		return fmt.Errorf("one or more doctor checks reported warnings (--strict)")
	}

	return nil
}

func writeDoctorJSON(w io.Writer, checks []doctorCheck) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(checks); err != nil {
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
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/runbooks.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/runbooks.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/runbooks.md"}
	}

	var result struct {
		SetupComplete bool  `json:"setup_complete"`
		AuthEnabled   *bool `json:"auth_enabled"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/runbooks.md"}
	}

	authEnabled := config.APIAuthEnabled()
	if result.AuthEnabled != nil {
		authEnabled = *result.AuthEnabled
	}

	if !authEnabled {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "initial setup not required; API auth is disabled", DocLink: "website/docs/operator/runbooks.md", apiAuthEnabled: false}
	}

	if result.SetupComplete {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "initial setup is complete", DocLink: "website/docs/operator/runbooks.md", apiAuthEnabled: true}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "initial setup is not complete", SuggestedAction: "Complete setup via the API or CLI", DocLink: "website/docs/operator/runbooks.md", apiAuthEnabled: true}
}

func doctorCLIToken(apiAuthEnabled bool) doctorCheck {
	const id = "cli.token"
	title := "CLI token present"
	if !apiAuthEnabled {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "CLI API token not required; API auth is disabled"}
	}

	if effectiveToken() == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "no CLI API token configured", SuggestedAction: "Set VECTIS_API_TOKEN or run login", DocLink: "website/docs/operator/repair-runbooks.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "CLI API token is configured"}
}

func doctorSchemaCurrent() doctorCheck {
	const id = "db.schema.current"
	title := "Database schema current"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/schema/status", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		CurrentVersion int  `json:"current_version"`
		HasSchema      bool `json:"has_schema"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if !result.HasSchema {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityCritical, Summary: "no schema found — database may be uninitialized", SuggestedAction: "Run vectis-cli database migrate", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityCritical, Summary: fmt.Sprintf("schema at version %d", result.CurrentVersion), Evidence: fmt.Sprintf("%d", result.CurrentVersion), DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorReconcilerActive() doctorCheck {
	const id = "reconciler.active"
	title := "Reconciler heartbeat recent"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/heartbeat", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		Active bool `json:"active"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if !result.Active {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no reconciler recovery activity recorded", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "reconciler recovery activity recorded", DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorAuditDrops() doctorCheck {
	const id = "audit.drops.recent"
	title := "No recent audit drops"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/drops", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		Dropped int64 `json:"dropped"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if result.Dropped > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit events dropped", result.Dropped), Evidence: fmt.Sprintf("%d", result.Dropped), SuggestedAction: "Check audit buffer configuration; check DB write capacity", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit events dropped", DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorDBPool() doctorCheck {
	const id = "db.connection.pool"
	title := "DB connection pool healthy"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/db/pool-stats", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		OpenConnections int   `json:"open_connections"`
		InUse           int   `json:"in_use"`
		WaitCount       int64 `json:"wait_count"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if result.OpenConnections > 0 && result.InUse == result.OpenConnections && result.WaitCount > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("pool exhausted: %d in-use / %d open, %d waits", result.InUse, result.OpenConnections, result.WaitCount), Evidence: fmt.Sprintf("in_use=%d open=%d wait=%d", result.InUse, result.OpenConnections, result.WaitCount), SuggestedAction: "Increase max connections or check slow queries", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("pool healthy: %d open, %d in-use", result.OpenConnections, result.InUse), Evidence: fmt.Sprintf("open=%d in_use=%d", result.OpenConnections, result.InUse), DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorQueueBacklog() doctorCheck {
	const id = "queue.backlog.ratio"
	title := "Queue backlog within threshold"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/queue/backlog", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		Queued int64 `json:"queued"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if result.Queued > 100 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("backlog high: %d queued", result.Queued), Evidence: fmt.Sprintf("%d", result.Queued), SuggestedAction: "Check queue service health and worker count", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("backlog ok: %d queued", result.Queued), Evidence: fmt.Sprintf("%d", result.Queued), DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorStuckRuns() doctorCheck {
	const id = "reconciler.stuck.runs"
	title := "No stuck runs beyond threshold"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/reconciler/stuck-runs", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		Stuck int64 `json:"stuck"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if result.Stuck > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d stuck runs detected", result.Stuck), Evidence: fmt.Sprintf("%d", result.Stuck), SuggestedAction: "Check reconciler; check dispatch path", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no stuck runs", DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorLogReachable() doctorCheck {
	const id = "log.reachable"
	title := "Log service reachable"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/log/reachable", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		Reachable bool `json:"reachable"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if !result.Reachable {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "log service is not reachable", SuggestedAction: "Check log service connectivity; check log DB", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "log service is reachable", DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorAuditFlushFailures() doctorCheck {
	const id = "audit.flush.failures"
	title := "No recent audit flush failures"
	req, err := newAPIRequest(http.MethodGet, "/api/v1/audit/flush-failures", nil)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorFail, Severity: severityWarning, Summary: err.Error(), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("request failed: %v", err), SuggestedAction: "Check API server reachability", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unexpected status: %s", resp.Status), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	var result struct {
		FlushFailures int64 `json:"flush_failures"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("failed to parse response: %v", err), SuggestedAction: "Check API server", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	if result.FlushFailures > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d audit flush failures", result.FlushFailures), Evidence: fmt.Sprintf("%d", result.FlushFailures), SuggestedAction: "Check audit persistence; check DB write capacity", DocLink: "website/docs/operator/doctor-check-catalog.md"}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no audit flush failures", DocLink: "website/docs/operator/doctor-check-catalog.md"}
}

func doctorTLSFiles() doctorCheck {
	const id = "tls.files"
	title := "TLS files valid"
	doc := "website/docs/operator/doctor-check-catalog.md"
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
	doc := "website/docs/operator/doctor-check-catalog.md"
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

Check IDs are frozen between releases (see website/docs/operator/doctor-check-catalog.md for the catalog).

Text output groups checks by subsystem and starts with an overall status.
  --json   emits the full check model as a JSON array.
  --strict treats warnings as exit-nonzero (for CI).

Failed checks always exit non-zero.`,
	Args: cobra.NoArgs,
	Run:  runDoctor,
}

func configureDoctorFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("json", false, "Emit output as a JSON array")
	cmd.Flags().Bool("strict", false, "Exit non-zero on warnings")
}
