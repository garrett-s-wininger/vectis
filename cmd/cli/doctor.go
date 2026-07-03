package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	encryptedfs "vectis/extensions/secrets/encryptedfs"
	"vectis/internal/config"
	"vectis/internal/platform"
	"vectis/internal/serviceidentity"
	"vectis/internal/workercore"
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
var doctorFilesystemStats = filesystemStats

const (
	doctorDiskWarnFreeBytes  = 1 << 30
	doctorCertExpiryWarn     = 14 * 24 * time.Hour
	doctorCatalogPendingWarn = 100
)

func runDoctor(cmd *cobra.Command, args []string) {
	doctorJSON, _ = cmd.Flags().GetBool("json")
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
		doctorAPIEdgeConfig(setupStatus.apiAuthEnabled),
		doctorSchemaCurrent(),
		doctorReconcilerActive(),
		doctorAuditDrops(),
		doctorDBPool(),
		doctorQueueBacklog(),
		doctorCronSchedules(),
		doctorStuckRuns(),
		doctorCellIngressRoutes(),
		doctorCatalogInbox(),
	}
	checks = append(checks, doctorSourceControlChecks()...)
	checks = append(checks,
		doctorLogReachable(),
		doctorLogForwarderSocket(),
		doctorAuditFlushFailures(),
		doctorEncryptedFSFiles(),
		doctorWorkerCoreSockets(),
		doctorWorkerSPIFFEConfig(),
		doctorWorkerWorkspaceFilesystem(),
		doctorServiceIdentityConfig(),
		doctorMetricsListenersConfig(),
		doctorTLSFiles(),
		doctorFilesystemPressure("queue.persistence.filesystem", "Queue persistence filesystem", "queue persistence", envOrDefaultAllowEmpty("VECTIS_QUEUE_PERSISTENCE_DIR", defaultDoctorQueuePersistenceDir())),
		doctorFilesystemPressure("log.storage.filesystem", "Log storage filesystem", "log storage", envOrDefault("VECTIS_LOG_STORAGE_DIR", defaultDoctorLogStorageDir())),
		doctorFilesystemPressure("log.forwarder.spool.filesystem", "Log forwarder spool filesystem", "log-forwarder spool", envOrDefault("VECTIS_LOG_FORWARDER_SPOOL_DIR", defaultDoctorForwarderSpoolDir())),
		doctorArtifactFilesystemCheck(),
	)

	if doctorJSON || outputIsJSON() {
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
		{ID: "api.edge.config", Label: "API edge"},
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
	{Name: "Worker", Items: []doctorTextItem{
		{ID: "worker.core.sockets", Label: "Core sockets"},
		{ID: "worker.spiffe.config", Label: "SPIFFE config"},
		{ID: "worker.workspace.filesystem", Label: "Workspace filesystem"},
	}},
	{Name: "Internal Trust", Items: []doctorTextItem{
		{ID: "service.identity.config", Label: "Service identity"},
	}},
	{Name: "Metrics", Items: []doctorTextItem{
		{ID: "metrics.listeners.config", Label: "Listeners"},
	}},
	{Name: "Catalog", Items: []doctorTextItem{
		{ID: "catalog.inbox", Label: "Cell event inbox"},
	}},
	{Name: "Source Control", Items: []doctorTextItem{
		{ID: "source.mode", Label: "Config-as-code"},
		{ID: "source.checkout.filesystem", Label: "Checkout filesystem"},
		{ID: "source.repositories.sync", Label: "Repository sync"},
		{ID: "source.repositories.declared", Label: "Repository declarations"},
		{ID: "source.schedules.declared", Label: "Schedule declarations"},
		{ID: "source.schedules.overrides", Label: "Schedule overrides"},
	}},
	{Name: "Logging", Items: []doctorTextItem{
		{ID: "log.reachable", Label: "Log service"},
		{ID: "log.forwarder.socket", Label: "Forwarder socket"},
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
	{Name: "Secrets", Items: []doctorTextItem{
		{ID: "secrets.encryptedfs.files", Label: "EncryptedFS files"},
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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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

func doctorAPIEdgeConfig(apiAuthEnabled bool) doctorCheck {
	const id = "api.edge.config"
	title := "API edge config valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	doctorBindAPIEdgeEnv()

	localVisible := doctorAPIEdgeEnvVisible()
	authEnabled := apiAuthEnabled || config.APIAuthEnabled()
	evidence := formatDoctorAPIEdgeEvidence(authEnabled, localVisible)

	var problems []string
	for _, check := range []struct {
		label string
		err   error
	}{
		{label: "API HTTPS", err: config.ValidateAPIHTTPS()},
		{label: "API Host allowlist", err: config.ValidateAPIHostConfig()},
		{label: "trusted proxy CIDRs", err: config.ValidateAPIClientIPConfig()},
		{label: "API CORS", err: config.ValidateAPICORSConfig()},
		{label: "API HSTS", err: config.ValidateAPIHSTSConfig()},
		{label: "API session", err: config.ValidateAPISessionConfig()},
	} {
		if check.err != nil {
			problems = append(problems, fmt.Sprintf("%s config is invalid: %v", check.label, check.err))
		}
	}

	if authEnabled && localVisible && !config.APIAuthEnabled() && !config.APISessionCookieSecure() && !config.APIHTTPSEnabled() && !config.APISessionAllowInsecureCookies() {
		problems = append(problems, "auth-enabled API edge config should set VECTIS_API_SESSION_COOKIE_SECURE=true or configure direct API TLS")
	}

	if authEnabled && localVisible && config.APISessionAllowInsecureCookies() {
		problems = append(problems, "auth-enabled API edge config allows insecure browser cookies; reserve VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES for local development only")
	}

	if authEnabled && localVisible && doctorAPIHostExternallyBound() && !doctorAPIAllowedHostsConfigured() {
		problems = append(problems, "externally bound auth-enabled API should configure VECTIS_API_ALLOWED_HOSTS with the browser-facing DNS names")
	}

	if config.APIHTTPSEnabled() && config.APIHSTSMaxAgeSeconds() == 0 {
		problems = append(problems, "API HTTPS is enabled but HSTS max-age is zero")
	}

	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(problems, "; "), Evidence: evidence, SuggestedAction: "Check VECTIS_API_* edge, session, Host, CORS, proxy, HSTS, and TLS settings", DocLink: doc}
	}

	if !localVisible {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "API edge config is not visible in this shell", Evidence: evidence, DocLink: doc}
	}

	if authEnabled {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "auth-enabled API edge config is valid", Evidence: evidence, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "API edge config is valid; API auth is disabled", Evidence: evidence, DocLink: doc}
}

func doctorBindAPIEdgeEnv() {
	for _, binding := range []struct {
		key      string
		envNames []string
	}{
		{key: "api.host", envNames: []string{"VECTIS_API_SERVER_HOST"}},
		{key: "api.auth.enabled", envNames: []string{"VECTIS_API_AUTH_ENABLED"}},
		{key: "api.authz.engine", envNames: []string{"VECTIS_API_AUTHZ_ENGINE"}},
		{key: "api.tls.cert_file", envNames: []string{"VECTIS_API_TLS_CERT_FILE", "VECTIS_API_SERVER_TLS_CERT_FILE"}},
		{key: "api.tls.key_file", envNames: []string{"VECTIS_API_TLS_KEY_FILE", "VECTIS_API_SERVER_TLS_KEY_FILE"}},
		{key: "api.tls.reload_interval", envNames: []string{"VECTIS_API_TLS_RELOAD_INTERVAL", "VECTIS_API_SERVER_TLS_RELOAD_INTERVAL"}},
		{key: "api.hsts.max_age_seconds", envNames: []string{"VECTIS_API_HSTS_MAX_AGE_SECONDS", "VECTIS_API_SERVER_HSTS_MAX_AGE_SECONDS"}},
		{key: "api.hsts.include_subdomains", envNames: []string{"VECTIS_API_HSTS_INCLUDE_SUBDOMAINS", "VECTIS_API_SERVER_HSTS_INCLUDE_SUBDOMAINS"}},
		{key: "api.hsts.preload", envNames: []string{"VECTIS_API_HSTS_PRELOAD", "VECTIS_API_SERVER_HSTS_PRELOAD"}},
		{key: "api.session.ttl", envNames: []string{"VECTIS_API_SESSION_TTL"}},
		{key: "api.session.idle_ttl", envNames: []string{"VECTIS_API_SESSION_IDLE_TTL"}},
		{key: "api.session.cookie_secure", envNames: []string{"VECTIS_API_SESSION_COOKIE_SECURE"}},
		{key: "api.session.allow_insecure_cookies", envNames: []string{"VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES"}},
		{key: "api.client_ip.trusted_proxy_cidrs", envNames: []string{"VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS"}},
		{key: "api.host_validation.allowed_hosts", envNames: []string{"VECTIS_API_ALLOWED_HOSTS"}},
		{key: "api.cors.allowed_origins", envNames: []string{"VECTIS_API_CORS_ALLOWED_ORIGINS"}},
	} {
		_ = viper.BindEnv(append([]string{binding.key}, binding.envNames...)...)
	}
}

func doctorAPIEdgeEnvVisible() bool {
	return anyEnvSet(
		"VECTIS_API_AUTH_ENABLED",
		"VECTIS_API_AUTHZ_ENGINE",
		"VECTIS_API_SERVER_HOST",
		"VECTIS_API_ALLOWED_HOSTS",
		"VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS",
		"VECTIS_API_CORS_ALLOWED_ORIGINS",
		"VECTIS_API_TLS_CERT_FILE",
		"VECTIS_API_TLS_KEY_FILE",
		"VECTIS_API_SERVER_TLS_CERT_FILE",
		"VECTIS_API_SERVER_TLS_KEY_FILE",
		"VECTIS_API_HSTS_MAX_AGE_SECONDS",
		"VECTIS_API_HSTS_INCLUDE_SUBDOMAINS",
		"VECTIS_API_HSTS_PRELOAD",
		"VECTIS_API_SERVER_HSTS_MAX_AGE_SECONDS",
		"VECTIS_API_SERVER_HSTS_INCLUDE_SUBDOMAINS",
		"VECTIS_API_SERVER_HSTS_PRELOAD",
		"VECTIS_API_SESSION_TTL",
		"VECTIS_API_SESSION_IDLE_TTL",
		"VECTIS_API_SESSION_COOKIE_SECURE",
		"VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES",
	)
}

func formatDoctorAPIEdgeEvidence(authEnabled, localVisible bool) string {
	return strings.Join([]string{
		fmt.Sprintf("auth_enabled=%t", authEnabled),
		fmt.Sprintf("local_config_visible=%t", localVisible),
		fmt.Sprintf("api_tls_enabled=%t", config.APIHTTPSEnabled()),
		fmt.Sprintf("cookie_secure=%t", config.APISessionCookieSecure()),
		fmt.Sprintf("allow_insecure_cookies=%t", config.APISessionAllowInsecureCookies()),
		fmt.Sprintf("allowed_hosts=%d", len(config.APIAllowedHosts())),
		fmt.Sprintf("trusted_proxy_cidrs=%d", len(config.APIClientIPTrustedProxyCIDRStrings())),
		fmt.Sprintf("cors_origins=%d", len(config.APICORSAllowedOrigins())),
		fmt.Sprintf("hsts_max_age_seconds=%d", config.APIHSTSMaxAgeSeconds()),
		fmt.Sprintf("hsts_preload=%t", config.APIHSTSPreload()),
		fmt.Sprintf("externally_bound=%t", doctorAPIHostExternallyBound()),
	}, " ")
}

func doctorAPIAllowedHostsConfigured() bool {
	_, ok := envValue("VECTIS_API_ALLOWED_HOSTS")
	return ok
}

func doctorAPIHostExternallyBound() bool {
	host := strings.TrimSpace(config.APIHost())
	if host == "" {
		return false
	}

	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.Trim(strings.TrimSpace(host), "[]")
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsUnspecified()
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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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

func doctorSourceControlChecks() []doctorCheck {
	status, statusLoadError := doctorLoadSourceStatus()
	loadRepositorySyncDetails := doctorSourceStatusNeedsRepositorySyncDetails(status, statusLoadError)
	loadRepositoryDeclarationDetails := doctorSourceStatusNeedsRepositoryDeclarationDetails(status, statusLoadError)
	loadScheduleDetails := doctorSourceStatusNeedsScheduleDetails(status, statusLoadError)
	loadRepositoryDetails := loadRepositorySyncDetails || loadRepositoryDeclarationDetails || loadScheduleDetails

	var repositories []sourceRepositorySummary
	repositoryLoadError := ""
	if loadRepositoryDetails {
		repositories, repositoryLoadError = doctorLoadSourceRepositories()
	}

	var schedules []sourceScheduleSummary
	scheduleLoadError := ""
	if loadScheduleDetails {
		if repositoryLoadError != "" {
			scheduleLoadError = "source repository inventory unavailable: " + repositoryLoadError
		} else {
			schedules, scheduleLoadError = doctorLoadSourceSchedules(repositories)
		}
	}

	repositorySyncCheck := doctorSourceRepositorySyncFromStatus(status, statusLoadError)
	repositoryDeclarationCheck := doctorSourceRepositoryDeclarationsFromStatus(status, statusLoadError)
	if loadRepositorySyncDetails {
		repositorySyncCheck = doctorSourceRepositorySync(repositories, repositoryLoadError)
	}
	if loadRepositoryDeclarationDetails {
		repositoryDeclarationCheck = doctorSourceRepositoryDeclarations(repositories, repositoryLoadError)
	}

	scheduleDeclarationCheck := doctorSourceScheduleDeclarationsFromStatus(status, statusLoadError)
	scheduleOverrideCheck := doctorSourceScheduleOverridesFromStatus(status, statusLoadError)
	if loadScheduleDetails {
		scheduleDeclarationCheck = doctorSourceScheduleDeclarations(schedules, scheduleLoadError)
		scheduleOverrideCheck = doctorSourceScheduleOverrides(schedules, scheduleLoadError)
	}

	return []doctorCheck{
		doctorSourceMode(status, statusLoadError),
		doctorSourceCheckoutFilesystem(),
		repositorySyncCheck,
		repositoryDeclarationCheck,
		scheduleDeclarationCheck,
		scheduleOverrideCheck,
	}
}

func doctorSourceStatusNeedsRepositorySyncDetails(status doctorSourceStatus, statusLoadError string) bool {
	if statusLoadError != "" {
		return true
	}

	return status.Repositories.SyncFailed > 0 || status.Repositories.SyncRunning > 0
}

func doctorSourceStatusNeedsRepositoryDeclarationDetails(status doctorSourceStatus, statusLoadError string) bool {
	if statusLoadError != "" {
		return true
	}

	return status.Repositories.StaleEnabled > 0
}

func doctorSourceStatusNeedsScheduleDetails(status doctorSourceStatus, statusLoadError string) bool {
	if statusLoadError != "" {
		return true
	}

	return status.Schedules.StaleEnabled > 0 || status.Schedules.ActiveOverrides > 0
}

func doctorSourceCheckoutFilesystem() doctorCheck {
	const id = "source.checkout.filesystem"
	title := "Source checkout filesystem"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	root := defaultDoctorSourceCheckoutRoot()
	if strings.TrimSpace(root) == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "source checkout path is not configured", SuggestedAction: "Configure VECTIS_SOURCE_CHECKOUT_ROOT or run vectis-cli health check on the API host", DocLink: doc}
	}

	if !filepath.IsAbs(root) {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "source checkout path must be absolute", Evidence: fmt.Sprintf("path=%s", root), SuggestedAction: "Set VECTIS_SOURCE_CHECKOUT_ROOT to an absolute managed checkout root", DocLink: doc}
	}

	return doctorFilesystemPressure(id, title, "source checkout", root)
}

type doctorSourceStatus struct {
	RepositoriesConfigured bool                         `json:"repositories_configured"`
	SchedulesConfigured    bool                         `json:"schedules_configured"`
	DeclaredRepositories   int                          `json:"declared_repositories"`
	DeclaredSchedules      int                          `json:"declared_schedules"`
	Repositories           doctorSourceRepositoryCounts `json:"repositories"`
	Schedules              doctorSourceScheduleCounts   `json:"schedules"`
}

type doctorSourceRepositoryCounts struct {
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

type doctorSourceScheduleCounts struct {
	Total           int `json:"total"`
	Enabled         int `json:"enabled"`
	Disabled        int `json:"disabled"`
	Declared        int `json:"declared"`
	StaleEnabled    int `json:"stale_enabled"`
	StaleDisabled   int `json:"stale_disabled"`
	ActiveOverrides int `json:"active_overrides"`
}

func doctorLoadSourceStatus() (doctorSourceStatus, string) {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/source/status", nil)
	if err != nil {
		return doctorSourceStatus{}, err.Error()
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return doctorSourceStatus{}, fmt.Sprintf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return doctorSourceStatus{}, fmt.Sprintf("unexpected status: %s", resp.Status)
	}

	var status doctorSourceStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return doctorSourceStatus{}, fmt.Sprintf("failed to parse response: %v", err)
	}

	return status, ""
}

func doctorLoadSourceRepositories() ([]sourceRepositorySummary, string) {
	namespaces, loadError := doctorLoadSourceNamespaces()
	if loadError != "" {
		return nil, loadError
	}

	repositories := make([]sourceRepositorySummary, 0)
	for _, namespace := range namespaces {
		namespacePath := strings.TrimSpace(namespace.Path)
		if namespacePath == "" {
			continue
		}

		params := url.Values{}
		params.Set("namespace", namespacePath)
		path := appendQueryParams("/api/v1/source-repositories", params)

		namespaceRepositories, loadError := doctorLoadSourceRepositoriesPath(path)
		if loadError != "" {
			return nil, loadError
		}
		repositories = append(repositories, namespaceRepositories...)
	}

	return repositories, ""
}

func doctorLoadSourceNamespaces() ([]namespaceCLIResponse, string) {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		return nil, err.Error()
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, fmt.Sprintf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("unexpected status: %s", resp.Status)
	}

	var namespaces []namespaceCLIResponse
	if err := json.NewDecoder(resp.Body).Decode(&namespaces); err != nil {
		return nil, fmt.Sprintf("failed to parse response: %v", err)
	}

	sort.Slice(namespaces, func(i, j int) bool {
		return namespaces[i].Path < namespaces[j].Path
	})

	return namespaces, ""
}

func doctorLoadSourceRepositoriesPath(path string) ([]sourceRepositorySummary, string) {
	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err.Error()
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, fmt.Sprintf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("unexpected status: %s", resp.Status)
	}

	var repositories []sourceRepositorySummary
	if err := json.NewDecoder(resp.Body).Decode(&repositories); err != nil {
		return nil, fmt.Sprintf("failed to parse response: %v", err)
	}

	return repositories, ""
}

func doctorLoadSourceSchedules(repositories []sourceRepositorySummary) ([]sourceScheduleSummary, string) {
	schedules := make([]sourceScheduleSummary, 0)
	for _, repo := range repositories {
		repoID := strings.TrimSpace(repo.RepositoryID)
		if repoID == "" {
			continue
		}

		repoSchedules, loadError := doctorLoadSourceSchedulesPath("/api/v1/source-repositories/" + url.PathEscape(repoID) + "/schedules")
		if loadError != "" {
			return nil, loadError
		}

		schedules = append(schedules, repoSchedules...)
	}

	return schedules, ""
}

func doctorLoadSourceSchedulesPath(path string) ([]sourceScheduleSummary, string) {
	req, err := newAPIRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err.Error()
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return nil, fmt.Sprintf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("unexpected status: %s", resp.Status)
	}

	var result sourceSchedulesResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Sprintf("failed to parse response: %v", err)
	}

	return result.Schedules, ""
}

func doctorSourceMode(status doctorSourceStatus, statusLoadError string) doctorCheck {
	const id = "source.mode"
	title := "Config-as-code healthy"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if statusLoadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: statusLoadError, SuggestedAction: "Check source status API reachability", DocLink: doc}
	}

	evidence := formatDoctorSourceModeEvidence(status)
	if !status.RepositoriesConfigured {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "config-as-code is missing source repository persistence", Evidence: evidence, SuggestedAction: "Check API database wiring", DocLink: doc}
	}

	enabled := status.Repositories.Enabled
	if enabled == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "config-as-code has no enabled source repositories", Evidence: evidence, SuggestedAction: "Declare or enable a source repository", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("config-as-code ready: %d enabled source repositories", enabled), Evidence: evidence, DocLink: doc}
}

func formatDoctorSourceModeEvidence(status doctorSourceStatus) string {
	return strings.Join([]string{
		fmt.Sprintf("repositories_configured=%t", status.RepositoriesConfigured),
		fmt.Sprintf("schedules_configured=%t", status.SchedulesConfigured),
		fmt.Sprintf("declared_repositories=%d", status.DeclaredRepositories),
		fmt.Sprintf("declared_schedules=%d", status.DeclaredSchedules),
		fmt.Sprintf("repositories=%d", status.Repositories.Total),
		fmt.Sprintf("enabled_repositories=%d", status.Repositories.Enabled),
		fmt.Sprintf("disabled_repositories=%d", status.Repositories.Disabled),
		fmt.Sprintf("schedules=%d", status.Schedules.Total),
		fmt.Sprintf("enabled_schedules=%d", status.Schedules.Enabled),
		fmt.Sprintf("disabled_schedules=%d", status.Schedules.Disabled),
	}, " ")
}

func doctorSourceRepositorySync(repositories []sourceRepositorySummary, loadError string) doctorCheck {
	const id = "source.repositories.sync"
	title := "Source repository sync healthy"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if loadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: loadError, SuggestedAction: "Check source repository API reachability", DocLink: doc}
	}

	if len(repositories) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source repositories configured", DocLink: doc}
	}

	var enabled, disabled, succeeded, failed, running, never, unknown int
	failedRepositories := make([]string, 0)
	credentialFailedRepositories := make([]string, 0)
	staleRunningRepositories := make([]string, 0)
	unknownStatusRepositories := make([]string, 0)
	staleBeforeUnix := time.Now().Add(-config.SourceSyncRunningTimeout()).Unix()

	for _, repo := range repositories {
		if !repo.Enabled {
			disabled++
			continue
		}

		enabled++
		status := strings.TrimSpace(repo.Sync.Status)
		if status == "" {
			status = "never"
		}

		switch status {
		case "succeeded":
			succeeded++
		case "failed":
			failed++
			failedRepositories = append(failedRepositories, repo.RepositoryID)
			if sourceRepositorySyncCredentialFailure(repo.Sync.Error) {
				credentialFailedRepositories = append(credentialFailedRepositories, repo.RepositoryID)
			}
		case "running":
			running++
			if repo.Sync.LastStartedAtUnix > 0 && repo.Sync.LastStartedAtUnix <= staleBeforeUnix {
				staleRunningRepositories = append(staleRunningRepositories, repo.RepositoryID)
			}
		case "never":
			never++
		default:
			unknown++
			unknownStatusRepositories = append(unknownStatusRepositories, fmt.Sprintf("%s:%s", repo.RepositoryID, status))
		}
	}

	evidence := formatDoctorSourceRepositorySyncEvidence(len(repositories), enabled, disabled, succeeded, failed, running, never, unknown, failedRepositories, credentialFailedRepositories, staleRunningRepositories, unknownStatusRepositories)
	if len(failedRepositories) > 0 || len(staleRunningRepositories) > 0 || len(unknownStatusRepositories) > 0 {
		problems := make([]string, 0, 3)
		if len(failedRepositories) > 0 {
			problems = append(problems, fmt.Sprintf("%d failed", len(failedRepositories)))
		}

		if len(credentialFailedRepositories) > 0 {
			problems = append(problems, fmt.Sprintf("%d credential resolution failed", len(credentialFailedRepositories)))
		}

		if len(staleRunningRepositories) > 0 {
			problems = append(problems, fmt.Sprintf("%d running past timeout", len(staleRunningRepositories)))
		}

		if len(unknownStatusRepositories) > 0 {
			problems = append(problems, fmt.Sprintf("%d unknown status", len(unknownStatusRepositories)))
		}

		suggestedAction := "Run vectis-cli sources status <repository-id> or retry vectis-cli sources sync <repository-id>"
		if len(credentialFailedRepositories) > 0 {
			suggestedAction = "Run vectis-cli sources status <repository-id>, verify credential_ref and source credential resolver configuration, then retry vectis-cli sources sync <repository-id>"
		}

		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "source repository sync needs attention: " + strings.Join(problems, ", "), Evidence: evidence, SuggestedAction: suggestedAction, DocLink: doc}
	}

	if enabled == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("no enabled source repositories (%d disabled)", disabled), Evidence: evidence, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source repository sync ok: %d enabled", enabled), Evidence: evidence, DocLink: doc}
}

func sourceRepositorySyncCredentialFailure(syncError string) bool {
	errText := strings.ToLower(strings.TrimSpace(syncError))
	return strings.HasPrefix(errText, "git_credentials_unavailable:") ||
		strings.Contains(errText, "credential_ref is configured but source credential resolver is not configured") ||
		strings.Contains(errText, "resolve source repository credential") ||
		strings.Contains(errText, "parse source repository credential")
}

func doctorSourceRepositorySyncFromStatus(status doctorSourceStatus, statusLoadError string) doctorCheck {
	const id = "source.repositories.sync"
	title := "Source repository sync healthy"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if statusLoadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: statusLoadError, SuggestedAction: "Check source status API reachability", DocLink: doc}
	}

	if status.Repositories.Total == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source repositories configured", DocLink: doc}
	}

	evidence := formatDoctorSourceRepositorySyncEvidence(
		status.Repositories.Total,
		status.Repositories.Enabled,
		status.Repositories.Disabled,
		status.Repositories.SyncSucceeded,
		status.Repositories.SyncFailed,
		status.Repositories.SyncRunning,
		status.Repositories.SyncNever,
		0,
		nil,
		nil,
		nil,
		nil,
	)

	if status.Repositories.Enabled == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("no enabled source repositories (%d disabled)", status.Repositories.Disabled), Evidence: evidence, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source repository sync ok: %d enabled", status.Repositories.Enabled), Evidence: evidence, DocLink: doc}
}

func formatDoctorSourceRepositorySyncEvidence(total, enabled, disabled, succeeded, failed, running, never, unknown int, failedRepositories, credentialFailedRepositories, staleRunningRepositories, unknownStatusRepositories []string) string {
	parts := []string{
		fmt.Sprintf("repositories=%d", total),
		fmt.Sprintf("enabled=%d", enabled),
		fmt.Sprintf("disabled=%d", disabled),
		fmt.Sprintf("succeeded=%d", succeeded),
		fmt.Sprintf("failed=%d", failed),
		fmt.Sprintf("running=%d", running),
		fmt.Sprintf("never=%d", never),
		fmt.Sprintf("unknown=%d", unknown),
	}

	appendRepositoryList := func(label string, values []string) {
		if len(values) == 0 {
			return
		}

		sort.Strings(values)
		parts = append(parts, fmt.Sprintf("%s=%s", label, strings.Join(values, ",")))
	}

	appendRepositoryList("failed_repositories", failedRepositories)
	appendRepositoryList("credential_failed_repositories", credentialFailedRepositories)
	appendRepositoryList("stale_running_repositories", staleRunningRepositories)
	appendRepositoryList("unknown_status_repositories", unknownStatusRepositories)

	return strings.Join(parts, " ")
}

func doctorSourceRepositoryDeclarations(repositories []sourceRepositorySummary, loadError string) doctorCheck {
	const id = "source.repositories.declared"
	title := "Source repositories declared"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if loadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: loadError, SuggestedAction: "Check source repository API reachability", DocLink: doc}
	}

	if len(repositories) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source repositories configured", DocLink: doc}
	}

	var enabled, disabled, declared, staleEnabled, staleDisabled int
	staleEnabledIDs := make([]string, 0)
	staleDisabledIDs := make([]string, 0)
	for _, repo := range repositories {
		if repo.Enabled {
			enabled++
		} else {
			disabled++
		}

		if repo.Declared {
			declared++
			continue
		}

		if repo.Enabled {
			staleEnabled++
			staleEnabledIDs = append(staleEnabledIDs, repo.RepositoryID)
		} else {
			staleDisabled++
			staleDisabledIDs = append(staleDisabledIDs, repo.RepositoryID)
		}
	}

	evidence := formatDoctorSourceRepositoryDeclarationEvidence(len(repositories), enabled, disabled, declared, staleEnabled, staleDisabled, staleEnabledIDs, staleDisabledIDs)
	if staleEnabled > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d enabled stale source repositories", staleEnabled), Evidence: evidence, SuggestedAction: "Disable stale source repositories or restore their source repository declarations", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source repositories aligned: %d repositories", len(repositories)), Evidence: evidence, DocLink: doc}
}

func doctorSourceRepositoryDeclarationsFromStatus(status doctorSourceStatus, statusLoadError string) doctorCheck {
	const id = "source.repositories.declared"
	title := "Source repositories declared"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if statusLoadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: statusLoadError, SuggestedAction: "Check source status API reachability", DocLink: doc}
	}

	if status.Repositories.Total == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source repositories configured", DocLink: doc}
	}

	evidence := formatDoctorSourceRepositoryDeclarationEvidence(status.Repositories.Total, status.Repositories.Enabled, status.Repositories.Disabled, status.Repositories.Declared, status.Repositories.StaleEnabled, status.Repositories.StaleDisabled, nil, nil)
	if status.Repositories.StaleEnabled > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d enabled stale source repositories", status.Repositories.StaleEnabled), Evidence: evidence, SuggestedAction: "Disable stale source repositories or restore their source repository declarations", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source repositories aligned: %d repositories", status.Repositories.Total), Evidence: evidence, DocLink: doc}
}

func formatDoctorSourceRepositoryDeclarationEvidence(total, enabled, disabled, declared, staleEnabled, staleDisabled int, staleEnabledIDs, staleDisabledIDs []string) string {
	parts := []string{
		fmt.Sprintf("repositories=%d", total),
		fmt.Sprintf("enabled=%d", enabled),
		fmt.Sprintf("disabled=%d", disabled),
		fmt.Sprintf("declared=%d", declared),
		fmt.Sprintf("stale_enabled=%d", staleEnabled),
		fmt.Sprintf("stale_disabled=%d", staleDisabled),
	}

	appendRepositoryList := func(label string, values []string) {
		if len(values) == 0 {
			return
		}

		sort.Strings(values)
		parts = append(parts, fmt.Sprintf("%s=%s", label, strings.Join(values, ",")))
	}

	appendRepositoryList("stale_enabled_ids", staleEnabledIDs)
	appendRepositoryList("stale_disabled_ids", staleDisabledIDs)

	return strings.Join(parts, " ")
}

func doctorSourceScheduleDeclarations(schedules []sourceScheduleSummary, loadError string) doctorCheck {
	const id = "source.schedules.declared"
	title := "Source schedules declared"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if loadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: loadError, SuggestedAction: "Check source schedule API reachability", DocLink: doc}
	}

	if len(schedules) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source schedules configured", DocLink: doc}
	}

	var enabled, disabled, declared, staleEnabled, staleDisabled int
	staleEnabledIDs := make([]string, 0)
	staleDisabledIDs := make([]string, 0)
	for _, schedule := range schedules {
		if schedule.Enabled {
			enabled++
		} else {
			disabled++
		}

		if schedule.Declared {
			declared++
			continue
		}

		if schedule.Enabled {
			staleEnabled++
			staleEnabledIDs = append(staleEnabledIDs, schedule.ScheduleID)
		} else {
			staleDisabled++
			staleDisabledIDs = append(staleDisabledIDs, schedule.ScheduleID)
		}
	}

	evidence := formatDoctorSourceScheduleDeclarationEvidence(len(schedules), enabled, disabled, declared, staleEnabled, staleDisabled, staleEnabledIDs, staleDisabledIDs)
	if staleEnabled > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d enabled stale source schedules", staleEnabled), Evidence: evidence, SuggestedAction: "Disable stale source schedules or restore their source schedule declarations", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source schedules aligned: %d schedules", len(schedules)), Evidence: evidence, DocLink: doc}
}

func doctorSourceScheduleDeclarationsFromStatus(status doctorSourceStatus, statusLoadError string) doctorCheck {
	const id = "source.schedules.declared"
	title := "Source schedules declared"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if statusLoadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: statusLoadError, SuggestedAction: "Check source status API reachability", DocLink: doc}
	}

	if status.Schedules.Total == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source schedules configured", DocLink: doc}
	}

	evidence := formatDoctorSourceScheduleDeclarationEvidence(status.Schedules.Total, status.Schedules.Enabled, status.Schedules.Disabled, status.Schedules.Declared, status.Schedules.StaleEnabled, status.Schedules.StaleDisabled, nil, nil)
	if status.Schedules.StaleEnabled > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d enabled stale source schedules", status.Schedules.StaleEnabled), Evidence: evidence, SuggestedAction: "Disable stale source schedules or restore their source schedule declarations", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("source schedules aligned: %d schedules", status.Schedules.Total), Evidence: evidence, DocLink: doc}
}

func formatDoctorSourceScheduleDeclarationEvidence(total, enabled, disabled, declared, staleEnabled, staleDisabled int, staleEnabledIDs, staleDisabledIDs []string) string {
	parts := []string{
		fmt.Sprintf("schedules=%d", total),
		fmt.Sprintf("enabled=%d", enabled),
		fmt.Sprintf("disabled=%d", disabled),
		fmt.Sprintf("declared=%d", declared),
		fmt.Sprintf("stale_enabled=%d", staleEnabled),
		fmt.Sprintf("stale_disabled=%d", staleDisabled),
	}

	appendScheduleList := func(label string, values []string) {
		if len(values) == 0 {
			return
		}

		sort.Strings(values)
		parts = append(parts, fmt.Sprintf("%s=%s", label, strings.Join(values, ",")))
	}

	appendScheduleList("stale_enabled_ids", staleEnabledIDs)
	appendScheduleList("stale_disabled_ids", staleDisabledIDs)

	return strings.Join(parts, " ")
}

func doctorSourceScheduleOverrides(schedules []sourceScheduleSummary, loadError string) doctorCheck {
	const id = "source.schedules.overrides"
	title := "Source schedule overrides clear"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if loadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: loadError, SuggestedAction: "Check source schedule API reachability", DocLink: doc}
	}

	if len(schedules) == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source schedules configured", DocLink: doc}
	}

	overrideIDs := make([]string, 0)
	for _, schedule := range schedules {
		if schedule.Override != nil {
			overrideIDs = append(overrideIDs, schedule.ScheduleID)
		}
	}

	evidence := formatDoctorSourceScheduleOverrideEvidence(len(schedules), len(overrideIDs), overrideIDs)
	if len(overrideIDs) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d active source schedule overrides", len(overrideIDs)), Evidence: evidence, SuggestedAction: "Clear source schedule overrides after hotfixes land back in source", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no active source schedule overrides", Evidence: evidence, DocLink: doc}
}

func doctorSourceScheduleOverridesFromStatus(status doctorSourceStatus, statusLoadError string) doctorCheck {
	const id = "source.schedules.overrides"
	title := "Source schedule overrides clear"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	if statusLoadError != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: statusLoadError, SuggestedAction: "Check source status API reachability", DocLink: doc}
	}

	if status.Schedules.Total == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no source schedules configured", DocLink: doc}
	}

	evidence := formatDoctorSourceScheduleOverrideEvidence(status.Schedules.Total, status.Schedules.ActiveOverrides, nil)
	if status.Schedules.ActiveOverrides > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%d active source schedule overrides", status.Schedules.ActiveOverrides), Evidence: evidence, SuggestedAction: "Clear source schedule overrides after hotfixes land back in source", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "no active source schedule overrides", Evidence: evidence, DocLink: doc}
}

func formatDoctorSourceScheduleOverrideEvidence(total, overrideCount int, overrideIDs []string) string {
	parts := []string{
		fmt.Sprintf("schedules=%d", total),
		fmt.Sprintf("overrides=%d", overrideCount),
	}

	if len(overrideIDs) > 0 {
		sort.Strings(overrideIDs)
		parts = append(parts, fmt.Sprintf("override_ids=%s", strings.Join(overrideIDs, ",")))
	}

	return strings.Join(parts, " ")
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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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

func doctorEncryptedFSFiles() doctorCheck {
	const id = "secrets.encryptedfs.files"
	title := "EncryptedFS secret files valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"
	action := fmt.Sprintf("Check %s and %s", encryptedfs.EnvRoot, encryptedfs.EnvKeyFile)

	root, rootConfigured := envValue(encryptedfs.EnvRoot)
	keyFile, keyConfigured := envValue(encryptedfs.EnvKeyFile)

	if !rootConfigured && !keyConfigured {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "encryptedfs secret provider is not configured", DocLink: doc}
	}

	if root == "" || keyFile == "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "encryptedfs root and key file must be configured together", Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: action, DocLink: doc}
	}

	statPath, exists, err := existingPathForStat(root)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("encryptedfs root is not usable: %v", err), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: "Check encryptedfs root ownership and parent path", DocLink: doc}
	}

	if !exists {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "encryptedfs root does not exist", Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: "Create the encryptedfs root on durable private storage", DocLink: doc}
	}

	if err := directoryUsable(root); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("encryptedfs root is not writable: %v", err), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: "Check encryptedfs root ownership and permissions", DocLink: doc}
	}

	if _, err := doctorFilesystemStats(statPath); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("cannot inspect filesystem for encryptedfs root: %v", err), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: "Run vectis-cli health check on the host that owns the encryptedfs root", DocLink: doc}
	}

	info, err := os.Stat(keyFile)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("encryptedfs key file is not readable: %v", err), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: action, DocLink: doc}
	}

	if !info.Mode().IsRegular() {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: "encryptedfs key file is not a regular file", Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: action, DocLink: doc}
	}

	if doctorChecksPOSIXModeBits() && info.Mode().Perm()&0o077 != 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("encryptedfs key file permissions are too broad: %s", info.Mode().Perm()), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: "Restrict encryptedfs key file permissions to the service owner", DocLink: doc}
	}

	if _, err := encryptedfs.LoadEncryptedFSKeyFile(keyFile); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("encryptedfs key file is invalid: %v", err), Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), SuggestedAction: action, DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "encryptedfs root is writable and key file is valid", Evidence: formatDoctorEncryptedFSEvidence(root, keyFile), DocLink: doc}
}

func formatDoctorEncryptedFSEvidence(root, keyFile string) string {
	return fmt.Sprintf("root=%s key_file=%s", root, keyFile)
}

func doctorWorkerCoreSockets() doctorCheck {
	const id = "worker.core.sockets"
	title := "Worker core sockets private"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	coreRaw, coreConfigured := envValue("VECTIS_WORKER_CORE_SOCKET")
	shellRaw, shellConfigured := envValue("VECTIS_WORKER_CORE_SHELL_SOCKET")
	if !coreConfigured && !shellConfigured {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "worker-core socket paths are not configured in this shell", DocLink: doc}
	}

	checks := []doctorSocketPathCheck{}
	if coreConfigured {
		checks = append(checks, doctorSocketPathCheck{label: "core", raw: coreRaw})
	}

	if shellConfigured {
		checks = append(checks, doctorSocketPathCheck{label: "shell", raw: shellRaw})
	}

	resolved := make(map[string]string, len(checks))
	var problems []string
	for _, check := range checks {
		path, err := workercore.SocketPathFromEndpoint(check.raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s socket path is invalid: %v", check.label, err))
			continue
		}

		resolved[check.label] = path
		if problem := doctorUnixSocketProblem(check.label, path); problem != "" {
			problems = append(problems, problem)
		}
	}

	if core, ok := resolved["core"]; ok {
		if shell, ok := resolved["shell"]; ok && core == shell {
			problems = append(problems, "core and shell sockets must be distinct")
		}
	}

	evidence := formatDoctorWorkerCoreSocketEvidence(resolved)
	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(problems, "; "), Evidence: evidence, SuggestedAction: "Check VECTIS_WORKER_CORE_SOCKET, VECTIS_WORKER_CORE_SHELL_SOCKET, worker-core, and worker service ownership", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "configured worker-core sockets are present and private", Evidence: evidence, DocLink: doc}
}

func doctorLogForwarderSocket() doctorCheck {
	const id = "log.forwarder.socket"
	title := "Log forwarder socket private"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	raw, configured := envValue("VECTIS_LOG_FORWARDER_SOCKET")
	if !configured {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "log-forwarder socket path is not configured in this shell", DocLink: doc}
	}

	socketPath := strings.TrimSpace(raw)
	evidence := fmt.Sprintf("socket=%s", socketPath)
	if problem := doctorUnixSocketProblem("log-forwarder", socketPath); problem != "" {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: problem, Evidence: evidence, SuggestedAction: "Check VECTIS_LOG_FORWARDER_SOCKET, vectis-log-forwarder service state, and runtime directory ownership", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "configured log-forwarder socket is present and private", Evidence: evidence, DocLink: doc}
}

func doctorWorkerSPIFFEConfig() doctorCheck {
	const id = "worker.spiffe.config"
	title := "Worker SPIFFE config valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	doctorBindWorkerSPIFFEEnv()
	evidence := formatDoctorWorkerSPIFFEEvidence()
	if err := config.ValidateWorkerExecutionIdentityConfig(); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("worker execution identity config is invalid: %v", err), Evidence: evidence, SuggestedAction: "Check VECTIS_WORKER_EXECUTION_IDENTITY_* values", DocLink: doc}
	}

	if err := config.ValidateWorkerSPIFFEConfig(); err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("worker SPIFFE config is invalid: %v", err), Evidence: evidence, SuggestedAction: "Check VECTIS_WORKER_SPIFFE_* values and worker execution identity settings", DocLink: doc}
	}

	if !config.WorkerSPIFFEEnabled() {
		summary := "worker SPIFFE is disabled"
		if config.WorkerExecutionIdentityEnabled() {
			summary = "worker SPIFFE is disabled; execution identity derivation is enabled"
		}
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: summary, Evidence: evidence, DocLink: doc}
	}

	var problems []string
	workloadPath, err := doctorUnixAddressPath(config.WorkerSPIFFEWorkloadAPIAddress())
	if err != nil {
		problems = append(problems, fmt.Sprintf("SPIFFE Workload API address is invalid: %v", err))
	} else if problem := doctorProtectedUnixSocketProblem("SPIFFE Workload API", workloadPath); problem != "" {
		problems = append(problems, problem)
	}

	if config.WorkerSPIFFERegistrationEnabled() {
		registrationPath, err := doctorUnixAddressPath(config.WorkerSPIFFERegistrationServerAddress())
		if err != nil {
			problems = append(problems, fmt.Sprintf("SPIFFE registration API address is invalid: %v", err))
		} else if problem := doctorProtectedUnixSocketProblem("SPIFFE registration API", registrationPath); problem != "" {
			problems = append(problems, problem)
		}
	}

	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(problems, "; "), Evidence: evidence, SuggestedAction: "Check SPIFFE socket mounts, service state, and socket permissions on the worker host", DocLink: doc}
	}

	summary := "worker SPIFFE config ok; Workload API socket is present"
	if config.WorkerSPIFFERegistrationEnabled() {
		summary = "worker SPIFFE config ok; Workload API and registration sockets are present"
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: summary, Evidence: evidence, DocLink: doc}
}

func doctorBindWorkerSPIFFEEnv() {
	for key, envName := range map[string]string{
		"worker.execution_identity.enabled":         "VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED",
		"worker.execution_identity.trust_domain":    "VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN",
		"worker.execution_identity.path_template":   "VECTIS_WORKER_EXECUTION_IDENTITY_PATH_TEMPLATE",
		"worker.spiffe.enabled":                     "VECTIS_WORKER_SPIFFE_ENABLED",
		"worker.spiffe.workload_api_address":        "VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS",
		"worker.spiffe.fetch_timeout":               "VECTIS_WORKER_SPIFFE_FETCH_TIMEOUT",
		"worker.spiffe.registration.enabled":        "VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED",
		"worker.spiffe.registration.server_address": "VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS",
		"worker.spiffe.registration.parent_id":      "VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID",
		"worker.spiffe.registration.selectors":      "VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS",
		"worker.spiffe.registration.x509_svid_ttl":  "VECTIS_WORKER_SPIFFE_REGISTRATION_X509_SVID_TTL",
		"worker.spiffe.registration.min_ttl":        "VECTIS_WORKER_SPIFFE_REGISTRATION_MIN_TTL",
		"worker.spiffe.registration.max_ttl":        "VECTIS_WORKER_SPIFFE_REGISTRATION_MAX_TTL",
	} {
		_ = viper.BindEnv(key, envName)
	}
}

func formatDoctorWorkerSPIFFEEvidence() string {
	parts := []string{
		fmt.Sprintf("enabled=%t", config.WorkerSPIFFEEnabled()),
		fmt.Sprintf("execution_identity_enabled=%t", config.WorkerExecutionIdentityEnabled()),
		fmt.Sprintf("registration_enabled=%t", config.WorkerSPIFFERegistrationEnabled()),
		fmt.Sprintf("fetch_timeout=%s", config.WorkerSPIFFEFetchTimeout()),
	}

	if address := config.WorkerSPIFFEWorkloadAPIAddress(); strings.TrimSpace(address) != "" {
		parts = append(parts, fmt.Sprintf("workload_api=%s", address))
	}

	if address := config.WorkerSPIFFERegistrationServerAddress(); strings.TrimSpace(address) != "" {
		parts = append(parts, fmt.Sprintf("registration_api=%s", address))
	}

	if specs := config.WorkerSPIFFERegistrationSelectorSpecs(); len(specs) > 0 {
		parts = append(parts, fmt.Sprintf("registration_selectors=%d", len(specs)))
	}

	return strings.Join(parts, " ")
}

func doctorUnixAddressPath(address string) (string, error) {
	address = strings.TrimSpace(address)
	u, err := url.Parse(address)
	if err != nil {
		return "", err
	}

	if u.Scheme != "unix" {
		return "", fmt.Errorf("must use unix:// scheme")
	}

	if strings.TrimSpace(u.Host) != "" || strings.TrimSpace(u.Path) == "" || !strings.HasPrefix(u.Path, "/") {
		return "", fmt.Errorf("must be in unix:///path/to/socket form")
	}

	if u.RawQuery != "" || u.Fragment != "" || u.User != nil || u.Opaque != "" {
		return "", fmt.Errorf("must not include userinfo, query, fragment, or opaque data")
	}

	return u.Path, nil
}

func doctorProtectedUnixSocketProblem(label, path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Sprintf("%s socket path is empty", label)
	}

	if !filepath.IsAbs(path) {
		return fmt.Sprintf("%s socket path must be absolute", label)
	}

	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return fmt.Sprintf("%s socket parent is not usable: %v", label, err)
	}

	if !info.IsDir() {
		return fmt.Sprintf("%s socket parent is not a directory", label)
	}

	socketInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Sprintf("%s socket is not present: %v", label, err)
	}

	if socketInfo.Mode()&os.ModeSocket == 0 {
		return fmt.Sprintf("%s socket path is not a Unix socket", label)
	}

	if doctorChecksPOSIXModeBits() && socketInfo.Mode().Perm()&0o007 != 0 {
		return fmt.Sprintf("%s socket permissions are world-accessible: %s", label, socketInfo.Mode().Perm())
	}

	return ""
}

type doctorSocketPathCheck struct {
	label string
	raw   string
}

func doctorUnixSocketProblem(label, path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Sprintf("%s socket path is empty", label)
	}

	if !filepath.IsAbs(path) {
		return fmt.Sprintf("%s socket path must be absolute", label)
	}

	parent := filepath.Dir(path)
	info, err := os.Stat(parent)
	if err != nil {
		return fmt.Sprintf("%s socket parent is not usable: %v", label, err)
	}

	if !info.IsDir() {
		return fmt.Sprintf("%s socket parent is not a directory", label)
	}

	socketInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Sprintf("%s socket is not present: %v", label, err)
	}

	if socketInfo.Mode()&os.ModeSocket == 0 {
		return fmt.Sprintf("%s socket path is not a Unix socket", label)
	}

	if doctorChecksPOSIXModeBits() && socketInfo.Mode().Perm()&0o077 != 0 {
		return fmt.Sprintf("%s socket permissions are too broad: %s", label, socketInfo.Mode().Perm())
	}

	return ""
}

func doctorChecksPOSIXModeBits() bool {
	return runtime.GOOS != "windows"
}

func formatDoctorWorkerCoreSocketEvidence(paths map[string]string) string {
	labels := make([]string, 0, len(paths))
	for label := range paths {
		labels = append(labels, label)
	}

	sort.Strings(labels)
	parts := make([]string, 0, len(labels))
	for _, label := range labels {
		parts = append(parts, fmt.Sprintf("%s=%s", label, paths[label]))
	}

	return strings.Join(parts, " ")
}

func doctorWorkerWorkspaceFilesystem() doctorCheck {
	const id = "worker.workspace.filesystem"
	title := "Worker workspace filesystem healthy"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	backend, backendConfigured := envValue("VECTIS_WORKER_CORE_EXECUTION_BACKEND")
	workspaceRoot, rootConfigured := envValue("VECTIS_WORKER_CORE_WORKSPACE_ROOT")
	guestRoot, guestRootConfigured := envValue("VECTIS_WORKER_CORE_LIMA_GUEST_WORKSPACE_ROOT")

	if backend == "" {
		backend = workercore.ExecutionBackendHost
	}

	rootSource := "configured"
	if workspaceRoot == "" {
		workspaceRoot = os.TempDir()
		rootSource = "os_temp"
	}

	evidence := doctorWorkerWorkspaceEvidence{
		Backend:             backend,
		WorkspaceRoot:       workspaceRoot,
		WorkspaceRootSource: rootSource,
		GuestWorkspaceRoot:  guestRoot,
	}

	var warnings []string
	switch backend {
	case workercore.ExecutionBackendHost:
		if guestRootConfigured {
			warnings = append(warnings, "Lima guest workspace root is configured but execution backend is host")
		}
	case workercore.ExecutionBackendLima:
		if !guestRootConfigured {
			warnings = append(warnings, "Lima guest workspace root is not configured; host workspace root must be mounted and writable in the guest")
		} else if !strings.HasPrefix(guestRoot, "/") {
			warnings = append(warnings, "Lima guest workspace root should be an absolute guest path")
		}
	default:
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("unknown worker-core execution backend %q", backend), Evidence: evidence.String(), SuggestedAction: "Check VECTIS_WORKER_CORE_EXECUTION_BACKEND", DocLink: doc}
	}

	if workspaceRoot == "" {
		warnings = append(warnings, "workspace root is empty")
	} else if !filepath.IsAbs(workspaceRoot) {
		warnings = append(warnings, "workspace root should be an absolute path")
	}

	statPath, exists, err := existingPathForStat(workspaceRoot)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("workspace root is not usable: %v", err), Evidence: evidence.String(), SuggestedAction: "Check worker-core workspace root ownership and parent path", DocLink: doc}
	}

	evidence.StatPath = statPath
	if !exists {
		warnings = append(warnings, "workspace root does not exist")
	} else if err := directoryUsable(workspaceRoot); err != nil {
		warnings = append(warnings, fmt.Sprintf("workspace root is not writable: %v", err))
	}

	stats, err := doctorFilesystemStats(statPath)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("cannot inspect filesystem for workspace root: %v", err), Evidence: evidence.String(), SuggestedAction: "Run vectis-cli health check on the worker-core host", DocLink: doc}
	}

	evidence.FreeBytes = stats.freeBytes
	evidence.FreePercent = stats.freePercent
	if stats.freeInodesKnown {
		evidence.FreeInodes = stats.freeInodes
	}

	if stats.freeBytes < doctorDiskWarnFreeBytes || (stats.freeInodesKnown && stats.freeInodes == 0) {
		warnings = append(warnings, fmt.Sprintf("workspace filesystem pressure: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent))
	}

	if len(warnings) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(warnings, "; "), Evidence: evidence.String(), SuggestedAction: "Check worker-core workspace root, durable storage, and Lima workspace mapping", DocLink: doc}
	}

	summary := fmt.Sprintf("workspace filesystem ok: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent)
	if !backendConfigured && !rootConfigured && !guestRootConfigured {
		summary += "; using worker-core default temp root"
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: summary, Evidence: evidence.String(), DocLink: doc}
}

type doctorWorkerWorkspaceEvidence struct {
	Backend             string
	WorkspaceRoot       string
	WorkspaceRootSource string
	GuestWorkspaceRoot  string
	StatPath            string
	FreeBytes           uint64
	FreePercent         int
	FreeInodes          uint64
}

func (e doctorWorkerWorkspaceEvidence) String() string {
	parts := []string{
		"backend=" + e.Backend,
		"workspace_root=" + e.WorkspaceRoot,
		"workspace_root_source=" + e.WorkspaceRootSource,
	}

	if e.GuestWorkspaceRoot != "" {
		parts = append(parts, "guest_workspace_root="+e.GuestWorkspaceRoot)
	}

	if e.StatPath != "" {
		parts = append(parts, "stat_path="+e.StatPath)
	}

	if e.FreeBytes > 0 || e.FreePercent > 0 || e.FreeInodes > 0 {
		parts = append(parts,
			fmt.Sprintf("free_bytes=%d", e.FreeBytes),
			fmt.Sprintf("free_percent=%d", e.FreePercent),
			fmt.Sprintf("free_inodes=%d", e.FreeInodes),
		)
	}

	return strings.Join(parts, " ")
}

func doctorServiceIdentityConfig() doctorCheck {
	const id = "service.identity.config"
	title := "Service identity allowlists valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	doctorBindServiceIdentityEnv()

	allowlists := doctorServiceIdentityAllowlists()
	counts := make(map[string]int, len(allowlists))
	var problems []string
	configuredAllowlists := 0
	identityCount := 0
	for _, allowlist := range allowlists {
		if len(allowlist.identities) == 0 {
			continue
		}

		configuredAllowlists++
		counts[allowlist.evidenceLabel] = len(allowlist.identities)
		identityCount += len(allowlist.identities)

		normalized, err := serviceidentity.NormalizeSPIFFEAllowlist(allowlist.identities)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s allowlist is invalid: %v", allowlist.label, err))
			continue
		}

		counts[allowlist.evidenceLabel] = len(normalized)
		identityCount += len(normalized) - len(allowlist.identities)
	}

	evidence := formatDoctorServiceIdentityEvidence(counts, configuredAllowlists, identityCount)
	if configuredAllowlists == 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "service identity allowlists are not configured", Evidence: evidence, DocLink: doc}
	}

	if config.GRPCTLSInsecure() {
		problems = append(problems, "service identity allowlists require VECTIS_GRPC_TLS_INSECURE=false")
	}

	if doctorViperString("grpc_tls.cert_file") == "" || doctorViperString("grpc_tls.key_file") == "" {
		problems = append(problems, "service identity allowlists require VECTIS_GRPC_TLS_CERT_FILE and VECTIS_GRPC_TLS_KEY_FILE")
	}

	if doctorViperString("grpc_tls.client_ca_file") == "" {
		problems = append(problems, "service identity allowlists require VECTIS_GRPC_TLS_CLIENT_CA_FILE")
	}

	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(problems, "; "), Evidence: evidence, SuggestedAction: "Check VECTIS_SERVICE_IDENTITY_* allowlists and VECTIS_GRPC_TLS_* mTLS settings", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("service identity allowlists valid for %d listener roles", configuredAllowlists), Evidence: evidence, DocLink: doc}
}

type doctorServiceIdentityAllowlist struct {
	label         string
	evidenceLabel string
	identities    []string
}

func doctorServiceIdentityAllowlists() []doctorServiceIdentityAllowlist {
	return []doctorServiceIdentityAllowlist{
		{label: "registry", evidenceLabel: "registry", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleRegistry)},
		{label: "queue", evidenceLabel: "queue", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleQueue)},
		{label: "log", evidenceLabel: "log", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleLog)},
		{label: "artifact", evidenceLabel: "artifact", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleArtifact)},
		{label: "orchestrator", evidenceLabel: "orchestrator", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleOrchestrator)},
		{label: "worker control", evidenceLabel: "worker_control", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleWorkerControl)},
		{label: "secrets", evidenceLabel: "secrets", identities: config.ServiceIdentityAllowedClientIdentities(config.ServiceIdentityRoleSecrets)},
		{label: "cell ingress", evidenceLabel: "cell_ingress", identities: config.CellIngressAllowedProducerIdentities()},
	}
}

func formatDoctorServiceIdentityEvidence(counts map[string]int, configuredAllowlists, identityCount int) string {
	parts := []string{
		fmt.Sprintf("allowlists=%d", configuredAllowlists),
		fmt.Sprintf("identities=%d", identityCount),
		fmt.Sprintf("grpc_tls_insecure=%t", config.GRPCTLSInsecure()),
		fmt.Sprintf("server_cert_configured=%t", doctorViperString("grpc_tls.cert_file") != ""),
		fmt.Sprintf("server_key_configured=%t", doctorViperString("grpc_tls.key_file") != ""),
		fmt.Sprintf("client_ca_configured=%t", doctorViperString("grpc_tls.client_ca_file") != ""),
	}

	labels := make([]string, 0, len(counts))
	for label := range counts {
		labels = append(labels, label)
	}

	sort.Strings(labels)
	for _, label := range labels {
		parts = append(parts, fmt.Sprintf("%s=%d", label, counts[label]))
	}

	return strings.Join(parts, " ")
}

func doctorBindServiceIdentityEnv() {
	for _, binding := range []struct {
		key      string
		envNames []string
	}{
		{key: "grpc_tls.insecure", envNames: []string{"VECTIS_GRPC_TLS_INSECURE"}},
		{key: "grpc_tls.cert_file", envNames: []string{"VECTIS_GRPC_TLS_CERT_FILE"}},
		{key: "grpc_tls.key_file", envNames: []string{"VECTIS_GRPC_TLS_KEY_FILE"}},
		{key: "grpc_tls.client_ca_file", envNames: []string{"VECTIS_GRPC_TLS_CLIENT_CA_FILE"}},
		{key: "service_identity.registry_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_REGISTRY_ALLOWED_CLIENT_IDENTITIES", "VECTIS_REGISTRY_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.queue_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_QUEUE_ALLOWED_CLIENT_IDENTITIES", "VECTIS_QUEUE_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.log_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_LOG_ALLOWED_CLIENT_IDENTITIES", "VECTIS_LOG_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.artifact_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_ARTIFACT_ALLOWED_CLIENT_IDENTITIES", "VECTIS_ARTIFACT_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.orchestrator_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES", "VECTIS_ORCHESTRATOR_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.worker_control_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES", "VECTIS_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.secrets_allowed_client_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_SECRETS_ALLOWED_CLIENT_IDENTITIES", "VECTIS_SECRETS_ALLOWED_CLIENT_IDENTITIES"}},
		{key: "service_identity.cell_ingress_allowed_producer_identities", envNames: []string{"VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES", "VECTIS_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES"}},
	} {
		_ = viper.BindEnv(append([]string{binding.key}, binding.envNames...)...)
	}
}

func doctorViperString(key string) string {
	return strings.TrimSpace(viper.GetString(key))
}

func doctorMetricsListenersConfig() doctorCheck {
	const id = "metrics.listeners.config"
	title := "Dedicated metrics listeners valid"
	doc := "website/docs/operating/reference/health-check-catalog.md"

	doctorBindMetricsEnv()

	localVisible := doctorMetricsEnvVisible()
	surfaces := doctorMetricsSurfaces()
	evidence := doctorMetricsEvidence{
		LocalConfigVisible: localVisible,
		TLSEnabled:         !config.MetricsTLSInsecure(),
		TLSCertConfigured:  doctorViperString("metrics_tls.cert_file") != "",
		TLSKeyConfigured:   doctorViperString("metrics_tls.key_file") != "",
	}

	var problems []string
	if err := config.ValidateMetricsTLS(); err != nil {
		problems = append(problems, fmt.Sprintf("metrics TLS config is invalid: %v", err))
	}

	globalAllowed, globalAllowedConfigured := envValue("VECTIS_METRICS_ALLOWED_HOSTS")
	if globalAllowedConfigured {
		evidence.AllowedHostLists++
		if err := doctorValidateMetricsAllowedHosts(globalAllowed); err != nil {
			problems = append(problems, fmt.Sprintf("global metrics allowed Hosts are invalid: %v", err))
		}
	}

	for _, surface := range surfaces {
		host, hostConfigured := envValue(surface.hostEnv)
		if !hostConfigured {
			host = surface.defaultHost
		} else {
			evidence.ConfiguredBinds++
			if err := doctorValidateMetricsBindHost(host); err != nil {
				problems = append(problems, fmt.Sprintf("%s metrics host is invalid: %v", surface.label, err))
			}
		}

		_, allowedConfigured := envValue(surface.allowedEnv)
		if allowedConfigured {
			evidence.AllowedHostLists++
			allowed, _ := envValue(surface.allowedEnv)
			if err := doctorValidateMetricsAllowedHosts(allowed); err != nil {
				problems = append(problems, fmt.Sprintf("%s metrics allowed Hosts are invalid: %v", surface.label, err))
			}
		}

		if doctorMetricsHostOffHost(host) {
			evidence.OffHostBinds++
			if !globalAllowedConfigured && !allowedConfigured {
				problems = append(problems, fmt.Sprintf("%s metrics binds off localhost without VECTIS_METRICS_ALLOWED_HOSTS or %s", surface.label, surface.allowedEnv))
			}
		}
	}

	if len(problems) > 0 {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: strings.Join(problems, "; "), Evidence: evidence.String(), SuggestedAction: "Check VECTIS_METRICS_TLS_*, VECTIS_METRICS_ALLOWED_HOSTS, service metrics hosts, and service-specific metrics allowed Hosts", DocLink: doc}
	}

	if !localVisible {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "dedicated metrics listener config is not visible in this shell", Evidence: evidence.String(), DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "dedicated metrics listener config is valid", Evidence: evidence.String(), DocLink: doc}
}

type doctorMetricsSurface struct {
	label       string
	hostEnv     string
	allowedEnv  string
	defaultHost string
}

func doctorMetricsSurfaces() []doctorMetricsSurface {
	return []doctorMetricsSurface{
		{label: "queue", hostEnv: "VECTIS_QUEUE_METRICS_HOST", allowedEnv: "VECTIS_QUEUE_METRICS_ALLOWED_HOSTS", defaultHost: config.QueueMetricsHost()},
		{label: "orchestrator", hostEnv: "VECTIS_ORCHESTRATOR_METRICS_HOST", allowedEnv: "VECTIS_ORCHESTRATOR_METRICS_ALLOWED_HOSTS", defaultHost: config.OrchestratorMetricsHost()},
		{label: "worker", hostEnv: "VECTIS_WORKER_METRICS_HOST", allowedEnv: "VECTIS_WORKER_METRICS_ALLOWED_HOSTS", defaultHost: config.WorkerMetricsHost()},
		{label: "worker-core", hostEnv: "VECTIS_WORKER_CORE_METRICS_HOST", allowedEnv: "VECTIS_WORKER_CORE_METRICS_ALLOWED_HOSTS", defaultHost: config.WorkerCoreMetricsHost()},
		{label: "log", hostEnv: "VECTIS_LOG_METRICS_HOST", allowedEnv: "VECTIS_LOG_METRICS_ALLOWED_HOSTS", defaultHost: config.LogMetricsHost()},
		{label: "artifact", hostEnv: "VECTIS_ARTIFACT_METRICS_HOST", allowedEnv: "VECTIS_ARTIFACT_METRICS_ALLOWED_HOSTS", defaultHost: config.ArtifactMetricsHost()},
		{label: "log-forwarder", hostEnv: "VECTIS_LOG_FORWARDER_METRICS_HOST", allowedEnv: "VECTIS_LOG_FORWARDER_METRICS_ALLOWED_HOSTS", defaultHost: config.LogForwarderMetricsHost()},
		{label: "secrets", hostEnv: "VECTIS_SECRETS_METRICS_HOST", allowedEnv: "VECTIS_SECRETS_METRICS_ALLOWED_HOSTS", defaultHost: config.SecretsMetricsHost()},
		{label: "reconciler", hostEnv: "VECTIS_RECONCILER_METRICS_HOST", allowedEnv: "VECTIS_RECONCILER_METRICS_ALLOWED_HOSTS", defaultHost: config.ReconcilerMetricsHost()},
		{label: "catalog", hostEnv: "VECTIS_CATALOG_METRICS_HOST", allowedEnv: "VECTIS_CATALOG_METRICS_ALLOWED_HOSTS", defaultHost: config.CatalogMetricsHost()},
		{label: "cell-ingress", hostEnv: "VECTIS_CELL_INGRESS_METRICS_HOST", allowedEnv: "VECTIS_CELL_INGRESS_METRICS_ALLOWED_HOSTS", defaultHost: config.CellIngressMetricsHost()},
	}
}

type doctorMetricsEvidence struct {
	LocalConfigVisible bool
	TLSEnabled         bool
	TLSCertConfigured  bool
	TLSKeyConfigured   bool
	ConfiguredBinds    int
	OffHostBinds       int
	AllowedHostLists   int
}

func (e doctorMetricsEvidence) String() string {
	return strings.Join([]string{
		fmt.Sprintf("local_config_visible=%t", e.LocalConfigVisible),
		fmt.Sprintf("tls_enabled=%t", e.TLSEnabled),
		fmt.Sprintf("tls_cert_configured=%t", e.TLSCertConfigured),
		fmt.Sprintf("tls_key_configured=%t", e.TLSKeyConfigured),
		fmt.Sprintf("configured_binds=%d", e.ConfiguredBinds),
		fmt.Sprintf("off_host_binds=%d", e.OffHostBinds),
		fmt.Sprintf("allowed_host_lists=%d", e.AllowedHostLists),
	}, " ")
}

func doctorBindMetricsEnv() {
	for _, binding := range []struct {
		key      string
		envNames []string
	}{
		{key: "metrics_tls.insecure", envNames: []string{"VECTIS_METRICS_TLS_INSECURE"}},
		{key: "metrics_tls.cert_file", envNames: []string{"VECTIS_METRICS_TLS_CERT_FILE"}},
		{key: "metrics_tls.key_file", envNames: []string{"VECTIS_METRICS_TLS_KEY_FILE"}},
		{key: "metrics_tls.reload_interval", envNames: []string{"VECTIS_METRICS_TLS_RELOAD_INTERVAL"}},
	} {
		_ = viper.BindEnv(append([]string{binding.key}, binding.envNames...)...)
	}
}

func doctorMetricsEnvVisible() bool {
	names := []string{
		"VECTIS_METRICS_TLS_INSECURE",
		"VECTIS_METRICS_TLS_CERT_FILE",
		"VECTIS_METRICS_TLS_KEY_FILE",
		"VECTIS_METRICS_TLS_RELOAD_INTERVAL",
		"VECTIS_METRICS_ALLOWED_HOSTS",
	}
	for _, surface := range doctorMetricsSurfaces() {
		names = append(names, surface.hostEnv, surface.allowedEnv)
	}

	return anyEnvSet(names...)
}

func doctorValidateMetricsBindHost(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("host must not be empty")
	}

	if strings.Contains(host, "://") || strings.ContainsAny(host, "/?#@") || strings.ContainsFunc(host, isDoctorHostSpaceOrControl) {
		return fmt.Errorf("invalid host %q", host)
	}

	if _, _, err := net.SplitHostPort(host); err == nil {
		return fmt.Errorf("host must not include a port")
	}

	return nil
}

func doctorValidateMetricsAllowedHosts(raw string) error {
	hosts := doctorSplitCommaNonEmpty(raw)
	if len(hosts) == 0 {
		return fmt.Errorf("allowed host list must not be empty")
	}

	for _, host := range hosts {
		if err := doctorValidateAllowedHost(host); err != nil {
			return err
		}
	}

	return nil
}

func doctorSplitCommaNonEmpty(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}

	return out
}

func doctorValidateAllowedHost(raw string) error {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return fmt.Errorf("host must not be empty")
	}

	if value == "*" || strings.Contains(value, "*") {
		return fmt.Errorf("wildcard hosts are not allowed")
	}

	if strings.Contains(value, "://") || strings.ContainsAny(value, "/?#@") || strings.ContainsFunc(value, isDoctorHostSpaceOrControl) {
		return fmt.Errorf("invalid host %q", raw)
	}

	host := value
	port := ""
	if h, p, err := net.SplitHostPort(value); err == nil {
		host = h
		port = p
	} else if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		host = strings.Trim(value, "[]")
	}

	host = strings.TrimSuffix(strings.Trim(host, "[]"), ".")
	if host == "" {
		return fmt.Errorf("host must not be empty")
	}

	if strings.Contains(host, ":") && net.ParseIP(host) == nil {
		return fmt.Errorf("invalid host %q", raw)
	}

	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n <= 0 || n > 65535 {
			return fmt.Errorf("invalid port in host %q", raw)
		}
	}

	return nil
}

func doctorMetricsHostOffHost(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}

	host = strings.Trim(strings.TrimSpace(host), "[]")
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return false
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}

	return !ip.IsLoopback()
}

func isDoctorHostSpaceOrControl(r rune) bool {
	return r <= ' ' || r == 0x7f
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
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("%s path is not configured", label), SuggestedAction: "Configure the deploy path or run vectis-cli health check on the host that owns it", DocLink: doc}
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

	stats, err := doctorFilesystemStats(statPath)
	if err != nil {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("cannot inspect filesystem for %s: %v", label, err), Evidence: statPath, SuggestedAction: "Run vectis-cli health check on the host that owns the path", DocLink: doc}
	}

	freeInodes := "unknown"
	if stats.freeInodesKnown {
		freeInodes = strconv.FormatUint(stats.freeInodes, 10)
	}
	evidence := fmt.Sprintf("path=%s stat_path=%s free_bytes=%d free_percent=%d free_inodes=%s", path, statPath, stats.freeBytes, stats.freePercent, freeInodes)

	if stats.freeBytes < doctorDiskWarnFreeBytes || (stats.freeInodesKnown && stats.freeInodes == 0) {
		return doctorCheck{ID: id, Title: title, Status: doctorWarn, Severity: severityWarning, Summary: fmt.Sprintf("filesystem pressure: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent), Evidence: evidence, SuggestedAction: "Free disk space or move the path to a larger volume", DocLink: doc}
	}

	return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: fmt.Sprintf("filesystem ok: %s free (%d%%)", formatBytes(stats.freeBytes), stats.freePercent), Evidence: evidence, DocLink: doc}
}

func doctorArtifactFilesystemCheck() doctorCheck {
	const (
		id    = "artifact.storage.filesystem"
		title = "Artifact storage filesystem"
		doc   = "website/docs/operating/reference/health-check-catalog.md"
	)

	backend := strings.ToLower(strings.TrimSpace(os.Getenv("VECTIS_ARTIFACT_STORAGE_BACKEND")))
	if backend == "s3" {
		return doctorCheck{ID: id, Title: title, Status: doctorOK, Severity: severityWarning, Summary: "filesystem check skipped; artifact storage backend is s3", Evidence: "storage_backend=s3", DocLink: doc}
	}

	return doctorFilesystemPressure(id, title, "artifact storage", envOrDefault("VECTIS_ARTIFACT_STORAGE_DIR", defaultDoctorArtifactStorageDir()))
}

type doctorFSStats struct {
	freeBytes       uint64
	freePercent     int
	freeInodes      uint64
	freeInodesKnown bool
}

func filesystemStats(path string) (doctorFSStats, error) {
	stats, err := platform.StatFileSystem(path)
	if err != nil {
		return doctorFSStats{}, err
	}

	percent := 0
	if stats.TotalBytes > 0 {
		percent = int((stats.FreeBytes * 100) / stats.TotalBytes)
	}

	return doctorFSStats{
		freeBytes:       stats.FreeBytes,
		freePercent:     percent,
		freeInodes:      stats.FreeInodes,
		freeInodesKnown: stats.FreeInodesKnown,
	}, nil
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

	probe, err := os.CreateTemp(path, ".vectis-health-check-*")
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

func envValue(name string) (string, bool) {
	v, ok := os.LookupEnv(name)
	v = strings.TrimSpace(v)
	return v, ok && v != ""
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
	return filepath.Join(platform.DataHome(), "vectis", "log")
}

func defaultDoctorArtifactStorageDir() string {
	return filepath.Join(platform.DataHome(), "vectis", "artifact")
}

func defaultDoctorSourceCheckoutRoot() string {
	return config.SourceCheckoutRoot(platform.DataHome())
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

	return filepath.Join(platform.DataHome(), "vectis", "queue", sanitizeDoctorQueuePathComponent(pool), sanitizeDoctorQueuePathComponent(instanceID))
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
