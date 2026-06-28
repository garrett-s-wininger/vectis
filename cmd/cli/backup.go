package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vectis/internal/artifact"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/logserver"
	"vectis/internal/utils"
	"vectis/internal/version"
)

const backupSchemaInspectTimeout = 2 * time.Second
const backupDefaultQueuePool = "default"

type backupInventory struct {
	GeneratedAt  string                    `json:"generated_at"`
	Version      string                    `json:"version"`
	Database     backupDatabaseInventory   `json:"database"`
	Instances    []backupInstanceInventory `json:"instances"`
	LocalState   []backupPathInventory     `json:"local_state"`
	SecretStores []backupPathInventory     `json:"secret_stores,omitempty"`
	TLSFiles     []backupPathInventory     `json:"tls_files,omitempty"`
	ConfigPaths  []backupPathInventory     `json:"config_paths,omitempty"`
	Warnings     []string                  `json:"warnings,omitempty"`
}

type backupDatabaseInventory struct {
	Driver          string                        `json:"driver"`
	DriverSource    string                        `json:"driver_source"`
	SplitGlobalCell bool                          `json:"split_global_cell"`
	Roles           []backupDatabaseRoleInventory `json:"roles"`
}

type backupDatabaseRoleInventory struct {
	Role       string                `json:"role"`
	DSN        string                `json:"dsn"`
	DSNSource  string                `json:"dsn_source"`
	LocalPath  string                `json:"local_path,omitempty"`
	Schema     backupSchemaInventory `json:"schema"`
	SameAsRole string                `json:"same_as_role,omitempty"`
}

type backupSchemaInventory struct {
	Inspectable    bool   `json:"inspectable"`
	CurrentVersion *int   `json:"current_version,omitempty"`
	Dirty          *bool  `json:"dirty,omitempty"`
	Error          string `json:"error,omitempty"`
}

type backupInstanceInventory struct {
	Service    string `json:"service"`
	InstanceID string `json:"instance_id"`
	Source     string `json:"source"`
}

type backupPathInventory struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Path       string `json:"path,omitempty"`
	Source     string `json:"source"`
	Enabled    bool   `json:"enabled"`
	Exists     bool   `json:"exists"`
	IsDir      bool   `json:"is_dir,omitempty"`
	IsFile     bool   `json:"is_file,omitempty"`
	Readable   bool   `json:"readable"`
	Error      string `json:"error,omitempty"`
	BackupNote string `json:"backup_note,omitempty"`
}

var backupCmd = &cobra.Command{
	Use:     "backup",
	Short:   "Inspect backup and restore inputs",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var backupInventoryCmd = &cobra.Command{
	Use:   "inventory",
	Short: "Print local backup inventory evidence",
	Long: `Print a local, machine-readable inventory of Vectis durable state surfaces.

The inventory identifies database DSNs without credentials, local queue/log/artifact
paths, log spools, configured secret/TLS paths, and service instance IDs. It does
not perform a backup and does not read secret key material.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeBackupInventory(os.Stdout, time.Now().UTC()))
	},
}

func writeBackupInventory(w io.Writer, generatedAt time.Time) error {
	inventory := collectBackupInventory(generatedAt)
	if outputIsJSON() {
		return writeJSON(w, inventory)
	}

	return writeBackupInventoryText(w, inventory)
}

func collectBackupInventory(generatedAt time.Time) backupInventory {
	warnings := []string{
		"inventory is local to this host and environment; repeat on hosts that own other queue/log/artifact/spool paths",
	}

	queueInstanceID, queueInstanceSource := backupQueueInstanceID()
	logInstanceID, logInstanceSource := backupLogInstanceID()
	artifactInstanceID, artifactInstanceSource := backupArtifactInstanceID()
	cronInstanceID, cronInstanceSource := backupEnvOrEmpty("VECTIS_CRON_INSTANCE_ID")

	localState := []backupPathInventory{
		backupPath("queue.persistence", "directory", backupQueuePersistenceDir(queueInstanceID), backupQueuePersistenceSource(), true, "Back up this directory when queue persistence is enabled."),
		backupPath("log.storage", "directory", backupLogStorageDir(logInstanceID), backupPathSource("VECTIS_LOG_STORAGE_DIR", logInstanceSource), true, "Back up durable run logs for this log shard."),
		backupPath("artifact.storage", "directory", backupArtifactStorageDir(artifactInstanceID), backupPathSource("VECTIS_ARTIFACT_STORAGE_DIR", artifactInstanceSource), true, "Back up content-addressed artifact blobs for this artifact shard."),
		backupPath("log_forwarder.spool", "directory", backupEnvOrDefault("VECTIS_LOG_FORWARDER_SPOOL_DIR", defaultDoctorForwarderSpoolDir()), backupPathSource("VECTIS_LOG_FORWARDER_SPOOL_DIR", "default"), true, "Back up pending worker-side log batches when this host owns the spool."),
		backupPath("source.checkout_root", "directory", config.SourceCheckoutRoot(utils.DataHome()), backupSourceCheckoutRootSource(), true, "Back up managed checkouts only when the deployment relies on local source checkout state."),
		backupPath("worker.pending_log_spool", "directory", filepath.Join(os.TempDir(), "vectis-log-spool", "pending"), "derived from os.TempDir", true, "Temp-backed worker pending log spool; treat as best-effort until it becomes configurable."),
	}

	secretStores := backupSecretStorePaths()
	tlsFiles := backupTLSPaths()
	configPaths := backupConfigPaths()

	if len(secretStores) == 0 {
		warnings = append(warnings, "no encryptedfs secret store paths are configured in this environment")
	}
	if len(tlsFiles) == 0 {
		warnings = append(warnings, "no TLS material paths are configured in this environment")
	}

	return backupInventory{
		GeneratedAt: generatedAt.Format(time.RFC3339),
		Version:     version.String(),
		Database:    backupDatabase(),
		Instances: []backupInstanceInventory{
			{Service: "queue", InstanceID: queueInstanceID, Source: queueInstanceSource},
			{Service: "log", InstanceID: logInstanceID, Source: logInstanceSource},
			{Service: "artifact", InstanceID: artifactInstanceID, Source: artifactInstanceSource},
			{Service: "cron", InstanceID: cronInstanceID, Source: cronInstanceSource},
		},
		LocalState:   localState,
		SecretStores: secretStores,
		TLSFiles:     tlsFiles,
		ConfigPaths:  configPaths,
		Warnings:     warnings,
	}
}

func backupDatabase() backupDatabaseInventory {
	return backupDatabaseInventory{
		Driver:          database.EffectiveDBDriver(),
		DriverSource:    backupEnvSource(database.EnvDatabaseDriver, "defaults.toml"),
		SplitGlobalCell: database.GlobalAndCellDatabasesAreSplit(),
		Roles: []backupDatabaseRoleInventory{
			backupDatabaseRole("default", database.RoleDefault),
			backupDatabaseRole("global", database.RoleGlobal),
			backupDatabaseRole("cell", database.RoleCell),
		},
	}
}

func backupDatabaseRole(label string, role database.Role) backupDatabaseRoleInventory {
	dsn := database.GetDBPathForRole(role)
	roleInv := backupDatabaseRoleInventory{
		Role:      label,
		DSN:       redactDSN(dsn),
		DSNSource: backupDatabaseDSNSource(role),
		LocalPath: sqliteLocalPath(dsn),
		Schema:    inspectBackupSchema(dsn),
	}

	if label != "default" && dsn == database.GetDBPath() {
		roleInv.SameAsRole = "default"
	}

	return roleInv
}

func inspectBackupSchema(dsn string) backupSchemaInventory {
	driver := database.EffectiveDBDriver()
	if driver == "sqlite3" {
		path := sqliteLocalPath(dsn)
		if path == "" {
			return backupSchemaInventory{Inspectable: false, Error: "sqlite DSN is not a local file path"}
		}
		if _, err := os.Stat(path); err != nil {
			return backupSchemaInventory{Inspectable: false, Error: err.Error()}
		}
	}

	db, err := database.OpenDB(dsn)
	if err != nil {
		return backupSchemaInventory{Inspectable: false, Error: err.Error()}
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), backupSchemaInspectTimeout)
	defer cancel()

	var version int
	var dirty bool
	err = db.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&version, &dirty)
	if err != nil {
		if err == sql.ErrNoRows {
			return backupSchemaInventory{Inspectable: true, Error: "schema_migrations has no rows"}
		}
		return backupSchemaInventory{Inspectable: false, Error: err.Error()}
	}

	return backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty}
}

func backupQueueInstanceID() (string, string) {
	if value, source := backupEnvOrEmpty("VECTIS_QUEUE_INSTANCE_ID"); value != "" {
		return value, source
	}

	port := config.QueuePort()
	if raw := strings.TrimSpace(os.Getenv("VECTIS_QUEUE_PORT")); raw != "" {
		if parsed, err := parsePositiveInt(raw); err == nil {
			port = parsed
		}
	}

	return backupDefaultQueueInstanceID(port), "derived from hostname and queue port"
}

func backupLogInstanceID() (string, string) {
	if value, source := backupEnvOrEmpty("VECTIS_LOG_INSTANCE_ID"); value != "" {
		return value, source
	}

	return logserver.DefaultInstanceID(backupListenAddr("VECTIS_LOG_GRPC_PORT", config.LogGRPCPort())), "derived from hostname and log grpc port"
}

func backupArtifactInstanceID() (string, string) {
	if value, source := backupEnvOrEmpty("VECTIS_ARTIFACT_INSTANCE_ID"); value != "" {
		return value, source
	}

	return artifact.DefaultInstanceID(backupListenAddr("VECTIS_ARTIFACT_GRPC_PORT", config.ArtifactGRPCPort())), "derived from hostname and artifact grpc port"
}

func backupQueuePersistenceDir(instanceID string) string {
	if _, ok := os.LookupEnv("VECTIS_QUEUE_PERSISTENCE_DIR"); ok {
		return os.Getenv("VECTIS_QUEUE_PERSISTENCE_DIR")
	}

	pool := strings.TrimSpace(os.Getenv("VECTIS_QUEUE_POOL"))
	if pool == "" {
		pool = backupDefaultQueuePool
	}

	return backupDefaultQueuePersistenceDir(pool, instanceID)
}

func backupQueuePersistenceSource() string {
	if _, ok := os.LookupEnv("VECTIS_QUEUE_PERSISTENCE_DIR"); ok {
		return "VECTIS_QUEUE_PERSISTENCE_DIR"
	}
	return "derived from queue pool and instance ID"
}

func backupLogStorageDir(instanceID string) string {
	return backupEnvOrDefault("VECTIS_LOG_STORAGE_DIR", filepath.Join(utils.DataHome(), "vectis", "log", instanceID))
}

func backupArtifactStorageDir(instanceID string) string {
	return backupEnvOrDefault("VECTIS_ARTIFACT_STORAGE_DIR", filepath.Join(utils.DataHome(), "vectis", "artifact", instanceID))
}

func backupListenAddr(portEnv string, fallbackPort int) string {
	port := fallbackPort
	if raw := strings.TrimSpace(os.Getenv(portEnv)); raw != "" {
		if parsed, err := parsePositiveInt(raw); err == nil {
			port = parsed
		}
	}
	return ":" + strconv.Itoa(port)
}

func backupDefaultQueueInstanceID(port int) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	host := backupSanitizeQueuePathComponent(hostname)
	if host == "" {
		host = "localhost"
	}
	if port <= 0 {
		return host
	}
	return fmt.Sprintf("%s-%d", host, port)
}

func backupDefaultQueuePersistenceDir(pool, instanceID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "queue", backupSanitizeQueuePathComponent(pool), backupSanitizeQueuePathComponent(instanceID))
}

func backupSanitizeQueuePathComponent(value string) string {
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

	return strings.Trim(b.String(), "-")
}

func backupSecretStorePaths() []backupPathInventory {
	paths := []backupPathInventory{}
	root := backupEnvOrDefault("VECTIS_SECRETS_ENCRYPTEDFS_ROOT", config.SecretsEncryptedFSRoot())
	keyFile := backupEnvOrDefault("VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE", config.SecretsEncryptedFSKeyFile())
	if root != "" {
		paths = append(paths, backupPath("secrets.encryptedfs.root", "directory", root, backupPathSource("VECTIS_SECRETS_ENCRYPTEDFS_ROOT", "config/default"), true, "Back up encrypted secret envelopes."))
	}
	if keyFile != "" {
		paths = append(paths, backupPath("secrets.encryptedfs.key_file", "file", keyFile, backupPathSource("VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE", "config/default"), true, "Back up provider key material separately with secret-handling controls."))
	}
	return paths
}

func backupTLSPaths() []backupPathInventory {
	specs := []struct {
		id   string
		kind string
		env  string
		note string
	}{
		{id: "grpc.ca_file", kind: "file", env: "VECTIS_GRPC_TLS_CA_FILE", note: "Back up gRPC trust material."},
		{id: "grpc.cert_file", kind: "file", env: "VECTIS_GRPC_TLS_CERT_FILE", note: "Back up gRPC server certificate material."},
		{id: "grpc.key_file", kind: "file", env: "VECTIS_GRPC_TLS_KEY_FILE", note: "Back up gRPC server private key material with secret-handling controls."},
		{id: "grpc.client_ca_file", kind: "file", env: "VECTIS_GRPC_TLS_CLIENT_CA_FILE", note: "Back up gRPC client CA material."},
		{id: "grpc.client_cert_file", kind: "file", env: "VECTIS_GRPC_TLS_CLIENT_CERT_FILE", note: "Back up gRPC client certificate material."},
		{id: "grpc.client_key_file", kind: "file", env: "VECTIS_GRPC_TLS_CLIENT_KEY_FILE", note: "Back up gRPC client private key material with secret-handling controls."},
		{id: "metrics.cert_file", kind: "file", env: "VECTIS_METRICS_TLS_CERT_FILE", note: "Back up metrics TLS certificate material."},
		{id: "metrics.key_file", kind: "file", env: "VECTIS_METRICS_TLS_KEY_FILE", note: "Back up metrics TLS private key material with secret-handling controls."},
		{id: "api.cert_file", kind: "file", env: "VECTIS_API_TLS_CERT_FILE", note: "Back up API HTTPS certificate material."},
		{id: "api.key_file", kind: "file", env: "VECTIS_API_TLS_KEY_FILE", note: "Back up API HTTPS private key material with secret-handling controls."},
		{id: "api.server_cert_file", kind: "file", env: "VECTIS_API_SERVER_TLS_CERT_FILE", note: "Back up API HTTPS certificate material."},
		{id: "api.server_key_file", kind: "file", env: "VECTIS_API_SERVER_TLS_KEY_FILE", note: "Back up API HTTPS private key material with secret-handling controls."},
		{id: "docs.cert_file", kind: "file", env: "VECTIS_DOCS_TLS_CERT_FILE", note: "Back up docs HTTPS certificate material."},
		{id: "docs.key_file", kind: "file", env: "VECTIS_DOCS_TLS_KEY_FILE", note: "Back up docs HTTPS private key material with secret-handling controls."},
	}

	out := []backupPathInventory{}
	seen := map[string]bool{}
	for _, spec := range specs {
		path := strings.TrimSpace(os.Getenv(spec.env))
		if path == "" {
			continue
		}
		key := spec.id + "\x00" + path
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, backupPath(spec.id, spec.kind, path, spec.env, true, spec.note))
	}
	return out
}

func backupConfigPaths() []backupPathInventory {
	paths := []backupPathInventory{}
	if deployDir := strings.TrimSpace(os.Getenv("VECTIS_DEPLOY_CONFIG_DIR")); deployDir != "" {
		paths = append(paths, backupPath("deploy.config_dir", "directory", deployDir, "VECTIS_DEPLOY_CONFIG_DIR", true, "Back up rendered deployment secrets and manifests when using reference deploy helpers."))
	}
	return paths
}

func backupPath(id, kind, path, source string, enabled bool, note string) backupPathInventory {
	item := backupPathInventory{
		ID:         id,
		Kind:       kind,
		Path:       path,
		Source:     source,
		Enabled:    enabled && strings.TrimSpace(path) != "",
		BackupNote: note,
	}

	if !item.Enabled {
		return item
	}

	info, err := os.Stat(path)
	if err != nil {
		item.Error = err.Error()
		return item
	}

	item.Exists = true
	item.IsDir = info.IsDir()
	item.IsFile = !info.IsDir()
	item.Readable = backupReadable(path, info)
	return item
}

func backupReadable(path string, info os.FileInfo) bool {
	if info.IsDir() {
		f, err := os.Open(path)
		if err != nil {
			return false
		}
		_ = f.Close()
		return true
	}

	f, err := os.Open(path)
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
}

func writeBackupInventoryText(w io.Writer, inventory backupInventory) error {
	if _, err := fmt.Fprintf(w, "Backup inventory generated: %s\n", inventory.GeneratedAt); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Vectis version: %s\n", inventory.Version); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Database driver: %s (%s)\n", inventory.Database.Driver, inventory.Database.DriverSource); err != nil {
		return err
	}
	for _, role := range inventory.Database.Roles {
		sameAs := ""
		if role.SameAsRole != "" {
			sameAs = " same_as=" + role.SameAsRole
		}
		schema := "schema=unknown"
		if role.Schema.CurrentVersion != nil {
			schema = fmt.Sprintf("schema=%d dirty=%t", *role.Schema.CurrentVersion, role.Schema.Dirty != nil && *role.Schema.Dirty)
		} else if role.Schema.Error != "" {
			schema = "schema_error=" + role.Schema.Error
		}
		if _, err := fmt.Fprintf(w, "  db[%s]: %s source=%s%s %s\n", role.Role, role.DSN, role.DSNSource, sameAs, schema); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintln(w, "Instances:"); err != nil {
		return err
	}
	for _, instance := range inventory.Instances {
		if _, err := fmt.Fprintf(w, "  %s: %s (%s)\n", instance.Service, backupDash(instance.InstanceID), instance.Source); err != nil {
			return err
		}
	}

	if err := writeBackupPathText(w, "Local state", inventory.LocalState); err != nil {
		return err
	}
	if err := writeBackupPathText(w, "Secret stores", inventory.SecretStores); err != nil {
		return err
	}
	if err := writeBackupPathText(w, "TLS files", inventory.TLSFiles); err != nil {
		return err
	}
	if err := writeBackupPathText(w, "Config paths", inventory.ConfigPaths); err != nil {
		return err
	}

	if len(inventory.Warnings) > 0 {
		if _, err := fmt.Fprintln(w, "Warnings:"); err != nil {
			return err
		}
		for _, warning := range inventory.Warnings {
			if _, err := fmt.Fprintf(w, "  - %s\n", warning); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeBackupPathText(w io.Writer, title string, paths []backupPathInventory) error {
	if len(paths) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "%s:\n", title); err != nil {
		return err
	}
	for _, path := range paths {
		status := "missing"
		if !path.Enabled {
			status = "disabled"
		} else if path.Readable {
			status = "readable"
		} else if path.Exists {
			status = "unreadable"
		}
		if _, err := fmt.Fprintf(w, "  %s: %s [%s] source=%s\n", path.ID, backupDash(path.Path), status, path.Source); err != nil {
			return err
		}
	}
	return nil
}

func redactDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return ""
	}

	if parsed, err := url.Parse(dsn); err == nil && parsed.Scheme != "" {
		if parsed.User != nil {
			username := parsed.User.Username()
			if _, hasPassword := parsed.User.Password(); hasPassword {
				parsed.User = url.UserPassword(username, "REDACTED")
			} else {
				parsed.User = url.User(username)
			}
		}

		query := parsed.Query()
		for key := range query {
			if dsnKeyIsSensitive(key) {
				query.Set(key, "REDACTED")
			}
		}
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}

	fields := strings.Fields(dsn)
	for i, field := range fields {
		key, value, ok := strings.Cut(field, "=")
		if !ok || value == "" {
			continue
		}
		if dsnKeyIsSensitive(key) {
			fields[i] = key + "=REDACTED"
		}
	}
	if len(fields) > 0 {
		return strings.Join(fields, " ")
	}

	return dsn
}

func dsnKeyIsSensitive(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	return strings.Contains(key, "password") ||
		strings.Contains(key, "passwd") ||
		strings.Contains(key, "secret") ||
		strings.Contains(key, "token") ||
		key == "pass" ||
		key == "sslkey"
}

func sqliteLocalPath(dsn string) string {
	if database.EffectiveDBDriver() != "sqlite3" {
		return ""
	}

	path := strings.TrimSpace(dsn)
	if path == "" || path == ":memory:" {
		return path
	}
	if before, _, ok := strings.Cut(path, "?"); ok {
		return before
	}
	return path
}

func backupDatabaseDSNSource(role database.Role) string {
	switch role {
	case database.RoleGlobal:
		if os.Getenv(database.EnvGlobalDatabaseDSN) != "" {
			return database.EnvGlobalDatabaseDSN
		}
	case database.RoleCell:
		if os.Getenv(database.EnvCellDatabaseDSN) != "" {
			return database.EnvCellDatabaseDSN
		}
	}

	return backupEnvSource(database.EnvDatabaseDSN, "defaults.toml")
}

func backupEnvSource(envName, fallback string) string {
	if os.Getenv(envName) != "" {
		return envName
	}
	return fallback
}

func backupPathSource(envName, fallback string) string {
	if _, ok := os.LookupEnv(envName); ok {
		return envName
	}
	return fallback
}

func backupSourceCheckoutRootSource() string {
	for _, envName := range []string{"VECTIS_SOURCE_CHECKOUT_ROOT", "VECTIS_API_SERVER_SOURCE_CHECKOUT_ROOT"} {
		if os.Getenv(envName) != "" {
			return envName
		}
	}
	return "defaults.toml"
}

func backupEnvOrDefault(envName, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
		return value
	}
	return fallback
}

func backupEnvOrEmpty(envName string) (string, string) {
	if value := strings.TrimSpace(os.Getenv(envName)); value != "" {
		return value, envName
	}
	return "", "unset"
}

func backupDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func parsePositiveInt(raw string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, fmt.Errorf("not positive")
	}
	return n, nil
}
