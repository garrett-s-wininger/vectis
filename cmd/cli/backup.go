package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	linuxdeploy "vectis/deploy/linux"
	encryptedfs "vectis/extensions/secrets/encryptedfs"
	"vectis/internal/artifact"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/logserver"
	"vectis/internal/storageverify"
	"vectis/internal/utils"
	"vectis/internal/version"
)

const backupSchemaInspectTimeout = 2 * time.Second
const backupDefaultQueuePool = "default"
const backupManifestSchemaVersion = 1
const backupExpectedTopologySchemaVersion = 1
const backupRestoreValidationSchemaVersion = 1

const (
	backupManifestStatusOK     = "ok"
	backupManifestStatusFailed = "failed"

	backupFindingSeverityError   = "error"
	backupFindingSeverityWarning = "warning"
)

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

type backupInventoryInput struct {
	Source    string
	Inventory backupInventory
}

type backupManifest struct {
	SchemaVersion int                          `json:"schema_version"`
	GeneratedAt   string                       `json:"generated_at"`
	Version       string                       `json:"version"`
	Inventories   []backupManifestInventory    `json:"inventories"`
	DatabaseRoles []backupManifestDatabaseRole `json:"database_roles"`
	Instances     []backupManifestInstance     `json:"instances"`
	RequiredPaths []backupManifestPath         `json:"required_paths"`
	Warnings      []backupManifestFinding      `json:"warnings,omitempty"`
}

type backupManifestInventory struct {
	Source          string   `json:"source"`
	GeneratedAt     string   `json:"generated_at"`
	Version         string   `json:"version"`
	DatabaseDriver  string   `json:"database_driver"`
	SplitGlobalCell bool     `json:"split_global_cell"`
	Warnings        []string `json:"warnings,omitempty"`
}

type backupManifestDatabaseRole struct {
	InventorySource string                `json:"inventory_source"`
	Role            string                `json:"role"`
	Driver          string                `json:"driver"`
	DSN             string                `json:"dsn"`
	DSNSource       string                `json:"dsn_source"`
	LocalPath       string                `json:"local_path,omitempty"`
	Schema          backupSchemaInventory `json:"schema"`
	SameAsRole      string                `json:"same_as_role,omitempty"`
}

type backupManifestInstance struct {
	InventorySource string `json:"inventory_source"`
	Service         string `json:"service"`
	InstanceID      string `json:"instance_id"`
	Source          string `json:"source"`
}

type backupManifestPath struct {
	InventorySource string `json:"inventory_source"`
	Category        string `json:"category"`
	ID              string `json:"id"`
	Kind            string `json:"kind"`
	Path            string `json:"path,omitempty"`
	Source          string `json:"source"`
	Enabled         bool   `json:"enabled"`
	Exists          bool   `json:"exists"`
	IsDir           bool   `json:"is_dir,omitempty"`
	IsFile          bool   `json:"is_file,omitempty"`
	Readable        bool   `json:"readable"`
	Error           string `json:"error,omitempty"`
	BackupNote      string `json:"backup_note,omitempty"`
}

type backupManifestVerification struct {
	Status              string                            `json:"status"`
	CheckedAt           string                            `json:"checked_at"`
	ManifestGeneratedAt string                            `json:"manifest_generated_at,omitempty"`
	ExpectationSource   string                            `json:"expectation_source,omitempty"`
	StorageReportMaxAge string                            `json:"storage_report_max_age,omitempty"`
	Summary             backupManifestVerificationSummary `json:"summary"`
	StorageReports      []backupStorageReportEvidence     `json:"storage_reports,omitempty"`
	Errors              []backupManifestFinding           `json:"errors,omitempty"`
	Warnings            []backupManifestFinding           `json:"warnings,omitempty"`
}

type backupManifestVerificationSummary struct {
	Inventories                int `json:"inventories"`
	DatabaseRoles              int `json:"database_roles"`
	Instances                  int `json:"instances"`
	RequiredPaths              int `json:"required_paths"`
	ExpectedInventorySources   int `json:"expected_inventory_sources,omitempty"`
	ExpectedDatabaseRoles      int `json:"expected_database_roles,omitempty"`
	ExpectedInstances          int `json:"expected_instances,omitempty"`
	ExpectedPaths              int `json:"expected_paths,omitempty"`
	ExpectedRequiredCategories int `json:"expected_required_categories,omitempty"`
	StorageReports             int `json:"storage_reports,omitempty"`
	StorageReportsVerified     int `json:"storage_reports_verified,omitempty"`
	StoragePathsRequired       int `json:"storage_paths_required,omitempty"`
}

type backupManifestFinding struct {
	Severity        string `json:"severity"`
	ID              string `json:"id"`
	Message         string `json:"message"`
	InventorySource string `json:"inventory_source,omitempty"`
	Category        string `json:"category,omitempty"`
	PathID          string `json:"path_id,omitempty"`
	Path            string `json:"path,omitempty"`
}

type backupStorageReportInput struct {
	Source string
	Report storageverify.Report
}

type backupStorageReportEvidence struct {
	Source         string   `json:"source"`
	Surface        string   `json:"surface"`
	Path           string   `json:"path"`
	Status         string   `json:"status"`
	CheckedAt      string   `json:"checked_at"`
	Age            string   `json:"age,omitempty"`
	CheckedFiles   int64    `json:"checked_files"`
	CheckedBytes   int64    `json:"checked_bytes"`
	Records        int64    `json:"records"`
	Batches        int64    `json:"batches,omitempty"`
	Warnings       int      `json:"warnings"`
	Errors         int      `json:"errors"`
	MatchedPathIDs []string `json:"matched_path_ids,omitempty"`
}

type backupRestoreValidation struct {
	SchemaVersion  int                        `json:"schema_version"`
	Status         string                     `json:"status"`
	GeneratedAt    string                     `json:"generated_at"`
	Deployment     string                     `json:"deployment,omitempty"`
	Profile        string                     `json:"profile,omitempty"`
	Manifest       string                     `json:"manifest"`
	Expect         string                     `json:"expect,omitempty"`
	StorageReports []string                   `json:"storage_reports,omitempty"`
	Verification   backupManifestVerification `json:"verification"`
	SmokeRun       backupRestoreSmokeRun      `json:"smoke_run"`
}

type backupRestoreSmokeRun struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index,omitempty"`
	Status   string `json:"status"`
	Passed   bool   `json:"passed"`
}

type backupExpectedTopologyInput struct {
	Source       string
	Expectations backupExpectedTopology
}

type backupExpectedTopology struct {
	SchemaVersion     int                          `json:"schema_version,omitempty"`
	InventorySources  []string                     `json:"inventory_sources,omitempty"`
	DatabaseRoles     []backupExpectedDatabaseRole `json:"database_roles,omitempty"`
	Instances         []backupExpectedInstance     `json:"instances,omitempty"`
	Paths             []backupExpectedPath         `json:"paths,omitempty"`
	RequireCategories []string                     `json:"require_categories,omitempty"`
}

type backupExpectedDatabaseRole struct {
	InventorySource string `json:"inventory_source,omitempty"`
	Role            string `json:"role,omitempty"`
	Driver          string `json:"driver,omitempty"`
	LocalPath       string `json:"local_path,omitempty"`
	DSNSource       string `json:"dsn_source,omitempty"`
}

type backupExpectedInstance struct {
	InventorySource string `json:"inventory_source,omitempty"`
	Service         string `json:"service,omitempty"`
	InstanceID      string `json:"instance_id,omitempty"`
}

type backupExpectedPath struct {
	InventorySource string `json:"inventory_source,omitempty"`
	Category        string `json:"category,omitempty"`
	ID              string `json:"id,omitempty"`
	Path            string `json:"path,omitempty"`
}

var backupVerifyExpectPath string
var backupVerifyStorageReportPaths []string
var backupVerifyStorageMaxAge time.Duration
var backupRestoreValidationExpectPath string
var backupRestoreValidationStorageReportPaths []string
var backupRestoreValidationStorageMaxAge time.Duration
var backupRestoreValidationSmokeRunID string
var backupRestoreValidationDeployment string
var backupRestoreValidationProfile string
var backupExpectPodmanProfile = podmanProfileSimple
var backupExpectLinuxManifestPath = linuxdeploy.DefaultManifestPath

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

var backupManifestCmd = &cobra.Command{
	Use:   "manifest [inventory-json ...]",
	Short: "Build a backup manifest from inventory JSON",
	Long: `Build a machine-readable backup manifest from one or more inventory JSON files.

Run vectis-cli backup inventory --format json on each host that owns Vectis
state, then pass those JSON files here. The manifest records the database roles,
service instance IDs, and required local paths that the backup set must capture.`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeBackupManifest(os.Stdout, args, time.Now().UTC()))
	},
}

var backupExpectCmd = &cobra.Command{
	Use:   "expect",
	Short: "Generate expected backup topology inputs",
	Long: `Generate expected backup topology JSON for use with backup verify --expect.

The generated expectation describes deployment-owned backup surfaces that should
appear in a backup manifest. It does not inspect live state.`,
	Args: cobra.NoArgs,
	Run:  showCommandHelp,
}

var backupExpectPodmanCmd = &cobra.Command{
	Use:   "podman",
	Short: "Generate expected topology for the Podman reference deployment",
	Long: `Generate expected backup topology JSON for the Podman reference deployment.

Use this with vectis-cli backup verify --expect after collecting inventories and
building a backup manifest. The output is source-agnostic by default: it checks
that the expected Podman shard instances and paths exist somewhere in the
submitted manifest without assuming inventory file names.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeBackupPodmanExpectedTopology(os.Stdout, backupExpectPodmanProfile))
	},
}

var backupExpectLinuxCmd = &cobra.Command{
	Use:   "linux",
	Short: "Generate expected topology from Linux service artifacts",
	Long: `Generate expected backup topology JSON from the Linux service artifact manifest.

The Linux artifact manifest contains example environment for the standard
single-host service set. Config management still owns real host placement and
overrides, so treat this output as a baseline expectation that can be edited or
augmented for production topology.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeBackupLinuxExpectedTopology(os.Stdout, backupExpectLinuxManifestPath))
	},
}

var backupVerifyCmd = &cobra.Command{
	Use:   "verify [manifest-json]",
	Short: "Verify backup manifest completeness",
	Long: `Verify a backup manifest before using it as restore evidence.

The verifier checks for core database, queue, log, and artifact evidence,
missing or unreadable paths, dirty schema markers, and optional evidence gaps
such as secret store, TLS, or config paths. Pass --expect with an expected
topology JSON file to fail when a required host, service instance, database
role, path, or path category is absent from the submitted manifest. Pass
--storage-report with JSON output from vectis-cli storage verify to require
byte-level integrity evidence for each storage-backed local state path in the
manifest.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeBackupManifestVerification(os.Stdout, args[0], backupVerifyExpectPath, backupVerifyStorageReportPaths, backupVerifyStorageMaxAge, time.Now().UTC()))
	},
}

var backupRestoreValidationCmd = &cobra.Command{
	Use:   "restore-validation [manifest-json]",
	Short: "Build restore validation evidence from backup checks and a smoke run",
	Long: `Build a machine-readable restore validation artifact.

The command verifies the supplied backup manifest with the same expected-topology
and storage-report checks as backup verify, fetches the post-restore smoke run
from the live API, requires that run to have succeeded, and emits one validation
artifact operators can retain with release, compliance, or disaster-recovery
records.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		opts := backupRestoreValidationOptions{
			ManifestPath:       args[0],
			ExpectPath:         backupRestoreValidationExpectPath,
			StorageReportPaths: backupRestoreValidationStorageReportPaths,
			StorageMaxAge:      backupRestoreValidationStorageMaxAge,
			SmokeRunID:         backupRestoreValidationSmokeRunID,
			Deployment:         backupRestoreValidationDeployment,
			Profile:            backupRestoreValidationProfile,
		}
		runCLIError(writeBackupRestoreValidation(os.Stdout, opts, time.Now().UTC(), fetchRunDetail))
	},
}

type backupRestoreValidationOptions struct {
	ManifestPath       string
	ExpectPath         string
	StorageReportPaths []string
	StorageMaxAge      time.Duration
	SmokeRunID         string
	Deployment         string
	Profile            string
}

type backupRunDetailFetcher func(runID string) (runDetail, error)

func writeBackupInventory(w io.Writer, generatedAt time.Time) error {
	inventory := collectBackupInventory(generatedAt)
	if outputIsJSON() {
		return writeJSON(w, inventory)
	}

	return writeBackupInventoryText(w, inventory)
}

func writeBackupManifest(w io.Writer, inventoryPaths []string, generatedAt time.Time) error {
	inputs, err := readBackupInventoryInputs(inventoryPaths)
	if err != nil {
		return err
	}

	manifest := buildBackupManifest(inputs, generatedAt)
	if outputIsJSON() {
		return writeJSON(w, manifest)
	}

	return writeBackupManifestText(w, manifest)
}

func writeBackupManifestVerification(w io.Writer, manifestPath, expectPath string, storageReportPaths []string, storageMaxAge time.Duration, checkedAt time.Time) error {
	manifest, expected, storageReports, err := readBackupVerificationInputs(manifestPath, expectPath, storageReportPaths, storageMaxAge)
	if err != nil {
		return err
	}

	result := verifyBackupManifestWithStorage(manifest, expected, storageReports, storageMaxAge, checkedAt)
	if outputIsJSON() {
		if err := writeJSON(w, result); err != nil {
			return err
		}
	} else if err := writeBackupManifestVerificationText(w, result); err != nil {
		return err
	}

	if result.Status != backupManifestStatusOK {
		return fmt.Errorf("backup manifest verification failed: %d error(s)", len(result.Errors))
	}

	return nil
}

func writeBackupRestoreValidation(w io.Writer, opts backupRestoreValidationOptions, checkedAt time.Time, fetchRun backupRunDetailFetcher) error {
	if strings.TrimSpace(opts.SmokeRunID) == "" {
		return fmt.Errorf("--smoke-run is required")
	}
	if fetchRun == nil {
		return fmt.Errorf("restore validation smoke run fetcher is required")
	}

	manifest, expected, storageReports, err := readBackupVerificationInputs(opts.ManifestPath, opts.ExpectPath, opts.StorageReportPaths, opts.StorageMaxAge)
	if err != nil {
		return err
	}

	verification := verifyBackupManifestWithStorage(manifest, expected, storageReports, opts.StorageMaxAge, checkedAt)
	run, err := fetchRun(strings.TrimSpace(opts.SmokeRunID))
	if err != nil {
		return fmt.Errorf("fetch restore smoke run: %w", err)
	}
	if strings.TrimSpace(run.RunID) == "" {
		run.RunID = strings.TrimSpace(opts.SmokeRunID)
	}

	smoke := backupRestoreSmokeRun{
		RunID:    run.RunID,
		RunIndex: run.RunIndex,
		Status:   strings.TrimSpace(run.Status),
		Passed:   backupRunSucceeded(run.Status),
	}

	validation := backupRestoreValidation{
		SchemaVersion:  backupRestoreValidationSchemaVersion,
		Status:         backupManifestStatusOK,
		GeneratedAt:    checkedAt.Format(time.RFC3339),
		Deployment:     strings.TrimSpace(opts.Deployment),
		Profile:        strings.TrimSpace(opts.Profile),
		Manifest:       opts.ManifestPath,
		Expect:         strings.TrimSpace(opts.ExpectPath),
		StorageReports: append([]string(nil), opts.StorageReportPaths...),
		Verification:   verification,
		SmokeRun:       smoke,
	}
	if verification.Status != backupManifestStatusOK || !smoke.Passed {
		validation.Status = backupManifestStatusFailed
	}

	if outputIsJSON() {
		if err := writeJSON(w, validation); err != nil {
			return err
		}
	} else if err := writeBackupRestoreValidationText(w, validation); err != nil {
		return err
	}

	if verification.Status != backupManifestStatusOK {
		return fmt.Errorf("backup manifest verification failed: %d error(s)", len(verification.Errors))
	}
	if !smoke.Passed {
		return fmt.Errorf("restore smoke run %s finished with status %q", smoke.RunID, smoke.Status)
	}

	return nil
}

func readBackupVerificationInputs(manifestPath, expectPath string, storageReportPaths []string, storageMaxAge time.Duration) (backupManifest, *backupExpectedTopologyInput, []backupStorageReportInput, error) {
	if manifestPath == "-" && (expectPath == "-" || backupStorageReportPathsContainStdin(storageReportPaths)) {
		return backupManifest{}, nil, nil, fmt.Errorf("manifest, expected topology, and storage reports cannot share stdin")
	}
	if expectPath == "-" && backupStorageReportPathsContainStdin(storageReportPaths) {
		return backupManifest{}, nil, nil, fmt.Errorf("expected topology and storage reports cannot both be read from stdin")
	}
	if storageMaxAge > 0 && len(storageReportPaths) == 0 {
		return backupManifest{}, nil, nil, fmt.Errorf("--storage-max-age requires --storage-report")
	}

	manifest, err := readBackupManifestFile(manifestPath)
	if err != nil {
		return backupManifest{}, nil, nil, err
	}

	var expected *backupExpectedTopologyInput
	if strings.TrimSpace(expectPath) != "" {
		input, err := readBackupExpectedTopologyFile(expectPath)
		if err != nil {
			return backupManifest{}, nil, nil, err
		}

		expected = &input
	}

	storageReports, err := readBackupStorageReportInputs(storageReportPaths)
	if err != nil {
		return backupManifest{}, nil, nil, err
	}

	return manifest, expected, storageReports, nil
}

func writeBackupPodmanExpectedTopology(w io.Writer, profile string) error {
	expected, normalizedProfile, err := backupPodmanExpectedTopology(profile)
	if err != nil {
		return err
	}

	if outputIsJSON() {
		return writeJSON(w, expected)
	}

	return writeBackupExpectedTopologyText(w, "podman", normalizedProfile, expected)
}

func writeBackupLinuxExpectedTopology(w io.Writer, manifestPath string) error {
	manifest, err := linuxdeploy.LoadManifest(manifestPath)
	if err != nil {
		return fmt.Errorf("load Linux deploy manifest: %w", err)
	}

	expected := backupLinuxExpectedTopology(manifest)
	if outputIsJSON() {
		return writeJSON(w, expected)
	}

	return writeBackupExpectedTopologyText(w, "linux", manifestPath, expected)
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

func readBackupInventoryInputs(paths []string) ([]backupInventoryInput, error) {
	inputs := make([]backupInventoryInput, 0, len(paths))
	stdinUsed := false
	for _, path := range paths {
		source := strings.TrimSpace(path)
		if source == "" {
			return nil, fmt.Errorf("inventory path cannot be empty")
		}

		if source == "-" {
			if stdinUsed {
				return nil, fmt.Errorf("stdin inventory can only be read once")
			}
			stdinUsed = true
		}

		inventory, err := readBackupInventoryFile(source)
		if err != nil {
			return nil, err
		}

		inputs = append(inputs, backupInventoryInput{Source: source, Inventory: inventory})
	}

	return inputs, nil
}

func readBackupInventoryFile(path string) (backupInventory, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return backupInventory{}, fmt.Errorf("open backup inventory %s: %w", path, err)
		}
		defer f.Close()

		r = f
	}

	var inventory backupInventory
	if err := json.NewDecoder(r).Decode(&inventory); err != nil {
		return backupInventory{}, fmt.Errorf("decode backup inventory %s: %w", path, err)
	}

	if strings.TrimSpace(inventory.GeneratedAt) == "" {
		return backupInventory{}, fmt.Errorf("backup inventory %s is missing generated_at", path)
	}

	return inventory, nil
}

func readBackupManifestFile(path string) (backupManifest, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return backupManifest{}, fmt.Errorf("open backup manifest %s: %w", path, err)
		}
		defer f.Close()

		r = f
	}

	var manifest backupManifest
	if err := json.NewDecoder(r).Decode(&manifest); err != nil {
		return backupManifest{}, fmt.Errorf("decode backup manifest %s: %w", path, err)
	}

	return manifest, nil
}

func readBackupExpectedTopologyFile(path string) (backupExpectedTopologyInput, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return backupExpectedTopologyInput{}, fmt.Errorf("open backup expected topology %s: %w", path, err)
		}
		defer f.Close()

		r = f
	}

	var expectations backupExpectedTopology
	if err := json.NewDecoder(r).Decode(&expectations); err != nil {
		return backupExpectedTopologyInput{}, fmt.Errorf("decode backup expected topology %s: %w", path, err)
	}

	return backupExpectedTopologyInput{Source: path, Expectations: expectations}, nil
}

func readBackupStorageReportInputs(paths []string) ([]backupStorageReportInput, error) {
	inputs := make([]backupStorageReportInput, 0, len(paths))
	stdinUsed := false
	for _, path := range paths {
		source := strings.TrimSpace(path)
		if source == "" {
			return nil, fmt.Errorf("storage report path cannot be empty")
		}
		if source == "-" {
			if stdinUsed {
				return nil, fmt.Errorf("stdin storage report can only be read once")
			}
			stdinUsed = true
		}

		report, err := readBackupStorageReportFile(source)
		if err != nil {
			return nil, err
		}

		inputs = append(inputs, backupStorageReportInput{Source: source, Report: report})
	}

	return inputs, nil
}

func readBackupStorageReportFile(path string) (storageverify.Report, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return storageverify.Report{}, fmt.Errorf("open storage verification report %s: %w", path, err)
		}
		defer f.Close()

		r = f
	}

	var report storageverify.Report
	if err := json.NewDecoder(r).Decode(&report); err != nil {
		return storageverify.Report{}, fmt.Errorf("decode storage verification report %s: %w", path, err)
	}

	return report, nil
}

func backupStorageReportPathsContainStdin(paths []string) bool {
	for _, path := range paths {
		if strings.TrimSpace(path) == "-" {
			return true
		}
	}

	return false
}

func buildBackupManifest(inputs []backupInventoryInput, generatedAt time.Time) backupManifest {
	manifest := backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   generatedAt.Format(time.RFC3339),
		Version:       version.String(),
		Inventories:   make([]backupManifestInventory, 0, len(inputs)),
	}

	seenDatabasePaths := map[string]bool{}
	for _, input := range inputs {
		source := input.Source
		inventory := input.Inventory
		manifest.Inventories = append(manifest.Inventories, backupManifestInventory{
			Source:          source,
			GeneratedAt:     inventory.GeneratedAt,
			Version:         inventory.Version,
			DatabaseDriver:  inventory.Database.Driver,
			SplitGlobalCell: inventory.Database.SplitGlobalCell,
			Warnings:        inventory.Warnings,
		})

		for _, warning := range inventory.Warnings {
			manifest.Warnings = append(manifest.Warnings, backupManifestFinding{
				Severity:        backupFindingSeverityWarning,
				ID:              "inventory.warning",
				Message:         warning,
				InventorySource: source,
			})
		}

		for _, role := range inventory.Database.Roles {
			manifest.DatabaseRoles = append(manifest.DatabaseRoles, backupManifestDatabaseRole{
				InventorySource: source,
				Role:            role.Role,
				Driver:          inventory.Database.Driver,
				DSN:             role.DSN,
				DSNSource:       role.DSNSource,
				LocalPath:       role.LocalPath,
				Schema:          role.Schema,
				SameAsRole:      role.SameAsRole,
			})

			if role.LocalPath == "" || role.LocalPath == ":memory:" {
				continue
			}

			pathKey := source + "\x00" + role.LocalPath
			if seenDatabasePaths[pathKey] {
				continue
			}

			seenDatabasePaths[pathKey] = true
			manifest.RequiredPaths = append(manifest.RequiredPaths, backupManifestPath{
				InventorySource: source,
				Category:        "database",
				ID:              "database." + role.Role,
				Kind:            "file",
				Path:            role.LocalPath,
				Source:          role.DSNSource,
				Enabled:         true,
				Exists:          role.Schema.Inspectable,
				IsFile:          true,
				Readable:        role.Schema.Inspectable,
				Error:           role.Schema.Error,
				BackupNote:      "Back up this SQLite database file.",
			})
		}

		for _, instance := range inventory.Instances {
			if strings.TrimSpace(instance.Service) == "" {
				continue
			}

			manifest.Instances = append(manifest.Instances, backupManifestInstance{
				InventorySource: source,
				Service:         instance.Service,
				InstanceID:      instance.InstanceID,
				Source:          instance.Source,
			})
		}

		manifest.RequiredPaths = appendBackupManifestPaths(manifest.RequiredPaths, source, "local_state", inventory.LocalState)
		manifest.RequiredPaths = appendBackupManifestPaths(manifest.RequiredPaths, source, "secret_stores", inventory.SecretStores)
		manifest.RequiredPaths = appendBackupManifestPaths(manifest.RequiredPaths, source, "tls_files", inventory.TLSFiles)
		manifest.RequiredPaths = appendBackupManifestPaths(manifest.RequiredPaths, source, "config_paths", inventory.ConfigPaths)
	}

	return manifest
}

func backupPodmanExpectedTopology(profile string) (backupExpectedTopology, string, error) {
	profile, err := backupNormalizePodmanProfile(profile)
	if err != nil {
		return backupExpectedTopology{}, "", err
	}

	data := podmanTemplateDataForProfile(profile)
	expected := backupExpectedTopology{
		SchemaVersion: backupExpectedTopologySchemaVersion,
		DatabaseRoles: []backupExpectedDatabaseRole{
			{Role: "default", Driver: "pgx"},
			{Role: "global", Driver: "pgx"},
			{Role: "cell", Driver: "pgx"},
		},
		RequireCategories: []string{"local_state", "secret_stores", "config_paths"},
	}

	for _, shard := range data.QueueShards {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "queue",
			InstanceID: shard.InstanceID,
		})
		expected.Paths = append(expected.Paths, backupExpectedPath{
			Category: "local_state",
			ID:       "queue.persistence",
			Path:     shard.PersistenceDir,
		})
	}

	for _, shard := range data.LogShards {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "log",
			InstanceID: shard.InstanceID,
		})
		expected.Paths = append(expected.Paths, backupExpectedPath{
			Category: "local_state",
			ID:       "log.storage",
			Path:     shard.StorageDir,
		})
	}

	for _, shard := range data.ArtifactShards {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "artifact",
			InstanceID: shard.InstanceID,
		})
		expected.Paths = append(expected.Paths, backupExpectedPath{
			Category: "local_state",
			ID:       "artifact.storage",
			Path:     shard.StorageDir,
		})
	}

	expected.Paths = append(expected.Paths,
		backupExpectedPath{Category: "secret_stores", ID: "secrets.encryptedfs.root", Path: "/data/vectis/secrets/encryptedfs"},
		backupExpectedPath{Category: "secret_stores", ID: "secrets.encryptedfs.key_file", Path: "/run/vectis/secrets/encryptedfs.key"},
	)

	return expected, profile, nil
}

func backupLinuxExpectedTopology(manifest linuxdeploy.Manifest) backupExpectedTopology {
	driver := strings.TrimSpace(manifest.CommonEnvExample["VECTIS_DATABASE_DRIVER"])
	expected := backupExpectedTopology{
		SchemaVersion: backupExpectedTopologySchemaVersion,
		DatabaseRoles: []backupExpectedDatabaseRole{
			{Role: "default", Driver: driver},
			{Role: "global", Driver: driver},
			{Role: "cell", Driver: driver},
		},
		RequireCategories: []string{"local_state", "secret_stores", "config_paths"},
	}

	if env, ok := backupLinuxUnitEnv(manifest, "queue"); ok {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "queue",
			InstanceID: strings.TrimSpace(env["VECTIS_QUEUE_INSTANCE_ID"]),
		})
		if path := strings.TrimSpace(env["VECTIS_QUEUE_PERSISTENCE_DIR"]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "local_state",
				ID:       "queue.persistence",
				Path:     path,
			})
		}
	}

	if env, ok := backupLinuxUnitEnv(manifest, "log"); ok {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "log",
			InstanceID: strings.TrimSpace(env["VECTIS_LOG_INSTANCE_ID"]),
		})
		if path := strings.TrimSpace(env["VECTIS_LOG_STORAGE_DIR"]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "local_state",
				ID:       "log.storage",
				Path:     path,
			})
		}
	}

	if env, ok := backupLinuxUnitEnv(manifest, "artifact"); ok {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "artifact",
			InstanceID: strings.TrimSpace(env["VECTIS_ARTIFACT_INSTANCE_ID"]),
		})
		if path := strings.TrimSpace(env["VECTIS_ARTIFACT_STORAGE_DIR"]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "local_state",
				ID:       "artifact.storage",
				Path:     path,
			})
		}
	}

	if env, ok := backupLinuxUnitEnv(manifest, "cron"); ok {
		expected.Instances = append(expected.Instances, backupExpectedInstance{
			Service:    "cron",
			InstanceID: strings.TrimSpace(env["VECTIS_CRON_INSTANCE_ID"]),
		})
	}

	if env, ok := backupLinuxUnitEnv(manifest, "log-forwarder"); ok {
		if path := strings.TrimSpace(env["VECTIS_LOG_FORWARDER_SPOOL_DIR"]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "local_state",
				ID:       "log_forwarder.spool",
				Path:     path,
			})
		}
	}

	if env, ok := backupLinuxUnitEnv(manifest, "secrets"); ok {
		if path := strings.TrimSpace(env[encryptedfs.EnvRoot]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "secret_stores",
				ID:       "secrets.encryptedfs.root",
				Path:     path,
			})
		}
		if path := strings.TrimSpace(env[encryptedfs.EnvKeyFile]); path != "" {
			expected.Paths = append(expected.Paths, backupExpectedPath{
				Category: "secret_stores",
				ID:       "secrets.encryptedfs.key_file",
				Path:     path,
			})
		}
	}

	if configDir := backupLinuxConfigDir(manifest); configDir != "" {
		expected.Paths = append(expected.Paths, backupExpectedPath{
			Category: "config_paths",
			ID:       "linux.config_dir",
			Path:     configDir,
		})
	}

	return expected
}

func backupLinuxUnitEnv(manifest linuxdeploy.Manifest, id string) (map[string]string, bool) {
	for _, unit := range manifest.Units {
		if unit.ID == id {
			return unit.EnvExample, true
		}
	}
	return nil, false
}

func backupLinuxConfigDir(manifest linuxdeploy.Manifest) string {
	envFile := strings.TrimSpace(manifest.Defaults.CommonEnvFile)
	envFile = strings.TrimPrefix(envFile, "-")
	if envFile != "" {
		return filepath.Dir(envFile)
	}

	if configHome := strings.TrimSpace(manifest.CommonEnvExample["XDG_CONFIG_HOME"]); configHome != "" {
		return filepath.Join(configHome, "vectis")
	}

	return ""
}

func backupNormalizePodmanProfile(profile string) (string, error) {
	profile = strings.ToLower(strings.TrimSpace(profile))
	if profile == "" {
		profile = podmanProfileSimple
	}

	switch profile {
	case podmanProfileSimple, podmanProfileHA:
		return profile, nil
	default:
		return "", fmt.Errorf("invalid podman profile %q (must be simple or ha)", profile)
	}
}

func appendBackupManifestPaths(out []backupManifestPath, inventorySource, category string, paths []backupPathInventory) []backupManifestPath {
	for _, path := range paths {
		if !path.Enabled {
			continue
		}

		out = append(out, backupManifestPath{
			InventorySource: inventorySource,
			Category:        category,
			ID:              path.ID,
			Kind:            path.Kind,
			Path:            path.Path,
			Source:          path.Source,
			Enabled:         path.Enabled,
			Exists:          path.Exists,
			IsDir:           path.IsDir,
			IsFile:          path.IsFile,
			Readable:        path.Readable,
			Error:           path.Error,
			BackupNote:      path.BackupNote,
		})
	}

	return out
}

func verifyBackupManifest(manifest backupManifest, expected *backupExpectedTopologyInput, checkedAt time.Time) backupManifestVerification {
	return verifyBackupManifestWithStorage(manifest, expected, nil, 0, checkedAt)
}

func verifyBackupManifestWithStorage(manifest backupManifest, expected *backupExpectedTopologyInput, storageReports []backupStorageReportInput, storageMaxAge time.Duration, checkedAt time.Time) backupManifestVerification {
	result := backupManifestVerification{
		Status:              backupManifestStatusOK,
		CheckedAt:           checkedAt.Format(time.RFC3339),
		ManifestGeneratedAt: manifest.GeneratedAt,
		Summary: backupManifestVerificationSummary{
			Inventories:   len(manifest.Inventories),
			DatabaseRoles: len(manifest.DatabaseRoles),
			Instances:     len(manifest.Instances),
			RequiredPaths: len(manifest.RequiredPaths),
		},
	}

	if expected != nil {
		result.ExpectationSource = expected.Source
		result.Summary.ExpectedInventorySources = len(expected.Expectations.InventorySources)
		result.Summary.ExpectedDatabaseRoles = len(expected.Expectations.DatabaseRoles)
		result.Summary.ExpectedInstances = len(expected.Expectations.Instances)
		result.Summary.ExpectedPaths = len(expected.Expectations.Paths)
		result.Summary.ExpectedRequiredCategories = len(expected.Expectations.RequireCategories)
	}
	if len(storageReports) > 0 {
		result.Summary.StorageReports = len(storageReports)
		if storageMaxAge > 0 {
			result.StorageReportMaxAge = storageMaxAge.String()
		}
	}

	addError := func(id, message string) {
		result.Errors = append(result.Errors, backupManifestFinding{
			Severity: backupFindingSeverityError,
			ID:       id,
			Message:  message,
		})
	}

	addWarning := func(id, message string) {
		result.Warnings = append(result.Warnings, backupManifestFinding{
			Severity: backupFindingSeverityWarning,
			ID:       id,
			Message:  message,
		})
	}

	if manifest.SchemaVersion != backupManifestSchemaVersion {
		addError("manifest.schema_version", fmt.Sprintf("unsupported manifest schema version %d", manifest.SchemaVersion))
	}

	if len(manifest.Inventories) == 0 {
		addError("manifest.inventories.missing", "manifest has no inventory inputs")
	}

	if len(manifest.DatabaseRoles) == 0 {
		addError("database.roles.missing", "manifest has no database role evidence")
	}

	for _, role := range manifest.DatabaseRoles {
		if role.Schema.Dirty != nil && *role.Schema.Dirty {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "database.schema_dirty",
				Message:         fmt.Sprintf("database role %s schema is dirty", role.Role),
				InventorySource: role.InventorySource,
			})
		}

		if !role.Schema.Inspectable {
			result.Warnings = append(result.Warnings, backupManifestFinding{
				Severity:        backupFindingSeverityWarning,
				ID:              "database.schema_uninspectable",
				Message:         fmt.Sprintf("database role %s schema was not inspectable", role.Role),
				InventorySource: role.InventorySource,
			})
		} else if role.Schema.CurrentVersion == nil {
			result.Warnings = append(result.Warnings, backupManifestFinding{
				Severity:        backupFindingSeverityWarning,
				ID:              "database.schema_version_missing",
				Message:         fmt.Sprintf("database role %s schema version was not recorded", role.Role),
				InventorySource: role.InventorySource,
			})
		}
	}

	for _, path := range manifest.RequiredPaths {
		if !path.Enabled {
			continue
		}

		if strings.TrimSpace(path.Path) == "" {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "path.empty",
				Message:         fmt.Sprintf("required path %s has no path value", path.ID),
				InventorySource: path.InventorySource,
				Category:        path.Category,
				PathID:          path.ID,
			})

			continue
		}

		if !path.Exists {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "path.missing",
				Message:         fmt.Sprintf("required path %s is missing", path.ID),
				InventorySource: path.InventorySource,
				Category:        path.Category,
				PathID:          path.ID,
				Path:            path.Path,
			})

			continue
		}

		if !path.Readable {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "path.unreadable",
				Message:         fmt.Sprintf("required path %s is not readable", path.ID),
				InventorySource: path.InventorySource,
				Category:        path.Category,
				PathID:          path.ID,
				Path:            path.Path,
			})
		}
	}

	for _, required := range []string{"queue.persistence", "log.storage", "artifact.storage"} {
		if !manifestHasPathID(manifest, required) {
			addError("path."+required+".missing", "manifest is missing required local state path "+required)
		}
	}

	if !manifestHasPathCategory(manifest, "database") {
		addWarning("database.local_path.missing", "manifest has no local database path; this is expected for Postgres-only deployments")
	}

	if !manifestHasPathCategory(manifest, "secret_stores") {
		addWarning("secret_stores.missing", "manifest has no secret store paths")
	}

	if !manifestHasPathCategory(manifest, "tls_files") {
		addWarning("tls_files.missing", "manifest has no TLS material paths")
	}

	if !manifestHasPathCategory(manifest, "config_paths") {
		addWarning("config_paths.missing", "manifest has no deployment config paths")
	}

	for _, warning := range manifest.Warnings {
		result.Warnings = append(result.Warnings, warning)
	}

	if expected != nil {
		verifyBackupExpectedTopology(manifest, expected.Source, expected.Expectations, &result)
	}

	if len(storageReports) > 0 {
		verifyBackupStorageReports(manifest, storageReports, storageMaxAge, checkedAt, &result)
	}

	if len(result.Errors) > 0 {
		result.Status = backupManifestStatusFailed
	}

	return result
}

func verifyBackupStorageReports(manifest backupManifest, inputs []backupStorageReportInput, maxAge time.Duration, checkedAt time.Time, result *backupManifestVerification) {
	required := backupStorageRequiredPaths(manifest)
	result.Summary.StoragePathsRequired = len(required)

	requiredByKey := map[string][]backupManifestPath{}
	for _, path := range required {
		key := backupStorageReportKey(backupStorageSurfaceForPathID(path.ID), path.Path)
		requiredByKey[key] = append(requiredByKey[key], path)
	}

	matched := map[string]bool{}
	for _, input := range inputs {
		report := input.Report
		reportOK := true
		evidence := backupStorageReportEvidence{
			Source:       input.Source,
			Surface:      report.Surface,
			Path:         report.Path,
			Status:       report.Status,
			CheckedFiles: report.CheckedFiles,
			CheckedBytes: report.CheckedBytes,
			Records:      report.Records,
			Batches:      report.Batches,
			Warnings:     len(report.Warnings),
			Errors:       len(report.Errors),
		}
		if !report.CheckedAt.IsZero() {
			evidence.CheckedAt = report.CheckedAt.UTC().Format(time.RFC3339)
			age := checkedAt.Sub(report.CheckedAt.UTC())
			evidence.Age = age.String()
			if age < 0 {
				reportOK = false
				result.Errors = append(result.Errors, backupManifestFinding{
					Severity: backupFindingSeverityError,
					ID:       "storage_report.checked_at_future",
					Message:  fmt.Sprintf("storage report %s checked_at %s is after verification time %s", input.Source, evidence.CheckedAt, checkedAt.Format(time.RFC3339)),
					Path:     report.Path,
				})
			} else if maxAge > 0 && age > maxAge {
				reportOK = false
				result.Errors = append(result.Errors, backupManifestFinding{
					Severity: backupFindingSeverityError,
					ID:       "storage_report.stale",
					Message:  fmt.Sprintf("storage report %s is stale: checked_at=%s age=%s max_age=%s", input.Source, evidence.CheckedAt, age, maxAge),
					Path:     report.Path,
				})
			}
		} else {
			reportOK = false
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "storage_report.checked_at_missing",
				Message:  fmt.Sprintf("storage report %s is missing checked_at", input.Source),
				Path:     report.Path,
			})
		}

		if !backupStorageKnownSurface(report.Surface) {
			reportOK = false
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "storage_report.surface_unknown",
				Message:  fmt.Sprintf("storage report %s has unknown surface %q", input.Source, report.Surface),
				Path:     report.Path,
			})
		}

		if strings.TrimSpace(report.Path) == "" {
			reportOK = false
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "storage_report.path_missing",
				Message:  fmt.Sprintf("storage report %s is missing path", input.Source),
			})
		}

		if report.Status != storageverify.StatusOK {
			reportOK = false
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "storage_report.failed",
				Message:  fmt.Sprintf("storage report %s status is %s with %d error(s)", input.Source, backupDash(report.Status), len(report.Errors)),
				Path:     report.Path,
			})
		}

		if len(report.Warnings) > 0 {
			result.Warnings = append(result.Warnings, backupManifestFinding{
				Severity: backupFindingSeverityWarning,
				ID:       "storage_report.warnings",
				Message:  fmt.Sprintf("storage report %s has %d warning(s)", input.Source, len(report.Warnings)),
				Path:     report.Path,
			})
		}

		key := backupStorageReportKey(report.Surface, report.Path)
		if paths := requiredByKey[key]; len(paths) > 0 {
			for _, path := range paths {
				evidence.MatchedPathIDs = append(evidence.MatchedPathIDs, path.ID)
				if reportOK {
					matched[backupStorageManifestPathKey(path)] = true
				}
			}
			if reportOK {
				result.Summary.StorageReportsVerified++
			}
		} else {
			reportOK = false
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "storage_report.unmatched",
				Message:  fmt.Sprintf("storage report %s does not match any storage-backed local_state path in the manifest", input.Source),
				Path:     report.Path,
			})
		}

		result.StorageReports = append(result.StorageReports, evidence)
	}

	for _, path := range required {
		if !matched[backupStorageManifestPathKey(path)] {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "storage_report.missing",
				Message:         fmt.Sprintf("required storage path %s has no matching ok storage report", path.ID),
				InventorySource: path.InventorySource,
				Category:        path.Category,
				PathID:          path.ID,
				Path:            path.Path,
			})
		}
	}
}

func backupStorageRequiredPaths(manifest backupManifest) []backupManifestPath {
	out := make([]backupManifestPath, 0)
	for _, path := range manifest.RequiredPaths {
		if !path.Enabled || path.Category != "local_state" {
			continue
		}
		if backupStorageSurfaceForPathID(path.ID) == "" {
			continue
		}
		if strings.TrimSpace(path.Path) == "" {
			continue
		}

		out = append(out, path)
	}

	return out
}

func backupStorageSurfaceForPathID(id string) string {
	switch id {
	case "queue.persistence":
		return storageverify.SurfaceQueue
	case "log.storage":
		return storageverify.SurfaceLogs
	case "artifact.storage":
		return storageverify.SurfaceArtifact
	case "log_forwarder.spool":
		return storageverify.SurfaceLogForwarderSpool
	case "worker.pending_log_spool":
		return storageverify.SurfaceWorkerLogSpool
	default:
		return ""
	}
}

func backupStorageKnownSurface(surface string) bool {
	switch surface {
	case storageverify.SurfaceQueue,
		storageverify.SurfaceLogs,
		storageverify.SurfaceArtifact,
		storageverify.SurfaceLogForwarderSpool,
		storageverify.SurfaceWorkerLogSpool:
		return true
	default:
		return false
	}
}

func backupRunSucceeded(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "succeeded")
}

func backupStorageReportKey(surface, path string) string {
	return strings.TrimSpace(surface) + "\x00" + backupCleanStoragePath(path)
}

func backupStorageManifestPathKey(path backupManifestPath) string {
	return path.InventorySource + "\x00" + path.Category + "\x00" + path.ID + "\x00" + backupCleanStoragePath(path.Path)
}

func backupCleanStoragePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}

	return filepath.Clean(path)
}

func verifyBackupExpectedTopology(manifest backupManifest, source string, expected backupExpectedTopology, result *backupManifestVerification) {
	if expected.SchemaVersion != 0 && expected.SchemaVersion != backupExpectedTopologySchemaVersion {
		result.Errors = append(result.Errors, backupManifestFinding{
			Severity: backupFindingSeverityError,
			ID:       "expectation.schema_version",
			Message:  fmt.Sprintf("unsupported expected topology schema version %d", expected.SchemaVersion),
		})
	}

	for _, inventorySource := range expected.InventorySources {
		inventorySource = strings.TrimSpace(inventorySource)
		if inventorySource == "" {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.inventory_source_empty",
				Message:  "expected topology contains an empty inventory source",
			})

			continue
		}

		if !manifestHasInventorySource(manifest, inventorySource) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "expectation.inventory_missing",
				Message:         fmt.Sprintf("expected inventory source %s is missing", inventorySource),
				InventorySource: inventorySource,
			})
		}
	}

	for _, role := range expected.DatabaseRoles {
		if backupExpectedDatabaseRoleEmpty(role) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.database_role_empty",
				Message:  "expected topology contains an empty database role matcher",
			})

			continue
		}

		if !manifestHasExpectedDatabaseRole(manifest, role) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "expectation.database_role_missing",
				Message:         "expected database role is missing from manifest",
				InventorySource: role.InventorySource,
			})
		}
	}

	for _, instance := range expected.Instances {
		if strings.TrimSpace(instance.Service) == "" {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.instance_service_missing",
				Message:  "expected service instance is missing service",
			})

			continue
		}

		if !manifestHasExpectedInstance(manifest, instance) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "expectation.instance_missing",
				Message:         fmt.Sprintf("expected %s instance is missing from manifest", instance.Service),
				InventorySource: instance.InventorySource,
			})
		}
	}

	for _, path := range expected.Paths {
		if backupExpectedPathEmpty(path) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.path_empty",
				Message:  "expected topology contains an empty path matcher",
			})

			continue
		}

		if !manifestHasExpectedPath(manifest, path) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity:        backupFindingSeverityError,
				ID:              "expectation.path_missing",
				Message:         "expected path is missing from manifest",
				InventorySource: path.InventorySource,
				Category:        path.Category,
				PathID:          path.ID,
				Path:            path.Path,
			})
		}
	}

	for _, category := range expected.RequireCategories {
		category = strings.TrimSpace(category)
		if category == "" {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.category_empty",
				Message:  "expected topology contains an empty required category",
			})

			continue
		}

		if !manifestHasPathCategory(manifest, category) {
			result.Errors = append(result.Errors, backupManifestFinding{
				Severity: backupFindingSeverityError,
				ID:       "expectation.category_missing",
				Message:  fmt.Sprintf("expected path category %s is missing from manifest", category),
				Category: category,
			})
		}
	}

	if backupExpectedTopologyEmpty(expected) {
		result.Warnings = append(result.Warnings, backupManifestFinding{
			Severity: backupFindingSeverityWarning,
			ID:       "expectation.empty",
			Message:  fmt.Sprintf("expected topology %s did not define any requirements", source),
		})
	}
}

func manifestHasPathID(manifest backupManifest, id string) bool {
	for _, path := range manifest.RequiredPaths {
		if path.Enabled && path.ID == id {
			return true
		}
	}

	return false
}

func manifestHasPathCategory(manifest backupManifest, category string) bool {
	for _, path := range manifest.RequiredPaths {
		if path.Enabled && path.Category == category {
			return true
		}
	}

	return false
}

func manifestHasInventorySource(manifest backupManifest, source string) bool {
	for _, inventory := range manifest.Inventories {
		if inventory.Source == source {
			return true
		}
	}
	return false
}

func manifestHasExpectedDatabaseRole(manifest backupManifest, expected backupExpectedDatabaseRole) bool {
	for _, role := range manifest.DatabaseRoles {
		if expected.InventorySource != "" && role.InventorySource != expected.InventorySource {
			continue
		}
		if expected.Role != "" && role.Role != expected.Role {
			continue
		}
		if expected.Driver != "" && role.Driver != expected.Driver {
			continue
		}
		if expected.LocalPath != "" && role.LocalPath != expected.LocalPath {
			continue
		}
		if expected.DSNSource != "" && role.DSNSource != expected.DSNSource {
			continue
		}
		return true
	}
	return false
}

func manifestHasExpectedInstance(manifest backupManifest, expected backupExpectedInstance) bool {
	for _, instance := range manifest.Instances {
		if expected.InventorySource != "" && instance.InventorySource != expected.InventorySource {
			continue
		}
		if instance.Service != expected.Service {
			continue
		}
		if expected.InstanceID != "" && instance.InstanceID != expected.InstanceID {
			continue
		}
		return true
	}
	return false
}

func manifestHasExpectedPath(manifest backupManifest, expected backupExpectedPath) bool {
	for _, path := range manifest.RequiredPaths {
		if !path.Enabled {
			continue
		}
		if expected.InventorySource != "" && path.InventorySource != expected.InventorySource {
			continue
		}
		if expected.Category != "" && path.Category != expected.Category {
			continue
		}
		if expected.ID != "" && path.ID != expected.ID {
			continue
		}
		if expected.Path != "" && path.Path != expected.Path {
			continue
		}
		return true
	}
	return false
}

func backupExpectedTopologyEmpty(expected backupExpectedTopology) bool {
	return len(expected.InventorySources) == 0 &&
		len(expected.DatabaseRoles) == 0 &&
		len(expected.Instances) == 0 &&
		len(expected.Paths) == 0 &&
		len(expected.RequireCategories) == 0
}

func backupExpectedDatabaseRoleEmpty(expected backupExpectedDatabaseRole) bool {
	return strings.TrimSpace(expected.InventorySource) == "" &&
		strings.TrimSpace(expected.Role) == "" &&
		strings.TrimSpace(expected.Driver) == "" &&
		strings.TrimSpace(expected.LocalPath) == "" &&
		strings.TrimSpace(expected.DSNSource) == ""
}

func backupExpectedPathEmpty(expected backupExpectedPath) bool {
	return strings.TrimSpace(expected.InventorySource) == "" &&
		strings.TrimSpace(expected.Category) == "" &&
		strings.TrimSpace(expected.ID) == "" &&
		strings.TrimSpace(expected.Path) == ""
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
	root := backupEnvOrDefault(encryptedfs.EnvRoot, "")
	keyFile := backupEnvOrDefault(encryptedfs.EnvKeyFile, "")
	if root != "" {
		paths = append(paths, backupPath("secrets.encryptedfs.root", "directory", root, backupPathSource(encryptedfs.EnvRoot, "extension config"), true, "Back up encrypted secret envelopes."))
	}
	if keyFile != "" {
		paths = append(paths, backupPath("secrets.encryptedfs.key_file", "file", keyFile, backupPathSource(encryptedfs.EnvKeyFile, "extension config"), true, "Back up provider key material separately with secret-handling controls."))
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
	if configHome := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); configHome != "" {
		paths = append(paths, backupPath("linux.config_dir", "directory", filepath.Join(configHome, "vectis"), "XDG_CONFIG_HOME", true, "Back up Linux service environment files and host-local Vectis configuration."))
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

func writeBackupManifestText(w io.Writer, manifest backupManifest) error {
	if _, err := fmt.Fprintf(w, "Backup manifest generated: %s\n", manifest.GeneratedAt); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Schema version: %d\n", manifest.SchemaVersion); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Inventories: %d\n", len(manifest.Inventories)); err != nil {
		return err
	}

	for _, inventory := range manifest.Inventories {
		if _, err := fmt.Fprintf(w, "  %s: generated=%s version=%s db_driver=%s split_global_cell=%t\n", inventory.Source, inventory.GeneratedAt, inventory.Version, inventory.DatabaseDriver, inventory.SplitGlobalCell); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "Database roles: %d\n", len(manifest.DatabaseRoles)); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Instances: %d\n", len(manifest.Instances)); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Required paths: %d\n", len(manifest.RequiredPaths)); err != nil {
		return err
	}

	for _, path := range manifest.RequiredPaths {
		status := "missing"
		if !path.Enabled {
			status = "disabled"
		} else if path.Readable {
			status = "readable"
		} else if path.Exists {
			status = "unreadable"
		}

		if _, err := fmt.Fprintf(w, "  %s/%s: %s [%s] source=%s inventory=%s\n", path.Category, path.ID, backupDash(path.Path), status, path.Source, path.InventorySource); err != nil {
			return err
		}
	}

	if len(manifest.Warnings) > 0 {
		if _, err := fmt.Fprintln(w, "Warnings:"); err != nil {
			return err
		}

		for _, warning := range manifest.Warnings {
			if _, err := fmt.Fprintf(w, "  - %s: %s\n", backupDash(warning.InventorySource), warning.Message); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeBackupExpectedTopologyText(w io.Writer, source, profile string, expected backupExpectedTopology) error {
	if _, err := fmt.Fprintf(w, "Expected backup topology: %s profile=%s\n", source, profile); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Schema version: %d\n", expected.SchemaVersion); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Database roles: %d\n", len(expected.DatabaseRoles)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Instances: %d\n", len(expected.Instances)); err != nil {
		return err
	}
	for _, instance := range expected.Instances {
		if _, err := fmt.Fprintf(w, "  %s: %s\n", instance.Service, backupDash(instance.InstanceID)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Paths: %d\n", len(expected.Paths)); err != nil {
		return err
	}
	for _, path := range expected.Paths {
		if _, err := fmt.Fprintf(w, "  %s/%s: %s\n", path.Category, path.ID, backupDash(path.Path)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Required categories: %s\n", strings.Join(expected.RequireCategories, ", ")); err != nil {
		return err
	}

	return nil
}

func writeBackupManifestVerificationText(w io.Writer, result backupManifestVerification) error {
	if _, err := fmt.Fprintf(w, "Backup manifest verification: %s\n", result.Status); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Checked at: %s\n", result.CheckedAt); err != nil {
		return err
	}

	if result.ManifestGeneratedAt != "" {
		if _, err := fmt.Fprintf(w, "Manifest generated: %s\n", result.ManifestGeneratedAt); err != nil {
			return err
		}
	}

	if result.ExpectationSource != "" {
		if _, err := fmt.Fprintf(w, "Expected topology: %s\n", result.ExpectationSource); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "Summary: inventories=%d database_roles=%d instances=%d required_paths=%d\n", result.Summary.Inventories, result.Summary.DatabaseRoles, result.Summary.Instances, result.Summary.RequiredPaths); err != nil {
		return err
	}

	if len(result.StorageReports) > 0 {
		if _, err := fmt.Fprintf(w, "Storage reports: reports=%d verified=%d required_paths=%d", result.Summary.StorageReports, result.Summary.StorageReportsVerified, result.Summary.StoragePathsRequired); err != nil {
			return err
		}
		if result.StorageReportMaxAge != "" {
			if _, err := fmt.Fprintf(w, " max_age=%s", result.StorageReportMaxAge); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
		for _, report := range result.StorageReports {
			if _, err := fmt.Fprintf(w, "  %s: %s path=%s checked_at=%s warnings=%d errors=%d\n", report.Surface, report.Status, backupDash(report.Path), backupDash(report.CheckedAt), report.Warnings, report.Errors); err != nil {
				return err
			}
		}
	}

	if result.ExpectationSource != "" {
		if _, err := fmt.Fprintf(w, "Expected: inventory_sources=%d database_roles=%d instances=%d paths=%d required_categories=%d\n", result.Summary.ExpectedInventorySources, result.Summary.ExpectedDatabaseRoles, result.Summary.ExpectedInstances, result.Summary.ExpectedPaths, result.Summary.ExpectedRequiredCategories); err != nil {
			return err
		}
	}

	if len(result.Errors) > 0 {
		if _, err := fmt.Fprintln(w, "Errors:"); err != nil {
			return err
		}

		for _, finding := range result.Errors {
			if err := writeBackupFindingText(w, finding); err != nil {
				return err
			}
		}
	}

	if len(result.Warnings) > 0 {
		if _, err := fmt.Fprintln(w, "Warnings:"); err != nil {
			return err
		}

		for _, finding := range result.Warnings {
			if err := writeBackupFindingText(w, finding); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeBackupRestoreValidationText(w io.Writer, validation backupRestoreValidation) error {
	if _, err := fmt.Fprintf(w, "Restore validation: %s\n", validation.Status); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Generated at: %s\n", validation.GeneratedAt); err != nil {
		return err
	}
	if validation.Deployment != "" {
		if _, err := fmt.Fprintf(w, "Deployment: %s\n", validation.Deployment); err != nil {
			return err
		}
	}
	if validation.Profile != "" {
		if _, err := fmt.Fprintf(w, "Profile: %s\n", validation.Profile); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Manifest: %s\n", validation.Manifest); err != nil {
		return err
	}
	if validation.Expect != "" {
		if _, err := fmt.Fprintf(w, "Expected topology: %s\n", validation.Expect); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Verification: %s storage_reports=%d storage_verified=%d required_storage_paths=%d\n",
		validation.Verification.Status,
		validation.Verification.Summary.StorageReports,
		validation.Verification.Summary.StorageReportsVerified,
		validation.Verification.Summary.StoragePathsRequired,
	); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Smoke run: id=%s status=%s passed=%t\n", validation.SmokeRun.RunID, backupDash(validation.SmokeRun.Status), validation.SmokeRun.Passed); err != nil {
		return err
	}

	return nil
}

func writeBackupFindingText(w io.Writer, finding backupManifestFinding) error {
	context := finding.InventorySource
	if finding.PathID != "" {
		context = strings.TrimSpace(context + " " + finding.PathID)
	}

	if finding.Path != "" {
		context = strings.TrimSpace(context + " " + finding.Path)
	}

	if context == "" {
		_, err := fmt.Fprintf(w, "  - %s: %s\n", finding.ID, finding.Message)
		return err
	}

	_, err := fmt.Fprintf(w, "  - %s: %s (%s)\n", finding.ID, finding.Message, context)
	return err
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
