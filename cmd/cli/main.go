package main

import (
	"context"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"time"
	"vectis/internal/cli"
	"vectis/internal/config"

	_ "vectis/internal/dbdrivers"
)

func newAPIRequest(method, path string, body io.Reader) (*http.Request, error) {
	return newAPIRequestWithContext(context.Background(), method, path, body)
}

func newAPIRequestWithContext(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, config.PublicAPIBaseURL()+path, body)
	if err != nil {
		return nil, err
	}

	if token := effectiveToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	return req, nil
}

var apiHTTPClient = &http.Client{Timeout: 30 * time.Second}

func doAPIRequest(req *http.Request) (*http.Response, error) {
	return apiHTTPClient.Do(req)
}

func doAPIStreamRequest(req *http.Request) (*http.Response, error) {
	client := *apiHTTPClient
	client.Timeout = 0
	return client.Do(req)
}

func closeIgnoringError(closer interface{ Close() error }) {
	_ = closer.Close()
}

const (
	cliGroupWorkflows  = "workflows"
	cliGroupAccess     = "access"
	cliGroupOperations = "operations"
)

func showCommandHelp(cmd *cobra.Command, args []string) {
	_ = cmd.Help()
}

var (
	podmanNetwork               string
	podmanProfile               string
	podmanKubeSpec              string
	podmanGrafanaSpec           string
	podmanRenderOut             string
	resetYes                    bool
	resetDryRun                 bool
	retentionYes                bool
	retentionDryRun             bool
	retentionRunAge             time.Duration
	retentionDefAge             time.Duration
	retentionIdemAge            time.Duration
	retentionAuditAge           time.Duration
	retentionLogDir             string
	retentionArtifactAge        time.Duration
	retentionArtifactDir        string
	retentionBackupManifest     string
	retentionBackupExpect       string
	retentionBackupMaxAge       time.Duration
	retentionHoldRunID          string
	retentionHoldAuditSince     string
	retentionHoldAuditUntil     string
	retentionHoldReason         string
	retentionHoldOwner          string
	retentionHoldExternalRef    string
	retentionHoldCreatedBy      string
	retentionHoldExpiresAt      string
	retentionHoldListScope      string
	retentionHoldListRunID      string
	retentionHoldListAll        bool
	retentionHoldReleasedBy     string
	retentionHoldReleaseReason  string
	auditListEventType          string
	auditListActorID            int64
	auditListTargetID           int64
	auditListCorrelationID      string
	auditListSince              string
	auditListUntil              string
	auditListCursor             string
	auditListLimit              int
	auditExportEventType        string
	auditExportActorID          int64
	auditExportTargetID         int64
	auditExportCorrelationID    string
	auditExportSince            string
	auditExportUntil            string
	auditExportLimit            int
	auditExportOutputPath       string
	runListJobID                string
	runListRepositoryID         string
	runListLimit                int
	runListCursor               int
	runListCellID               string
	runTasksLimit               int
	runTasksCursor              int
	runArtifactsLimit           int
	runArtifactsCursor          int
	runArtifactsTaskID          string
	runArtifactsAttemptID       string
	runArtifactsExecID          string
	runArtifactOutput           string
	runReplayCellID             string
	runReplayIdemKey            string
	triggerIdemKey              string
	triggerCellIDs              []string
	runIdemKey                  string
	runCellID                   string
	sourceListNamespace         string
	sourceListQuiet             bool
	sourceListStaleOnly         bool
	sourceSchedulesNamespace    string
	sourceSchedulesQuiet        bool
	sourceSchedulesOverrideOnly bool
	sourceSchedulesStaleOnly    bool
	sourceOverrideRef           string
	sourceOverridePath          string
	sourceOverrideReason        string
	sourceRegisterNamespace     string
	sourceRegisterCheckoutMode  string
	sourceRegisterAuthoringMode string
	sourceRegisterCacheMode     string
	sourceRegisterCanonicalURL  string
	sourceRegisterFallbackURLs  []string
	sourceRegisterWarmRefspecs  []string
	sourceRegisterDefaultRef    string
	sourceRegisterCredentialRef string
	sourceRegisterDisabled      bool
	sourceUpdateSourceKind      string
	sourceUpdateCheckoutPath    string
	sourceUpdateCheckoutMode    string
	sourceUpdateAuthoringMode   string
	sourceUpdateCacheMode       string
	sourceUpdateCanonicalURL    string
	sourceUpdateFallbackURLs    []string
	sourceUpdateWarmRefspecs    []string
	sourceUpdateDefaultRef      string
	sourceUpdateCredentialRef   string
	sourceUpdateEnable          bool
	sourceUpdateDisable         bool
	sourceDeleteYes             bool
	sourceDeleteScheduleYes     bool
	sourceBranchesPrefix        string
	sourceBranchesLimit         int
	sourceBranchesQuiet         bool
	sourceTreeRef               string
	sourceTreePath              string
	sourceTreeLimit             int
	sourceTreeCursor            string
	sourceTreeRecursive         bool
	sourceTreeQuiet             bool
	sourceDefinitionsRef        string
	sourceDefinitionsPath       string
	sourceDefinitionsLimit      int
	sourceDefinitionsCursor     string
	sourceDefinitionsQuiet      bool
	sourceResolveRef            string
	sourceJobsRef               string
	sourceJobsPath              string
	sourceJobsLimit             int
	sourceJobsCursor            string
	sourceJobsQuiet             bool
	sourceShowRef               string
	sourceShowPath              string
	sourceWriteRef              string
	sourceWriteBranch           string
	sourceWritePath             string
	sourceWriteMessage          string
	sourceWriteExpectedHead     string
	sourceWriteQuiet            bool
	sourceRunsLimit             int
	sourceRunsCursor            int
	sourceRunsCellID            string
	sourceTriggerRef            string
	sourceTriggerPath           string
	sourceTriggerCellID         string
	sourceTriggerIdemKey        string
)

var (
	retentionBackupStorageReports  []string
	retentionBackupStorageMaxAge   time.Duration
	retentionAuditExport           string
	retentionAuditExportMaxAge     time.Duration
	retentionHoldReview            string
	retentionHoldReviewOutput      string
	retentionHoldReviewReviewedBy  string
	retentionHoldReviewReason      string
	retentionHoldReviewExternalRef string
	retentionHoldReviewMaxAge      time.Duration
	retentionRequireBackupManifest bool
	retentionRequireAuditExport    bool
	retentionRequireHoldReview     bool
	retentionWaiver                string
)

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Command line interface for Vectis",
	Long: `Vectis CLI provides a single entry point for jobs, runs, and user management.

Commands are grouped around the thing you want to work with:
  actions    resolve and inspect action descriptors
  jobs       create, show, trigger, run, edit, and delete job definitions
  sources    register, sync, browse, and administer source repositories
  runs       show run status, list tasks/artifacts or run history, cancel, fail, or retry runs
  cells      inspect execution cell routing and catalog state
  logs       stream run logs or follow future runs for a job
  audit      review API audit events
  storage    verify durable file storage integrity
  secrets    manage job secret stores
  auth       log in, log out, and manage API tokens`,
	Example: `  vectis-cli jobs create build.json --repository vectis --branch main
  vectis-cli jobs trigger build-main --repository vectis --ref main --follow
  vectis-cli jobs list --repository vectis --format json
  vectis-cli runs list build-main --repository vectis
  vectis-cli runs show run-123
  vectis-cli runs definition run-123
  vectis-cli runs artifacts list run-123
  vectis-cli secrets encryptedfs put encryptedfs://team/npm-token --root /var/lib/vectis/secrets --key-file /etc/vectis/secrets.key
  vectis-cli auth login
  vectis-cli health check --strict`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return validateOutputFormat(cliOutputFormat)
	},
}

type retentionCleanupDefaults struct {
	Policy              config.RetentionCleanupPolicyDefaults
	BackupMaxAge        time.Duration
	BackupStorageMaxAge time.Duration
	AuditExportMaxAge   time.Duration
	HoldReviewMaxAge    time.Duration
	RequireBackup       bool
	RequireAuditExport  bool
	RequireHoldReview   bool
}

func retentionCleanupDefaultsFromConfig() retentionCleanupDefaults {
	policy := config.RetentionCleanupPolicy()
	return retentionCleanupDefaults{
		Policy:              policy,
		BackupMaxAge:        config.RetentionCleanupBackupMaxAge(),
		BackupStorageMaxAge: config.RetentionCleanupBackupStorageMaxAge(),
		AuditExportMaxAge:   config.RetentionCleanupAuditExportMaxAge(),
		HoldReviewMaxAge:    config.RetentionCleanupHoldReviewMaxAge(),
		RequireBackup:       config.RetentionCleanupRequireBackupManifest(),
		RequireAuditExport:  config.RetentionCleanupRequireAuditExport(),
		RequireHoldReview:   config.RetentionCleanupRequireHoldReview(),
	}
}

func init() {
	cli.ConfigureVersion(rootCmd)
	rootCmd.PersistentFlags().StringVar(&cliOutputFormat, "format", outputText, "Output format: text or json")

	rootCmd.AddGroup(
		&cobra.Group{ID: cliGroupWorkflows, Title: "Workflows"},
		&cobra.Group{ID: cliGroupAccess, Title: "Access"},
		&cobra.Group{ID: cliGroupOperations, Title: "Operations"},
	)

	configureJobTriggerFlags(triggerCmd)
	configureJobRunFlags(runCmd)
	configureJobShowFlags(getCmd)
	configureJobCreateFlags(createCmd)
	configureJobEditFlags(editCmd)
	configureJobDeleteFlags(deleteCmd)
	configureJobListFlags(listCmd)
	jobsCmd.AddCommand(listCmd, getCmd, createCmd, editCmd, deleteCmd, triggerCmd, runCmd)
	rootCmd.AddCommand(jobsCmd)

	configureActionListFlags(actionsListCmd)
	configureActionResolveFlags(actionsResolveCmd)
	actionsCmd.AddCommand(actionsListCmd, actionsResolveCmd)
	rootCmd.AddCommand(actionsCmd)

	configureSourcesListFlags(sourcesListCmd)
	configureSourcesSchedulesFlags(sourcesSchedulesCmd)
	configureSourcesOverrideFlags(sourcesOverrideCmd)
	configureSourcesRegisterFlags(sourcesRegisterCmd)
	configureSourcesUpdateFlags(sourcesUpdateCmd)
	configureSourcesDeleteFlags(sourcesDeleteCmd)
	configureSourcesDeleteScheduleFlags(sourcesDeleteScheduleCmd)
	configureSourcesBranchesFlags(sourcesBranchesCmd)
	configureSourcesTreeFlags(sourcesTreeCmd)
	configureSourcesDefinitionsFlags(sourcesDefinitionsCmd)
	configureSourcesResolveFlags(sourcesResolveCmd)
	configureSourcesJobsFlags(sourcesJobsCmd)
	configureSourcesShowFlags(sourcesShowCmd)
	configureSourcesWriteFlags(sourcesWriteCmd)
	configureSourcesRunsFlags(sourcesRunsCmd)
	configureLogFilterFlags(sourcesLogsCmd)
	configureSourcesLogsFlags(sourcesLogsCmd)
	configureSourcesTriggerFlags(sourcesTriggerCmd)
	sourcesCmd.AddCommand(sourcesOverviewCmd, sourcesListCmd, sourcesSchedulesCmd, sourcesOverrideCmd, sourcesClearOverrideCmd, sourcesEnableScheduleCmd, sourcesDisableScheduleCmd, sourcesDeleteScheduleCmd, sourcesRegisterCmd, sourcesGetCmd, sourcesUpdateCmd, sourcesDeleteCmd, sourcesSyncCmd, sourcesStatusCmd, sourcesBranchesCmd, sourcesTreeCmd, sourcesDefinitionsCmd, sourcesResolveCmd, sourcesJobsCmd, sourcesShowCmd, sourcesWriteCmd, sourcesRunsCmd, sourcesLogsCmd, sourcesTriggerCmd)
	rootCmd.AddCommand(sourcesCmd)

	configureRunListFlags(runListCmd)
	configureRunTasksFlags(runTasksCmd)
	configureRunArtifactsListFlags(runArtifactsListCmd)
	configureRunArtifactsDownloadFlags(runArtifactsDownloadCmd)
	configureRunReplayFlags(runReplayCmd)
	runDefinitionCmd.Flags().Bool("raw", false, "Print definition JSON without reformatting")
	configureForceFailFlags(forceFailCmd)
	configureRepairMarkFlags(repairMarkSucceededCmd)
	configureRepairMarkFlags(repairMarkFailedCmd)
	configureRepairMarkFlags(repairMarkCancelledCmd)
	configureRepairMarkFlags(repairMarkAbandonedCmd)
	runRepairCmd.AddCommand(repairMarkSucceededCmd, repairMarkFailedCmd, repairMarkCancelledCmd, repairMarkAbandonedCmd, repairMarkQueuedCmd)
	runArtifactsCmd.AddCommand(runArtifactsListCmd, runArtifactsDownloadCmd)
	runsCmd.AddCommand(runListCmd, runGetCmd, runDefinitionCmd, runTasksCmd, runArtifactsCmd, runPayloadCmd, runReplayCmd, runCancelCmd, runRepairCmd, forceFailCmd, forceRequeueCmd)
	rootCmd.AddCommand(runsCmd)

	cellsCmd.AddCommand(cellsStatusCmd)
	rootCmd.AddCommand(cellsCmd)

	configureLogFilterFlags(logsRunCmd)
	configureLogFilterFlags(logsJobCmd)
	configureLogsJobFlags(logsJobCmd)
	logsCmd.AddCommand(logsRunCmd, logsJobCmd)
	rootCmd.AddCommand(logsCmd)

	configureSecretEncryptedFSPutFlags(secretsEncryptedFSPutCmd)
	secretsCmd.GroupID = cliGroupOperations
	secretsCmd.AddCommand(secretsEncryptedFSCmd)
	secretsEncryptedFSCmd.AddCommand(secretsEncryptedFSPutCmd)
	rootCmd.AddCommand(secretsCmd)

	configureLoginFlags(loginCmd)
	authCmd.AddCommand(loginCmd, logoutCmd)
	tokenCmd.AddCommand(tokenListCmd)
	configureTokenCreateFlags(tokenCreateCmd)
	tokenCmd.AddCommand(tokenCreateCmd, tokenDeleteCmd)
	authCmd.AddCommand(tokenCmd)
	rootCmd.AddCommand(authCmd)

	namespaceCreateCmd.Flags().Int64("parent-id", 0, "Parent namespace ID (default: root)")
	namespaceCmd.GroupID = cliGroupAccess
	namespaceCmd.Run = showCommandHelp
	namespaceCmd.AddCommand(namespaceListCmd, namespaceGetCmd, namespaceCreateCmd, namespaceDeleteCmd)
	rootCmd.AddCommand(namespaceCmd)

	userCreateCmd.Flags().String("password", "", "Initial password (default: generated by API)")
	userChangePasswordCmd.Flags().Int64("user-id", 0, "Target user ID (default: current user)")
	userChangePasswordCmd.Flags().String("current-password", "", "Current password for self-service password changes")
	userChangePasswordCmd.Flags().String("new-password", "", "New password")
	userCmd.GroupID = cliGroupAccess
	userCmd.Run = showCommandHelp
	userCmd.AddCommand(userListCmd, userGetCmd, userCreateCmd, userEnableCmd, userDisableCmd, userDeleteCmd, userChangePasswordCmd)
	rootCmd.AddCommand(userCmd)

	roleBindingCmd.GroupID = cliGroupAccess
	roleBindingCmd.Run = showCommandHelp
	roleBindingCmd.AddCommand(roleBindingListCmd, roleBindingCreateCmd, roleBindingDeleteCmd)
	rootCmd.AddCommand(roleBindingCmd)

	configureDoctorFlags(doctorCmd)
	healthCmd.AddCommand(doctorCmd)
	rootCmd.AddCommand(healthCmd)

	databaseCmd.AddCommand(migrateCmd)
	rootCmd.AddCommand(databaseCmd)

	backupExpectPodmanCmd.Flags().StringVar(&backupExpectPodmanProfile, "profile", podmanProfileSimple, "Podman deployment profile: simple or ha")
	backupExpectLinuxCmd.Flags().StringVar(&backupExpectLinuxManifestPath, "manifest", backupExpectLinuxManifestPath, "Path to the Linux deploy services TOML manifest")
	backupExpectCmd.AddCommand(backupExpectPodmanCmd, backupExpectLinuxCmd)
	backupVerifyCmd.Flags().StringVar(&backupVerifyExpectPath, "expect", "", "Expected backup topology JSON file")
	backupVerifyCmd.Flags().StringArrayVar(&backupVerifyStorageReportPaths, "storage-report", nil, "Storage verification report JSON from vectis-cli storage verify --format json (repeatable)")
	backupVerifyCmd.Flags().DurationVar(&backupVerifyStorageMaxAge, "storage-max-age", 0, "Maximum accepted storage verification report age (0 disables)")
	backupRestoreValidationCmd.Flags().StringVar(&backupRestoreValidationExpectPath, "expect", "", "Expected backup topology JSON file")
	backupRestoreValidationCmd.Flags().StringArrayVar(&backupRestoreValidationStorageReportPaths, "storage-report", nil, "Storage verification report JSON from vectis-cli storage verify --format json (repeatable)")
	backupRestoreValidationCmd.Flags().DurationVar(&backupRestoreValidationStorageMaxAge, "storage-max-age", 0, "Maximum accepted storage verification report age (0 disables)")
	backupRestoreValidationCmd.Flags().StringVar(&backupRestoreValidationSmokeRunID, "smoke-run", "", "Run ID from the restored deployment smoke run")
	backupRestoreValidationCmd.Flags().StringVar(&backupRestoreValidationDeployment, "deployment", "", "Deployment label to include in restore validation output")
	backupRestoreValidationCmd.Flags().StringVar(&backupRestoreValidationProfile, "profile", "", "Deployment profile label to include in restore validation output")
	backupCmd.AddCommand(backupInventoryCmd, backupManifestCmd, backupExpectCmd, backupVerifyCmd, backupRestoreValidationCmd)
	rootCmd.AddCommand(backupCmd)

	configureStorageVerifyDirFlag(storageVerifyArtifactCmd, "Artifact storage directory to verify")
	configureStorageVerifyDirFlag(storageVerifyLogsCmd, "Durable run log storage directory to verify")
	configureStorageVerifyDirFlag(storageVerifyQueueCmd, "Queue persistence directory to verify")
	configureStorageVerifyDirFlag(storageVerifyLogForwarderSpoolCmd, "Log-forwarder spool directory to verify")
	configureStorageVerifyDirFlag(storageVerifyWorkerLogSpoolCmd, "Worker pending log spool directory to verify")
	storageVerifyCmd.AddCommand(storageVerifyArtifactCmd, storageVerifyLogsCmd, storageVerifyQueueCmd, storageVerifyLogForwarderSpoolCmd, storageVerifyWorkerLogSpoolCmd)
	storageCmd.AddCommand(storageVerifyCmd)
	rootCmd.AddCommand(storageCmd)

	configureAuditListFlags(auditListCmd)
	configureAuditExportFlags(auditExportCmd)
	auditCmd.AddCommand(auditListCmd, auditExportCmd)
	rootCmd.AddCommand(auditCmd)

	deployPodmanCmd.PersistentFlags().StringVar(&podmanNetwork, "network", "pasta", "Podman network mode for play kube")
	deployPodmanCmd.PersistentFlags().StringVar(&podmanProfile, "profile", podmanProfileSimple, "Deployment profile: simple or ha")
	deployPodmanCmd.PersistentFlags().StringVar(&podmanKubeSpec, "kube-spec", defaultPodmanKubeSpec, "Path to the Podman kube spec template")
	deployPodmanCmd.PersistentFlags().StringVar(&podmanGrafanaSpec, "grafana-spec", defaultPodmanGrafanaSpec, "Path to generated Grafana ConfigMaps")
	deployPodmanInitCmd.Flags().Bool("rotate", false, "Regenerate deployment secrets")
	deployPodmanRenderCmd.Flags().Bool("rotate", false, "Regenerate deployment secrets before rendering")
	deployPodmanRenderCmd.Flags().StringVarP(&podmanRenderOut, "output", "o", "-", "Rendered manifest output path, or '-' for stdout")
	deployPodmanCmd.AddCommand(deployPodmanInitCmd)
	deployPodmanCmd.AddCommand(deployPodmanRenderCmd)
	deployPodmanCmd.AddCommand(deployPodmanUpCmd)
	deployPodmanCmd.AddCommand(deployPodmanDownCmd)
	deployPodmanCmd.AddCommand(deployPodmanStatusCmd)
	deployLinuxCmd.PersistentFlags().StringVar(&linuxManifestPath, "manifest", linuxManifestPath, "Path to the Linux deploy services TOML manifest")
	deployLinuxRenderCmd.Flags().StringVarP(&linuxRenderOut, "output", "o", linuxRenderOut, "Directory where rendered Linux artifacts are written")
	deployLinuxCmd.AddCommand(deployLinuxRenderCmd)
	deployCmd.AddCommand(deployPodmanCmd)
	deployCmd.AddCommand(deployLinuxCmd)
	rootCmd.AddCommand(deployCmd)

	configureResetFlags(resetCmd)
	localCmd.AddCommand(resetCmd)
	rootCmd.AddCommand(localCmd)

	defaultRetention := retentionCleanupDefaultsFromConfig()
	retentionCleanupCmd.Flags().BoolVar(&retentionYes, "yes", false, "Confirm deletion of retention-eligible records")
	retentionCleanupCmd.Flags().BoolVar(&retentionDryRun, "dry-run", false, "Print the records that would be deleted")
	retentionCleanupCmd.Flags().DurationVar(&retentionRunAge, "terminal-run-age", defaultRetention.Policy.TerminalRuns, "Delete terminal runs older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionDefAge, "job-definition-age", defaultRetention.Policy.JobDefinitions, "Delete unreferenced job definition snapshots older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionIdemAge, "idempotency-age", defaultRetention.Policy.IdempotencyKeys, "Delete idempotency keys older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionAuditAge, "audit-age", defaultRetention.Policy.AuditLog, "Delete audit log rows older than this duration (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionLogDir, "log-storage-dir", "", "Optional durable run log directory to prune for deleted terminal runs")
	retentionCleanupCmd.Flags().DurationVar(&retentionArtifactAge, "artifact-blob-age", defaultRetention.Policy.ArtifactBlobs, "Delete unreferenced artifact blobs older than this duration when --artifact-storage-dir is set (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionArtifactDir, "artifact-storage-dir", "", "Optional durable artifact storage directory to prune unreferenced blobs")
	retentionCleanupCmd.Flags().StringVar(&retentionBackupManifest, "backup-manifest", "", "Optional backup manifest JSON to verify before cleanup")
	retentionCleanupCmd.Flags().StringVar(&retentionBackupExpect, "backup-expect", "", "Optional expected topology JSON for backup manifest verification")
	retentionCleanupCmd.Flags().DurationVar(&retentionBackupMaxAge, "backup-max-age", defaultRetention.BackupMaxAge, "Maximum accepted backup manifest age before cleanup (0 disables)")
	retentionCleanupCmd.Flags().StringArrayVar(&retentionBackupStorageReports, "backup-storage-report", nil, "Optional storage verification report JSON to verify before cleanup (repeatable)")
	retentionCleanupCmd.Flags().DurationVar(&retentionBackupStorageMaxAge, "backup-storage-max-age", defaultRetention.BackupStorageMaxAge, "Maximum accepted storage verification report age before cleanup (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionAuditExport, "audit-export", "", "Optional audit export evidence JSON to verify before deleting audit rows")
	retentionCleanupCmd.Flags().DurationVar(&retentionAuditExportMaxAge, "audit-export-max-age", defaultRetention.AuditExportMaxAge, "Maximum accepted audit export evidence age before cleanup (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionHoldReview, "hold-review", "", "Optional active hold review evidence JSON to verify before cleanup")
	retentionCleanupCmd.Flags().DurationVar(&retentionHoldReviewMaxAge, "hold-review-max-age", defaultRetention.HoldReviewMaxAge, "Maximum accepted hold review evidence age before cleanup (0 disables)")
	retentionCleanupCmd.Flags().BoolVar(&retentionRequireBackupManifest, "require-backup-manifest", defaultRetention.RequireBackup, "Require --backup-manifest unless waived by --waiver")
	retentionCleanupCmd.Flags().BoolVar(&retentionRequireAuditExport, "require-audit-export", defaultRetention.RequireAuditExport, "Require --audit-export before deleting audit rows unless waived by --waiver")
	retentionCleanupCmd.Flags().BoolVar(&retentionRequireHoldReview, "require-hold-review", defaultRetention.RequireHoldReview, "Require --hold-review unless waived by --waiver")
	retentionCleanupCmd.Flags().StringVar(&retentionWaiver, "waiver", "", "Optional retention waiver JSON for required cleanup gates")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldRunID, "run", "", "Run ID to protect")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldAuditSince, "audit-since", "", "Start of audit_log range to protect (RFC3339 or YYYY-MM-DD)")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldAuditUntil, "audit-until", "", "End of audit_log range to protect (RFC3339 or YYYY-MM-DD)")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldReason, "reason", "", "Compliance or incident reason for the hold")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldOwner, "owner", "", "Accountable owner for the hold")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldExternalRef, "external-ref", "", "Optional ticket, case, or legal matter reference")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldCreatedBy, "created-by", "", "Operator creating the hold (default: VECTIS_OPERATOR, USER, or USERNAME)")
	retentionHoldCreateCmd.Flags().StringVar(&retentionHoldExpiresAt, "expires-at", "", "Optional RFC3339 expiry time for the hold")
	retentionHoldListCmd.Flags().StringVar(&retentionHoldListScope, "scope", "", "Only list holds for this scope: run or audit_range")
	retentionHoldListCmd.Flags().StringVar(&retentionHoldListRunID, "run", "", "Only list holds for this run ID")
	retentionHoldListCmd.Flags().BoolVar(&retentionHoldListAll, "all", false, "Include released and expired holds")
	retentionHoldReleaseCmd.Flags().StringVar(&retentionHoldReleaseReason, "reason", "", "Reason for releasing the hold")
	retentionHoldReleaseCmd.Flags().StringVar(&retentionHoldReleasedBy, "released-by", "", "Operator releasing the hold (default: VECTIS_OPERATOR, USER, or USERNAME)")
	retentionHoldReviewCmd.Flags().StringVarP(&retentionHoldReviewOutput, "output", "o", "-", "Hold review evidence JSON output path, or '-' for stdout")
	retentionHoldReviewCmd.Flags().StringVar(&retentionHoldReviewReviewedBy, "reviewed-by", "", "Operator reviewing active holds (default: VECTIS_OPERATOR, USER, or USERNAME)")
	retentionHoldReviewCmd.Flags().StringVar(&retentionHoldReviewReason, "reason", "", "Reason for the active hold review")
	retentionHoldReviewCmd.Flags().StringVar(&retentionHoldReviewExternalRef, "external-ref", "", "Optional ticket, case, or compliance reference")
	retentionHoldsCmd.AddCommand(retentionHoldCreateCmd, retentionHoldListCmd, retentionHoldReleaseCmd, retentionHoldReviewCmd)
	retentionCmd.AddCommand(retentionCleanupCmd)
	retentionCmd.AddCommand(retentionHoldsCmd)
	rootCmd.AddCommand(retentionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		runCLIError(err)
	}
}
