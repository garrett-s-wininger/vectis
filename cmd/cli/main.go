package main

import (
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"time"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/retention"

	_ "vectis/internal/dbdrivers"
)

func newAPIRequest(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, config.PublicAPIBaseURL()+path, body)
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
	runListJobID                string
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
	jobSourceCreateNamespace    string
	jobSourceCreateRef          string
	jobSourceUpdateRef          string
	jobSourceShowVersion        int
	jobSourceDefinitionVersion  int
	sourceListNamespace         string
	sourceListQuiet             bool
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
	sourceRegisterCanonicalURL  string
	sourceRegisterDefaultRef    string
	sourceRegisterCredentialRef string
	sourceRegisterDisabled      bool
	sourceUpdateSourceKind      string
	sourceUpdateCheckoutPath    string
	sourceUpdateCheckoutMode    string
	sourceUpdateAuthoringMode   string
	sourceUpdateCanonicalURL    string
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
	sourceTreeRecursive         bool
	sourceTreeQuiet             bool
	sourceDefinitionsRef        string
	sourceDefinitionsPath       string
	sourceDefinitionsLimit      int
	sourceDefinitionsQuiet      bool
	sourceResolveRef            string
	sourceImportRef             string
	sourceImportPath            string
	sourceImportLimit           int
	sourceImportDryRun          bool
	sourceImportUpdateExisting  bool
	sourceJobsRef               string
	sourceJobsPath              string
	sourceJobsLimit             int
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

var rootCmd = &cobra.Command{
	Use:   "vectis-cli",
	Short: "Command line interface for Vectis",
	Long: `Vectis CLI provides a single entry point for jobs, runs, and user management.

Commands are grouped around the thing you want to work with:
  actions    resolve and inspect action descriptors
  jobs       create, show, trigger, run, edit, and delete job definitions
  sources    register source repositories and trigger source-defined jobs
  runs       show run status, list tasks/artifacts or run history, cancel, fail, or retry runs
  cells      inspect execution cell routing and catalog state
  logs       stream run logs or follow future runs for a job
  secrets    manage job secret stores
  auth       log in, log out, and manage API tokens`,
	Example: `  vectis-cli jobs create build.json
  vectis-cli jobs trigger build-main --follow
  vectis-cli sources trigger vectis build-main --ref main --follow
  vectis-cli jobs list --format json
  vectis-cli runs list build-main
  vectis-cli runs show run-123
  vectis-cli runs artifacts list run-123
  vectis-cli secrets encryptedfs put encryptedfs://team/npm-token --root /var/lib/vectis/secrets --key-file /etc/vectis/secrets.key
  vectis-cli auth login
  vectis-cli health check --strict`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return validateOutputFormat(cliOutputFormat)
	},
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
	configureJobDeleteFlags(deleteCmd)
	configureJobListFlags(listCmd)
	configureJobSourceCreateFlags(jobSourceCreateCmd)
	configureJobSourceUpdateFlags(jobSourceUpdateCmd)
	configureJobSourceShowFlags(jobSourceShowCmd)
	configureJobSourceDefinitionFlags(jobSourceDefinitionCmd)
	jobSourceCmd.AddCommand(jobSourceCreateCmd, jobSourceUpdateCmd, jobSourceShowCmd, jobSourceDefinitionCmd)
	jobsCmd.AddCommand(listCmd, getCmd, createCmd, editCmd, deleteCmd, jobSourceCmd, triggerCmd, runCmd)
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
	configureSourcesImportFlags(sourcesImportCmd)
	configureSourcesJobsFlags(sourcesJobsCmd)
	configureSourcesShowFlags(sourcesShowCmd)
	configureSourcesWriteFlags(sourcesWriteCmd)
	configureSourcesRunsFlags(sourcesRunsCmd)
	configureLogFilterFlags(sourcesLogsCmd)
	configureSourcesLogsFlags(sourcesLogsCmd)
	configureSourcesTriggerFlags(sourcesTriggerCmd)
	sourcesCmd.AddCommand(sourcesListCmd, sourcesSchedulesCmd, sourcesOverrideCmd, sourcesClearOverrideCmd, sourcesEnableScheduleCmd, sourcesDisableScheduleCmd, sourcesDeleteScheduleCmd, sourcesRegisterCmd, sourcesGetCmd, sourcesUpdateCmd, sourcesDeleteCmd, sourcesSyncCmd, sourcesStatusCmd, sourcesBranchesCmd, sourcesTreeCmd, sourcesDefinitionsCmd, sourcesResolveCmd, sourcesImportCmd, sourcesJobsCmd, sourcesShowCmd, sourcesWriteCmd, sourcesRunsCmd, sourcesLogsCmd, sourcesTriggerCmd)
	rootCmd.AddCommand(sourcesCmd)

	configureRunListFlags(runListCmd)
	configureRunTasksFlags(runTasksCmd)
	configureRunArtifactsListFlags(runArtifactsListCmd)
	configureRunArtifactsDownloadFlags(runArtifactsDownloadCmd)
	configureRunReplayFlags(runReplayCmd)
	configureForceFailFlags(forceFailCmd)
	configureRepairMarkFlags(repairMarkSucceededCmd)
	configureRepairMarkFlags(repairMarkFailedCmd)
	configureRepairMarkFlags(repairMarkCancelledCmd)
	configureRepairMarkFlags(repairMarkAbandonedCmd)
	runRepairCmd.AddCommand(repairMarkSucceededCmd, repairMarkFailedCmd, repairMarkCancelledCmd, repairMarkAbandonedCmd, repairMarkQueuedCmd)
	runArtifactsCmd.AddCommand(runArtifactsListCmd, runArtifactsDownloadCmd)
	runsCmd.AddCommand(runListCmd, runGetCmd, runTasksCmd, runArtifactsCmd, runPayloadCmd, runReplayCmd, runCancelCmd, runRepairCmd, forceFailCmd, forceRequeueCmd)
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

	defaultRetention := retention.DefaultPolicy()
	retentionCleanupCmd.Flags().BoolVar(&retentionYes, "yes", false, "Confirm deletion of retention-eligible records")
	retentionCleanupCmd.Flags().BoolVar(&retentionDryRun, "dry-run", false, "Print the records that would be deleted")
	retentionCleanupCmd.Flags().DurationVar(&retentionRunAge, "terminal-run-age", defaultRetention.TerminalRuns, "Delete terminal runs older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionDefAge, "job-definition-age", defaultRetention.JobDefinitions, "Delete unreferenced ephemeral job definitions older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionIdemAge, "idempotency-age", defaultRetention.IdempotencyKeys, "Delete idempotency keys older than this duration (0 disables)")
	retentionCleanupCmd.Flags().DurationVar(&retentionAuditAge, "audit-age", defaultRetention.AuditLog, "Delete audit log rows older than this duration (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionLogDir, "log-storage-dir", "", "Optional durable run log directory to prune for deleted terminal runs")
	retentionCleanupCmd.Flags().DurationVar(&retentionArtifactAge, "artifact-blob-age", defaultRetention.ArtifactBlobs, "Delete unreferenced artifact blobs older than this duration when --artifact-storage-dir is set (0 disables)")
	retentionCleanupCmd.Flags().StringVar(&retentionArtifactDir, "artifact-storage-dir", "", "Optional durable artifact storage directory to prune unreferenced blobs")
	retentionCmd.AddCommand(retentionCleanupCmd)
	rootCmd.AddCommand(retentionCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		runCLIError(err)
	}
}
