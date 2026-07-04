package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kubernetesdeploy "vectis/deploy/kubernetes"
)

func main() {
	var opts kubernetesdeploy.SmokeOptions
	var outputJSON bool
	seedSecret := kubernetesdeploy.DefaultSmokeSeedSecret

	flag.StringVar(&opts.Kubectl, "kubectl", "kubectl", "kubectl command path")
	flag.StringVar(&opts.Context, "context", kubernetesdeploy.DefaultSmokeContext, "Kubernetes context to use; empty uses kubectl's current context")
	flag.StringVar(&opts.Namespace, "namespace", kubernetesdeploy.DefaultNamespace, "Kubernetes namespace")
	flag.StringVar(&opts.JobPath, "job", kubernetesdeploy.DefaultSmokeJobPath, "Kubernetes smoke job definition path")
	flag.StringVar(&opts.WorkerCoreJobPath, "worker-core-job", kubernetesdeploy.DefaultSmokeWorkerCoreJobPath, "Kubernetes worker-core provider smoke job definition path")
	flag.StringVar(&opts.CancelJobPath, "cancel-job", kubernetesdeploy.DefaultSmokeCancelJobPath, "Kubernetes worker-control cancel smoke job definition path")
	flag.StringVar(&opts.ScaleJobPath, "scale-job", kubernetesdeploy.DefaultSmokeScaleJobPath, "Kubernetes worker scaling smoke job definition path")
	flag.StringVar(&opts.OrphanJobPath, "orphan-job", kubernetesdeploy.DefaultSmokeOrphanJobPath, "Kubernetes worker pod-loss orphan smoke job definition path")
	flag.StringVar(&opts.RepairJobPath, "repair-job", kubernetesdeploy.DefaultSmokeRepairJobPath, "Kubernetes explicit orphan repair smoke job definition path")
	flag.BoolVar(&opts.WorkerCoreOnly, "worker-core-only", false, "Run only the Kubernetes worker-core provider smoke")
	flag.BoolVar(&opts.CancelOnly, "cancel-only", false, "Run only the worker-control cancel smoke")
	flag.BoolVar(&opts.ScaleOnly, "scale-only", false, "Run only the worker scaling smoke")
	flag.BoolVar(&opts.OrphanOnly, "orphan-only", false, "Run only the worker pod-loss orphan smoke")
	flag.BoolVar(&opts.RepairOnly, "repair-only", false, "Run only the explicit orphan repair smoke")
	flag.BoolVar(&opts.GerritStreamOnly, "gerrit-stream-only", false, "Run only the Kubernetes Gerrit stream bridge smoke")
	flag.BoolVar(&opts.S3ArtifactOnly, "s3-artifact-only", false, "Run only the Kubernetes S3 artifact backend smoke")
	flag.BoolVar(&opts.KnoxSecretsOnly, "knox-secrets-only", false, "Run only the Kubernetes Knox secrets provider smoke")
	flag.BoolVar(&opts.LDAPAuthOnly, "ldap-auth-only", false, "Run only the Kubernetes LDAP auth provider smoke")
	flag.IntVar(&opts.ScaleWorkerReplicas, "scale-worker-replicas", kubernetesdeploy.DefaultSmokeScaleWorkerReplicas, "Worker deployment replica count used during the scaling smoke")
	flag.IntVar(&opts.ScaleMinWorkers, "scale-min-workers", kubernetesdeploy.DefaultSmokeScaleMinWorkers, "Minimum distinct worker lease owners required by the scaling smoke")
	flag.DurationVar(&opts.OrphanLeaseTTL, "orphan-lease-ttl", kubernetesdeploy.DefaultSmokeOrphanLeaseTTL, "Temporary worker execution lease TTL used during the orphan smoke")
	flag.DurationVar(&opts.OrphanStability, "orphan-stability", kubernetesdeploy.DefaultSmokeOrphanStability, "How long the orphan smoke requires the run to remain orphaned")
	flag.DurationVar(&opts.RepairLeaseTTL, "repair-lease-ttl", kubernetesdeploy.DefaultSmokeRepairLeaseTTL, "Temporary worker execution lease TTL used during the repair smoke")
	flag.DurationVar(&opts.RepairReadyAfter, "repair-ready-after", kubernetesdeploy.DefaultSmokeRepairReadyAfter, "Delay after repair-job submission before its retry may take the success path")
	flag.StringVar(&opts.CLIImage, "cli-image", kubernetesdeploy.DefaultSmokeCLIImage, "vectis-cli image used to seed smoke secrets")
	flag.StringVar(&opts.WorkerCoreImage, "worker-core-image", kubernetesdeploy.DefaultSmokeWorkerCoreImage, "vectis-worker-core-kubernetes image used during the worker-core provider smoke")
	flag.StringVar(&opts.WorkerCoreTaskImage, "worker-core-task-image", kubernetesdeploy.DefaultSmokeWorkerCoreTaskImage, "Task container image used by the Kubernetes worker-core provider smoke")
	flag.StringVar(&opts.GerritImage, "gerrit-image", kubernetesdeploy.DefaultSmokeGerritImage, "Gerrit image used by the Kubernetes Gerrit stream smoke")
	flag.IntVar(&opts.GerritHTTPPort, "gerrit-http-local-port", kubernetesdeploy.DefaultSmokeGerritHTTPPort, "Local port used for the Gerrit HTTP port-forward")
	flag.IntVar(&opts.GerritSSHLocalPort, "gerrit-ssh-local-port", kubernetesdeploy.DefaultSmokeGerritSSHLocalPort, "Local port used for the Gerrit SSH port-forward")
	flag.StringVar(&opts.GerritClusterURL, "gerrit-cluster-url", kubernetesdeploy.DefaultSmokeGerritClusterURL, "In-cluster Gerrit base URL used by Vectis SCM triggers")
	flag.StringVar(&opts.GerritSSHHost, "gerrit-ssh-host", kubernetesdeploy.DefaultSmokeGerritSSHHost, "In-cluster Gerrit SSH host used by the stream bridge")
	flag.IntVar(&opts.GerritSSHPort, "gerrit-ssh-port", kubernetesdeploy.DefaultSmokeGerritSSHPort, "In-cluster Gerrit SSH port used by the stream bridge")
	flag.StringVar(&opts.GerritAccountID, "gerrit-account-id", kubernetesdeploy.DefaultSmokeGerritAccountID, "Development Gerrit account id used by the stream smoke")
	flag.StringVar(&opts.GerritUsername, "gerrit-username", kubernetesdeploy.DefaultSmokeGerritUsername, "Gerrit username used by the stream smoke")
	flag.StringVar(&opts.GerritProject, "gerrit-project", "", "Gerrit project name; generated when empty")
	flag.StringVar(&opts.GerritProjectPrefix, "gerrit-project-prefix", kubernetesdeploy.DefaultSmokeGerritProjectPrefix, "Generated Gerrit project prefix")
	flag.StringVar(&opts.GerritGitBin, "gerrit-git", kubernetesdeploy.DefaultSmokeGerritGitBin, "git executable used to push the Gerrit smoke change")
	flag.BoolVar(&opts.GerritKeepFixture, "gerrit-keep-fixture", false, "Keep the Gerrit fixture deployment and service after the Gerrit stream smoke")
	flag.StringVar(&opts.S3Image, "s3-image", kubernetesdeploy.DefaultSmokeS3Image, "SeaweedFS image used by the Kubernetes S3 artifact smoke")
	flag.IntVar(&opts.S3LocalPort, "s3-local-port", kubernetesdeploy.DefaultSmokeS3LocalPort, "Local port used for the S3 artifact fixture port-forward")
	flag.StringVar(&opts.S3ClusterEndpoint, "s3-cluster-endpoint", kubernetesdeploy.DefaultSmokeS3ClusterEndpoint, "In-cluster S3 endpoint used by vectis-artifact during the S3 smoke")
	flag.StringVar(&opts.S3Bucket, "s3-bucket", kubernetesdeploy.DefaultSmokeS3Bucket, "S3 bucket used by the Kubernetes S3 artifact smoke")
	flag.StringVar(&opts.S3Prefix, "s3-prefix", kubernetesdeploy.DefaultSmokeS3Prefix, "S3 object prefix used by the Kubernetes S3 artifact smoke")
	flag.StringVar(&opts.S3AccessKeyID, "s3-access-key-id", kubernetesdeploy.DefaultSmokeS3AccessKeyID, "S3 access key id used by the Kubernetes S3 artifact smoke")
	flag.StringVar(&opts.S3SecretAccessKey, "s3-secret-access-key", kubernetesdeploy.DefaultSmokeS3SecretAccessKey, "S3 secret access key used by the Kubernetes S3 artifact smoke")
	flag.StringVar(&opts.S3TempDir, "s3-temp-dir", kubernetesdeploy.DefaultSmokeS3TempDir, "Writable temp directory used by vectis-artifact while hashing S3 uploads")
	flag.BoolVar(&opts.S3KeepFixture, "s3-keep-fixture", false, "Keep the S3 artifact fixture deployment and service after the S3 smoke")
	flag.StringVar(&opts.KnoxImage, "knox-image", kubernetesdeploy.DefaultSmokeKnoxImage, "Knox smoke image used by the Kubernetes Knox secrets smoke")
	flag.IntVar(&opts.KnoxLocalPort, "knox-local-port", kubernetesdeploy.DefaultSmokeKnoxLocalPort, "Local port used for the Knox fixture port-forward")
	flag.StringVar(&opts.KnoxClusterURL, "knox-cluster-url", kubernetesdeploy.DefaultSmokeKnoxClusterURL, "In-cluster Knox URL used by vectis-secrets during the Knox smoke")
	flag.StringVar(&opts.KnoxCertMountPath, "knox-cert-mount-path", kubernetesdeploy.DefaultSmokeKnoxCertMountPath, "Mount path for Knox client and CA certificates on vectis-secrets")
	flag.StringVar(&opts.KnoxAuthToken, "knox-auth-token", kubernetesdeploy.DefaultSmokeKnoxAuthToken, "Knox Authorization header value used by the Kubernetes Knox secrets smoke")
	flag.StringVar(&opts.KnoxWrongAuthToken, "knox-wrong-auth-token", kubernetesdeploy.DefaultSmokeKnoxWrongAuthToken, "Wrong Knox token expected to be denied by the fixture")
	flag.StringVar(&opts.KnoxKeyID, "knox-key-id", kubernetesdeploy.DefaultSmokeKnoxKeyID, "Knox key id seeded by the fixture")
	flag.StringVar(&opts.KnoxRef, "knox-ref", kubernetesdeploy.DefaultSmokeKnoxRef, "Knox ref used by the Kubernetes Knox secrets smoke")
	flag.StringVar(&opts.KnoxMissingRef, "knox-missing-ref", kubernetesdeploy.DefaultSmokeKnoxMissingRef, "Missing Knox ref expected to return not found")
	flag.StringVar(&opts.KnoxExpectedData, "knox-expected-data", kubernetesdeploy.DefaultSmokeKnoxSecret, "Expected Knox secret data")
	flag.BoolVar(&opts.KnoxKeepFixture, "knox-keep-fixture", false, "Keep the Knox fixture deployment, service, and cert Secret after the Knox smoke")
	flag.StringVar(&opts.LDAPImage, "ldap-image", kubernetesdeploy.DefaultSmokeLDAPImage, "OpenLDAP image used by the Kubernetes LDAP auth smoke")
	flag.IntVar(&opts.LDAPLocalPort, "ldap-local-port", kubernetesdeploy.DefaultSmokeLDAPLocalPort, "Local port used for the LDAP fixture port-forward")
	flag.StringVar(&opts.LDAPClusterURL, "ldap-cluster-url", kubernetesdeploy.DefaultSmokeLDAPClusterURL, "In-cluster LDAP URL used by vectis-api during the LDAP auth smoke")
	flag.StringVar(&opts.LDAPBootstrapLDIF, "ldap-bootstrap-ldif", kubernetesdeploy.DefaultSmokeLDAPBootstrapLDIF, "Bootstrap LDIF mounted into the LDAP fixture")
	flag.StringVar(&opts.LDAPBaseDN, "ldap-base-dn", kubernetesdeploy.DefaultSmokeLDAPBaseDN, "LDAP base DN used for user search")
	flag.StringVar(&opts.LDAPBindDN, "ldap-bind-dn", kubernetesdeploy.DefaultSmokeLDAPBindDN, "LDAP service-account bind DN")
	flag.StringVar(&opts.LDAPBindPassword, "ldap-bind-password", kubernetesdeploy.DefaultSmokeLDAPBindPassword, "LDAP service-account bind password")
	flag.StringVar(&opts.LDAPUserFilter, "ldap-user-filter", kubernetesdeploy.DefaultSmokeLDAPUserFilter, "LDAP user search filter")
	flag.StringVar(&opts.LDAPSubjectAttr, "ldap-subject-attribute", "", "LDAP attribute used as stable external subject; defaults to entry DN")
	flag.StringVar(&opts.LDAPUsernameAttr, "ldap-username-attribute", "uid", "LDAP attribute mapped to username")
	flag.StringVar(&opts.LDAPDisplayNameAttr, "ldap-display-name-attribute", "cn", "LDAP attribute mapped to display name")
	flag.StringVar(&opts.LDAPUsername, "ldap-username", kubernetesdeploy.DefaultSmokeLDAPUsername, "LDAP username used by the auth smoke")
	flag.StringVar(&opts.LDAPPassword, "ldap-password", kubernetesdeploy.DefaultSmokeLDAPPassword, "LDAP password used by the auth smoke")
	flag.StringVar(&opts.LDAPWrongPassword, "ldap-wrong-password", kubernetesdeploy.DefaultSmokeLDAPWrongPassword, "LDAP password expected to fail")
	flag.StringVar(&opts.LDAPExpectedSubject, "ldap-expected-subject", kubernetesdeploy.DefaultSmokeLDAPExpectedSubject, "Expected LDAP identity subject")
	flag.StringVar(&opts.LDAPExpectedName, "ldap-expected-display-name", kubernetesdeploy.DefaultSmokeLDAPExpectedName, "Expected LDAP display name")
	flag.StringVar(&opts.LDAPBootstrapToken, "ldap-bootstrap-token", kubernetesdeploy.DefaultSmokeLDAPBootstrapToken, "API bootstrap token used if the deployed API setup is pending")
	flag.StringVar(&opts.LDAPAdminUsername, "ldap-admin-username", "root", "Initial setup admin username used by the LDAP auth smoke")
	flag.StringVar(&opts.LDAPAdminPassword, "ldap-admin-password", "longenough", "Initial setup admin password expected to be disabled after LDAP setup")
	flag.BoolVar(&opts.LDAPKeepFixture, "ldap-keep-fixture", false, "Keep the LDAP fixture deployment, service, and ConfigMap after the LDAP auth smoke")
	flag.BoolVar(&seedSecret, "seed-secret", kubernetesdeploy.DefaultSmokeSeedSecret, "Seed the canonical encryptedfs smoke secret before submitting the job")
	flag.IntVar(&opts.APILocalPort, "api-local-port", kubernetesdeploy.DefaultSmokeAPIPort, "Local port used for the API port-forward")
	flag.DurationVar(&opts.Wait, "wait", kubernetesdeploy.DefaultSmokeWait, "Maximum time to wait for each smoke phase")
	flag.StringVar(&opts.APIToken, "api-token", os.Getenv("VECTIS_API_TOKEN"), "Optional API bearer token")
	flag.BoolVar(&outputJSON, "json", false, "Write a JSON result after a successful smoke")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts.SeedSecret = &seedSecret
	opts.Stdout = os.Stdout
	result, err := kubernetesdeploy.RunSmoke(ctx, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if outputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "Error: write JSON result: %v\n", err)
			os.Exit(1)
		}
	}
}
