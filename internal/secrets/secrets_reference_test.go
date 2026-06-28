package secrets

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestSecretsReferenceMentionsContractConstants(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "using", "secrets-reference.md"))
	if err != nil {
		t.Fatalf("read secrets reference: %v", err)
	}
	doc := string(raw)

	tokens := []string{
		"secrets",
		"id",
		"ref",
		"delivery",
		"type",
		"path",
		"task_keys",
		string(DeliveryTypeFile),
		"SECRET_DELIVERY_TYPE_FILE",
		WorkspaceSecretsDir,
		"VECTIS_SECRETS_DIR",
		fmt.Sprintf("%04o", DefaultFileMode.Perm()),
		EncryptedFSScheme,
		fmt.Sprintf("%d", EncryptedFSKeySize),
		fmt.Sprintf("%d", DefaultMaxSecretBytes),
		"AES-256-GCM",
		"ResolveSecrets",
		"ResolveSecretsRequest",
		"ResolveSecretsResponse",
		"SecretFileMaterial",
		"run_id",
		"execution_id",
		"execution_claim_token",
		"files",
		"mode",
		"namespace",
		"namespace_path",
		"job",
		"job_id",
		"task",
		"task_key",
		"secret_ref",
		"worker.secrets.address",
		"VECTIS_WORKER_SECRETS_ADDRESS",
		"secrets.encryptedfs.root",
		"VECTIS_SECRETS_ENCRYPTEDFS_ROOT",
		"secrets.encryptedfs.key_file",
		"VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE",
		"secrets.policy.allow",
		"VECTIS_SECRETS_POLICY_ALLOW",
		"worker.execution_identity.enabled",
		"worker.spiffe.enabled",
		"grpc_tls.client_ca_file",
		"service_identity.secrets_allowed_client_identities",
		"vectis_secrets_resolve_requests_total",
		"vectis_secrets_resolve_duration_seconds",
		"secret_resolution",
		"latest_failed_security_event",
	}

	tokens = append(tokens, []string{
		resolveOutcomeSuccess,
		resolveOutcomeDenied,
		resolveOutcomeNotFound,
		resolveOutcomeFailed,
		resolveReasonOK,
		resolveReasonUnknown,
		resolveReasonMissingProvider,
		resolveReasonMissingAuthorizer,
		resolveReasonAuthorizationDenied,
		resolveReasonProviderDenied,
		resolveReasonProviderNotFound,
		resolveReasonProviderError,
		resolveReasonInvalidBundle,
	}...)

	var missing []string
	for _, token := range tokens {
		if !containsSecretsReferenceToken(doc, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("secrets reference is missing contract tokens: %s", strings.Join(missing, ", "))
	}
}

func containsSecretsReferenceToken(doc, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
	} {
		if strings.Contains(doc, needle) {
			return true
		}
	}

	return false
}
