package api

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestInternalGRPCServiceReferenceMentionsProtoServicesAndRPCs(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "operating", "reference", "internal-grpc-service-reference.md"))
	if err != nil {
		t.Fatalf("read internal gRPC service reference: %v", err)
	}
	doc := string(raw)

	tokens := protoServiceReferenceTokens(t)
	tokens = append(tokens,
		"queue.port",
		"registry.port",
		"log.grpc.port",
		"artifact.grpc.port",
		"orchestrator.port",
		"secrets.port",
		"worker.control.port",
		"worker.core.socket",
		"worker.core.shell_socket",
		"cell_ingress.port",
		"grpc_tls.insecure",
		"grpc_tls.client_ca_file",
		"service_identity.queue_allowed_client_identities",
		"service_identity.log_allowed_client_identities",
		"service_identity.artifact_allowed_client_identities",
		"service_identity.orchestrator_allowed_client_identities",
		"service_identity.worker_control_allowed_client_identities",
		"service_identity.secrets_allowed_client_identities",
	)

	var missing []string
	for _, token := range tokens {
		if !internalGRPCReferenceContains(doc, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("internal gRPC service reference is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func protoServiceReferenceTokens(t *testing.T) []string {
	t.Helper()

	files, err := filepath.Glob(filepath.Join("..", "..", "api", "proto", "*.proto"))
	if err != nil {
		t.Fatalf("glob proto files: %v", err)
	}

	serviceRe := regexp.MustCompile(`^\s*service\s+([A-Za-z0-9_]+)\s*\{`)
	rpcRe := regexp.MustCompile(`^\s*rpc\s+([A-Za-z0-9_]+)\s*\(`)
	seen := map[string]bool{}
	var tokens []string

	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}

		for _, line := range strings.Split(string(raw), "\n") {
			if match := serviceRe.FindStringSubmatch(line); match != nil {
				token := match[1]
				if !seen[token] {
					seen[token] = true
					tokens = append(tokens, token)
				}
				continue
			}

			if match := rpcRe.FindStringSubmatch(line); match != nil {
				token := match[1]
				if !seen[token] {
					seen[token] = true
					tokens = append(tokens, token)
				}
			}
		}
	}

	if len(tokens) == 0 {
		t.Fatal("no proto service tokens found")
	}

	return tokens
}

func internalGRPCReferenceContains(doc, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
		" " + token + " ",
	} {
		if strings.Contains(doc, needle) {
			return true
		}
	}

	return false
}
