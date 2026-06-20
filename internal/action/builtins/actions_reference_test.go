package builtins

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"
)

func TestActionsReferenceMentionsBuiltinsAndDescriptorContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "website", "docs", "using", "actions-reference.md"))
	if err != nil {
		t.Fatalf("read actions reference: %v", err)
	}
	doc := string(raw)

	descriptors, err := NewRegistry().ListDescriptors()
	if err != nil {
		t.Fatalf("list built-in descriptors: %v", err)
	}

	tokens := []string{
		"canonical_name",
		"display_name",
		"version",
		"digest",
		"source",
		"runtime",
		"runtime_config",
		"input_schema",
		"port_schema",
		"local_only",
		"capabilities",
		"status",
		"status_reason",
		string(actionregistry.SourceBuiltin),
		string(actionregistry.SourceLocalFilesystem),
		string(actionregistry.SourceOCI),
		string(actionregistry.RuntimeBuiltin),
		string(actionregistry.RuntimeProcess),
		string(actionregistry.RuntimeContainer),
		string(actionregistry.RuntimeWASM),
		string(actionregistry.RuntimeGRPC),
		string(actionregistry.DescriptorStatusActive),
		string(actionregistry.DescriptorStatusYanked),
		string(actionregistry.DescriptorStatusRevoked),
		string(actionregistry.DescriptorStatusPurged),
		string(actionregistry.CapabilityProcessLaunch),
		string(actionregistry.CapabilityNetwork),
		string(actionregistry.CapabilityWorkspaceRead),
		string(actionregistry.CapabilityWorkspaceWrite),
		string(actionregistry.CapabilitySecrets),
		"action_registry.local_roots",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS",
		"action_registry.allowed_namespaces",
		"VECTIS_ACTION_REGISTRY_ALLOWED_NAMESPACES",
		"action_registry.allowed_sources",
		"VECTIS_ACTION_REGISTRY_ALLOWED_SOURCES",
		"action_registry.require_digest_pins",
		"VECTIS_ACTION_REGISTRY_REQUIRE_DIGEST_PINS",
		"actions list",
		"actions resolve",
		"resolved_reference",
		"sha256:<64-hex-digest>",
	}

	for _, descriptor := range descriptors {
		tokens = append(tokens, descriptor.CanonicalName)
		for _, field := range descriptor.InputSchema.Fields {
			tokens = append(tokens, field.Name)
		}
		for _, port := range descriptor.PortSchema {
			tokens = append(tokens, port.Name)
		}
		for _, capability := range descriptor.Capabilities {
			tokens = append(tokens, string(capability))
		}
	}

	var missing []string
	for _, token := range tokens {
		if !actionsReferenceContains(doc, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("actions reference is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func actionsReferenceContains(doc, token string) bool {
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
