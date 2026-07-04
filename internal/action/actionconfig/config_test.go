package actionconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"

	"github.com/spf13/viper"
)

func TestDescriptorResolverIncludesLocalRoots(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeGreetActionManifest(t, root)

	viper.Set("action_registry.local_roots", []string{root})

	resolver, err := DescriptorResolver()
	if err != nil {
		t.Fatalf("DescriptorResolver(): %v", err)
	}

	descriptor, err := resolver.ResolveDescriptor("examples/greet@v1")
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.Source != actionregistry.SourceLocalFilesystem {
		t.Fatalf("descriptor source = %q, want %q", descriptor.Source, actionregistry.SourceLocalFilesystem)
	}

	if descriptor.DisplayName != "Greet" {
		t.Fatalf("descriptor display name = %q, want Greet", descriptor.DisplayName)
	}
}

func TestDescriptorResolverRejectsDisallowedNamespace(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.allowed_namespaces", []string{"acme"})

	resolver, err := DescriptorResolver()
	if err != nil {
		t.Fatalf("DescriptorResolver(): %v", err)
	}

	_, err = resolver.ResolveDescriptor("examples/greet@v1")
	if err == nil || !strings.Contains(err.Error(), `namespace "examples"`) {
		t.Fatalf("ResolveDescriptor error = %v, want namespace policy error", err)
	}
}

func TestDescriptorResolverRejectsDisallowedSource(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.allowed_sources", []string{"oci"})

	resolver, err := DescriptorResolver()
	if err != nil {
		t.Fatalf("DescriptorResolver(): %v", err)
	}

	_, err = resolver.ResolveDescriptor("examples/greet@v1")
	if err == nil || !strings.Contains(err.Error(), `source "local_filesystem"`) {
		t.Fatalf("ResolveDescriptor error = %v, want source policy error", err)
	}
}

func TestDescriptorResolverRequiresDigestPinsForCustomActions(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeGreetActionManifest(t, root)
	source, err := actionregistry.NewLocalManifestSource(root)
	if err != nil {
		t.Fatalf("NewLocalManifestSource: %v", err)
	}

	greet, err := source.ResolveDescriptor("examples/greet@v1")
	if err != nil {
		t.Fatalf("ResolveDescriptor local source: %v", err)
	}

	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.require_digest_pins", true)
	resolver, err := DescriptorResolver()
	if err != nil {
		t.Fatalf("DescriptorResolver(): %v", err)
	}

	if _, err := resolver.ResolveDescriptor("examples/greet@v1"); err == nil || !strings.Contains(err.Error(), "must be pinned by digest") {
		t.Fatalf("ResolveDescriptor version selector error = %v, want digest pin policy error", err)
	}

	if _, err := resolver.ResolveDescriptor("examples/greet@" + greet.Digest); err != nil {
		t.Fatalf("ResolveDescriptor digest selector: %v", err)
	}
}

func TestDescriptorResolverPolicyAllowsBuiltins(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("action_registry.allowed_namespaces", []string{"examples"})
	viper.Set("action_registry.allowed_sources", []string{"oci"})
	viper.Set("action_registry.require_digest_pins", true)

	resolver, err := DescriptorResolver()
	if err != nil {
		t.Fatalf("DescriptorResolver(): %v", err)
	}

	if _, err := resolver.ResolveDescriptor("builtins/script"); err != nil {
		t.Fatalf("ResolveDescriptor builtin: %v", err)
	}
}

func TestDescriptorResolverRejectsInvalidLocalRoot(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	missing := filepath.Join(t.TempDir(), "missing")
	viper.Set("action_registry.local_roots", []string{missing})

	_, err := DescriptorResolver()
	if err == nil || !strings.Contains(err.Error(), "action_registry.local_roots") {
		t.Fatalf("DescriptorResolver() error = %v, want local roots error", err)
	}
}

func writeGreetActionManifest(t *testing.T, root string) {
	t.Helper()

	writeActionManifest(t, root, "examples/greet", `{
		"schema_version": 1,
		"name": "examples/greet",
		"display_name": "Greet",
		"version": "v1",
		"runtime": "process",
		"runtime_config": {
			"command": "echo \"Hello, ${VECTIS_INPUT_NAME}\""
		},
		"input_schema": {
			"fields": [
				{"name": "name", "type": "string", "required": true}
			]
		},
		"capabilities": ["process_launch"]
	}`)
}

func writeActionManifest(t *testing.T, root, name, payload string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(name), actionregistry.LocalManifestFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}
