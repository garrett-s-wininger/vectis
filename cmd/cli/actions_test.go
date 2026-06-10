package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"

	"github.com/spf13/viper"
)

func TestResolveActionText(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})

	var buf bytes.Buffer
	if err := resolveAction(&buf, "examples/greet@v1", false); err != nil {
		t.Fatalf("resolveAction: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"Reference:     examples/greet@v1",
		"Resolved:      examples/greet@sha256:",
		"Name:          examples/greet",
		"Display name:  Greet",
		"Version:       v1",
		"Runtime:       process",
		"Capabilities:  process_launch",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestResolveActionJSON(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})

	var buf bytes.Buffer
	if err := resolveAction(&buf, "examples/greet@v1", false); err != nil {
		t.Fatalf("resolveAction: %v", err)
	}

	var result actionResolveResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if result.Reference != "examples/greet@v1" {
		t.Fatalf("Reference = %q, want examples/greet@v1", result.Reference)
	}

	if !strings.HasPrefix(result.ResolvedReference, "examples/greet@sha256:") {
		t.Fatalf("ResolvedReference = %q, want digest-pinned examples/greet", result.ResolvedReference)
	}

	if result.Descriptor.CanonicalName != "examples/greet" {
		t.Fatalf("descriptor canonical name = %q, want examples/greet", result.Descriptor.CanonicalName)
	}

	if result.Descriptor.Runtime != actionregistry.RuntimeProcess {
		t.Fatalf("descriptor runtime = %q, want %q", result.Descriptor.Runtime, actionregistry.RuntimeProcess)
	}

	if got := result.Descriptor.RuntimeConfig["command"]; got != `echo "Hello, ${VECTIS_INPUT_NAME}"` {
		t.Fatalf("runtime command = %q", got)
	}
}

func TestResolveActionEnforcesPolicy(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.require_digest_pins", true)

	var buf bytes.Buffer
	err := resolveAction(&buf, "examples/greet@v1", false)
	if err == nil || !strings.Contains(err.Error(), "must be pinned by digest") {
		t.Fatalf("resolveAction error = %v, want digest pin policy error", err)
	}
}

func TestResolveActionIgnorePolicyFindsDigest(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.require_digest_pins", true)

	var buf bytes.Buffer
	if err := resolveAction(&buf, "examples/greet@v1", true); err != nil {
		t.Fatalf("resolveAction: %v", err)
	}

	if out := buf.String(); !strings.Contains(out, "Resolved:      examples/greet@sha256:") {
		t.Fatalf("output missing resolved digest:\n%s", out)
	}
}

func TestListActionsText(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})

	var buf bytes.Buffer
	if err := listActions(&buf, false); err != nil {
		t.Fatalf("listActions: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"NAME",
		"builtins/shell",
		"examples/greet",
		"local_filesystem",
		"sha256:",
		"Greet",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestListActionsJSON(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	withOutputFormat(t, outputJSON)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})

	var buf bytes.Buffer
	if err := listActions(&buf, false); err != nil {
		t.Fatalf("listActions: %v", err)
	}

	var result actionListResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if !actionListContains(result.Actions, "builtins/shell") {
		t.Fatalf("list missing builtins/shell: %+v", result.Actions)
	}

	if !actionListContains(result.Actions, "examples/greet") {
		t.Fatalf("list missing examples/greet: %+v", result.Actions)
	}
}

func TestListActionsPolicyFiltersCustomActions(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.allowed_namespaces", []string{"acme"})

	var buf bytes.Buffer
	if err := listActions(&buf, false); err != nil {
		t.Fatalf("listActions: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "builtins/shell") {
		t.Fatalf("policy-filtered list should keep builtins:\n%s", out)
	}

	if strings.Contains(out, "examples/greet") {
		t.Fatalf("policy-filtered list included disallowed action:\n%s", out)
	}
}

func TestListActionsIgnorePolicyIncludesFilteredActions(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	root := t.TempDir()
	writeCLIGreetActionManifest(t, root)
	viper.Set("action_registry.local_roots", []string{root})
	viper.Set("action_registry.allowed_namespaces", []string{"acme"})

	var buf bytes.Buffer
	if err := listActions(&buf, true); err != nil {
		t.Fatalf("listActions: %v", err)
	}

	if out := buf.String(); !strings.Contains(out, "examples/greet") {
		t.Fatalf("ignore-policy list missing examples/greet:\n%s", out)
	}
}

func writeCLIGreetActionManifest(t *testing.T, root string) {
	t.Helper()

	writeCLIActionManifest(t, root, "examples/greet", `{
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

func writeCLIActionManifest(t *testing.T, root, name, payload string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(name), actionregistry.LocalManifestFile)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func actionListContains(actions []actionregistry.Descriptor, canonicalName string) bool {
	for _, descriptor := range actions {
		if descriptor.CanonicalName == canonicalName {
			return true
		}
	}

	return false
}
