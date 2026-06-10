package builtins

import (
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"
)

func TestRegistryResolveDescriptorForBuiltins(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	descriptor, err := registry.ResolveDescriptor("builtins/shell")
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.CanonicalName != "builtins/shell" || descriptor.DisplayName != "Shell" || descriptor.Version != "v1" {
		t.Fatalf("unexpected descriptor: %+v", descriptor)
	}

	if descriptor.Source != actionregistry.SourceBuiltin || descriptor.Runtime != actionregistry.RuntimeBuiltin {
		t.Fatalf("unexpected source/runtime: %+v", descriptor)
	}

	if !strings.HasPrefix(descriptor.Digest, "sha256:") {
		t.Fatalf("descriptor digest: %q", descriptor.Digest)
	}
}

func TestRegistryResolvesBuiltinSelectors(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	descriptor, err := registry.ResolveDescriptor("builtins/shell")
	if err != nil {
		t.Fatalf("ResolveDescriptor base: %v", err)
	}

	for _, uses := range []string{
		"shell",
		"shell@v1",
		"builtins/shell@v1",
		"builtins/shell@" + descriptor.Digest,
	} {
		t.Run(uses, func(t *testing.T) {
			t.Parallel()

			node, err := registry.Resolve(uses)
			if err != nil {
				t.Fatalf("Resolve(%q): %v", uses, err)
			}

			if node.Type() != "builtins/shell" {
				t.Fatalf("node type: got %q", node.Type())
			}
		})
	}
}

func TestRegistryRejectsSelectorMismatch(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	_, err := registry.Resolve("builtins/shell@v2")
	if err == nil || !strings.Contains(err.Error(), "does not match resolved version") {
		t.Fatalf("expected version mismatch, got %v", err)
	}
}

func TestRegistryRejectsUnknownAction(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	_, err := registry.ResolveDescriptor("acme/cache@v1")
	if err == nil || !strings.Contains(err.Error(), "unknown action") {
		t.Fatalf("expected unknown action, got %v", err)
	}
}
