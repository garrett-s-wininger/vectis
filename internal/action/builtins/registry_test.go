package builtins

import (
	"strings"
	"testing"

	"vectis/internal/action/actionregistry"
)

func TestRegistryResolveDescriptorForBuiltins(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	descriptor, err := registry.ResolveDescriptor("builtins/script")
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.CanonicalName != "builtins/script" || descriptor.DisplayName != "Script" || descriptor.Version != "v1" {
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
	descriptor, err := registry.ResolveDescriptor("builtins/script")
	if err != nil {
		t.Fatalf("ResolveDescriptor base: %v", err)
	}

	for _, uses := range []string{
		"script",
		"script@v1",
		"builtins/script@v1",
		"builtins/script@" + descriptor.Digest,
	} {
		t.Run(uses, func(t *testing.T) {
			t.Parallel()

			node, err := registry.Resolve(uses)
			if err != nil {
				t.Fatalf("Resolve(%q): %v", uses, err)
			}

			if node.Type() != "builtins/script" {
				t.Fatalf("node type: got %q", node.Type())
			}
		})
	}
}

func TestRegistryRejectsSelectorMismatch(t *testing.T) {
	t.Parallel()

	registry := NewRegistry()
	_, err := registry.Resolve("builtins/script@v2")
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
