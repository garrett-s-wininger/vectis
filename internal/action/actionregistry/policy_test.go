package actionregistry

import (
	"fmt"
	"strings"
	"testing"
)

func TestPolicyAllowsBuiltins(t *testing.T) {
	t.Parallel()

	ref := parsePolicyRef(t, "builtins/shell")
	descriptor := policyDescriptorWithSource("builtins/shell", SourceBuiltin)
	policy := Policy{
		AllowedNamespaces: []string{"examples"},
		AllowedSources:    []SourceType{SourceLocalFilesystem},
		RequireDigestPins: true,
	}

	if err := policy.Validate(ref, descriptor); err != nil {
		t.Fatalf("Validate builtin: %v", err)
	}
}

func TestPolicyRejectsDisallowedNamespace(t *testing.T) {
	t.Parallel()

	policy := Policy{AllowedNamespaces: []string{"examples"}}
	err := policy.Validate(parsePolicyRef(t, "acme/cache@v1"), policyDescriptor("acme/cache"))
	if err == nil || !strings.Contains(err.Error(), `namespace "acme"`) {
		t.Fatalf("Validate error = %v, want namespace policy error", err)
	}
}

func TestPolicyRejectsDisallowedSource(t *testing.T) {
	t.Parallel()

	policy := Policy{AllowedSources: []SourceType{SourceOCI}}
	err := policy.Validate(parsePolicyRef(t, "examples/cache@v1"), policyDescriptor("examples/cache"))
	if err == nil || !strings.Contains(err.Error(), `source "local_filesystem"`) {
		t.Fatalf("Validate error = %v, want source policy error", err)
	}
}

func TestPolicyRequiresDigestPinsForCustomActions(t *testing.T) {
	t.Parallel()

	policy := Policy{RequireDigestPins: true}
	err := policy.Validate(parsePolicyRef(t, "examples/cache@v1"), policyDescriptor("examples/cache"))
	if err == nil || !strings.Contains(err.Error(), "must be pinned by digest") {
		t.Fatalf("Validate version selector error = %v, want digest pin error", err)
	}

	ref := parsePolicyRef(t, "examples/cache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err := policy.Validate(ref, policyDescriptor("examples/cache")); err != nil {
		t.Fatalf("Validate digest selector: %v", err)
	}
}

func TestPolicyResolverAppliesPolicyAfterResolution(t *testing.T) {
	t.Parallel()

	resolver := NewPolicyResolver(policyMapResolver{
		"examples/cache@v1": policyDescriptor("examples/cache"),
	}, Policy{RequireDigestPins: true})

	_, err := resolver.ResolveDescriptor("examples/cache@v1")
	if err == nil || !strings.Contains(err.Error(), "must be pinned by digest") {
		t.Fatalf("ResolveDescriptor error = %v, want digest pin policy error", err)
	}
}

func TestPolicyResolverListDescriptorsFiltersPolicy(t *testing.T) {
	t.Parallel()

	resolver := NewPolicyResolver(policyMapResolver{
		"builtins/shell":     policyDescriptorWithSource("builtins/shell", SourceBuiltin),
		"examples/cache@v1":  policyDescriptor("examples/cache"),
		"acme/cache@v1":      policyDescriptor("acme/cache"),
		"examples/remote@v1": policyDescriptorWithSource("examples/remote", SourceOCI),
	}, Policy{
		AllowedNamespaces: []string{"examples"},
		AllowedSources:    []SourceType{SourceLocalFilesystem},
		RequireDigestPins: true,
	})

	descriptors, err := resolver.ListDescriptors()
	if err != nil {
		t.Fatalf("ListDescriptors: %v", err)
	}

	got := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		got = append(got, descriptor.CanonicalName)
	}

	want := []string{"builtins/shell", "examples/cache"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("listed descriptors = %v, want %v", got, want)
	}
}

type policyMapResolver map[string]Descriptor

func (r policyMapResolver) ResolveDescriptor(uses string) (Descriptor, error) {
	descriptor, ok := r[uses]
	if !ok {
		return Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func (r policyMapResolver) ListDescriptors() ([]Descriptor, error) {
	descriptors := make([]Descriptor, 0, len(r))
	for _, descriptor := range r {
		descriptors = append(descriptors, descriptor)
	}

	return deduplicateDescriptors(descriptors), nil
}

func policyDescriptor(name string) Descriptor {
	return policyDescriptorWithSource(name, SourceLocalFilesystem)
}

func policyDescriptorWithSource(name string, source SourceType) Descriptor {
	return Descriptor{
		CanonicalName: name,
		Version:       "v1",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Source:        source,
		Runtime:       RuntimeProcess,
	}
}

func parsePolicyRef(t *testing.T, raw string) Reference {
	t.Helper()

	ref, err := ParseReference(raw)
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}

	return ref
}
