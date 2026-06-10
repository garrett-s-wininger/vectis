package actionregistry

import (
	"strings"
	"testing"

	"vectis/internal/action"
)

func TestInputSchemaValidateWithRejectsUnknownWhenClosed(t *testing.T) {
	t.Parallel()

	schema := InputSchema{}
	errs := schema.ValidateWith(map[string]string{"extra": "value"})
	if len(errs) != 1 || errs[0].Field != "extra" {
		t.Fatalf("expected unknown extra field, got %+v", errs)
	}
}

func TestInputSchemaValidateWithAllowsUnknownWhenOpen(t *testing.T) {
	t.Parallel()

	schema := InputSchema{
		Fields:       []InputField{{Name: "command", Type: action.FieldString, Required: true}},
		AllowUnknown: true,
	}

	if errs := schema.ValidateWith(map[string]string{"command": "echo hi", "extra": "value"}); len(errs) != 0 {
		t.Fatalf("expected unknown fields to be allowed, got %+v", errs)
	}
}

func TestInputSchemaValidateWithStillChecksKnownFieldsWhenOpen(t *testing.T) {
	t.Parallel()

	schema := InputSchema{
		Fields:       []InputField{{Name: "command", Type: action.FieldString, Required: true}},
		AllowUnknown: true,
	}

	errs := schema.ValidateWith(map[string]string{"extra": "value"})
	if len(errs) != 1 || errs[0].Field != "command" {
		t.Fatalf("expected missing command, got %+v", errs)
	}
}

func TestDescriptorDigestIsStable(t *testing.T) {
	t.Parallel()

	descriptor := Descriptor{
		CanonicalName: "acme/cache",
		Version:       "1.2.3",
		Source:        SourceLocalFilesystem,
		Runtime:       RuntimeProcess,
		InputSchema: InputSchema{
			Fields: []InputField{{Name: "key", Type: action.FieldString, Required: true}},
		},
		Capabilities: []Capability{CapabilityWorkspaceWrite, CapabilityProcessLaunch},
	}

	first, err := DescriptorDigest(descriptor)
	if err != nil {
		t.Fatalf("DescriptorDigest: %v", err)
	}

	descriptor.Capabilities = []Capability{CapabilityProcessLaunch, CapabilityWorkspaceWrite}
	second, err := DescriptorDigest(descriptor)
	if err != nil {
		t.Fatalf("DescriptorDigest second: %v", err)
	}

	if first != second {
		t.Fatalf("digest changed after capability reorder: %q then %q", first, second)
	}

	if !strings.HasPrefix(first, "sha256:") || len(first) != len("sha256:")+64 {
		t.Fatalf("unexpected digest format: %q", first)
	}
}

func TestDescriptorDigestIncludesRuntimeConfig(t *testing.T) {
	t.Parallel()

	descriptor := Descriptor{
		CanonicalName: "acme/cache",
		Version:       "1.2.3",
		Source:        SourceLocalFilesystem,
		Runtime:       RuntimeProcess,
		RuntimeConfig: map[string]string{"entrypoint": "./one"},
	}

	first, err := DescriptorDigest(descriptor)
	if err != nil {
		t.Fatalf("DescriptorDigest first: %v", err)
	}

	descriptor.RuntimeConfig["entrypoint"] = "./two"
	second, err := DescriptorDigest(descriptor)
	if err != nil {
		t.Fatalf("DescriptorDigest second: %v", err)
	}

	if first == second {
		t.Fatalf("digest did not change after runtime_config changed: %q", first)
	}
}

func TestDescriptorMatchReference(t *testing.T) {
	t.Parallel()

	descriptor := Descriptor{
		CanonicalName: "acme/cache",
		Version:       "1.2.3",
		Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	for _, raw := range []string{
		"acme/cache",
		"acme/cache@1.2.3",
		"acme/cache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		ref, err := ParseReference(raw)
		if err != nil {
			t.Fatalf("ParseReference(%q): %v", raw, err)
		}

		if err := descriptor.MatchReference(ref); err != nil {
			t.Fatalf("MatchReference(%q): %v", raw, err)
		}
	}

	ref, err := ParseReference("acme/cache@2.0.0")
	if err != nil {
		t.Fatalf("ParseReference mismatch: %v", err)
	}

	if err := descriptor.MatchReference(ref); err == nil || !strings.Contains(err.Error(), "does not match resolved version") {
		t.Fatalf("expected version mismatch, got %v", err)
	}
}
