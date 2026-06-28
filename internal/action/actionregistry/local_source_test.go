package actionregistry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/action"
)

func TestLocalManifestSourceResolveVersionedManifest(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeLocalManifest(t, root, "acme", "cache", "1.2.3", LocalManifest{
		SchemaVersion: 1,
		Name:          "acme/cache",
		DisplayName:   "Cache",
		Version:       "1.2.3",
		Runtime:       RuntimeProcess,
		RuntimeConfig: map[string]string{
			"entrypoint": "./cache-action",
		},
		InputSchema: InputSchema{
			Fields: []InputField{
				{Name: "key", Type: action.FieldString, Required: true},
				{Name: "ttl", Type: action.FieldNumber},
			},
		},
		Capabilities: []Capability{CapabilityProcessLaunch, CapabilityWorkspaceWrite},
	})

	source := newLocalManifestSourceForTest(t, root)
	descriptor, err := source.ResolveDescriptor("acme/cache@1.2.3")
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.CanonicalName != "acme/cache" || descriptor.DisplayName != "Cache" || descriptor.Version != "1.2.3" {
		t.Fatalf("descriptor identity mismatch: %+v", descriptor)
	}

	if descriptor.Source != SourceLocalFilesystem || descriptor.Runtime != RuntimeProcess {
		t.Fatalf("descriptor source/runtime mismatch: %+v", descriptor)
	}

	if descriptor.RuntimeConfig["entrypoint"] != "./cache-action" {
		t.Fatalf("runtime_config entrypoint: got %+v", descriptor.RuntimeConfig)
	}

	if descriptor.SourcePath == "" || filepath.Base(descriptor.SourcePath) != "1.2.3" {
		t.Fatalf("source path: got %q", descriptor.SourcePath)
	}

	payload, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatalf("Marshal descriptor: %v", err)
	}

	if strings.Contains(string(payload), "source_path") || strings.Contains(string(payload), "SourcePath") {
		t.Fatalf("descriptor JSON leaked source path: %s", payload)
	}

	if !strings.HasPrefix(descriptor.Digest, "sha256:") {
		t.Fatalf("descriptor digest: %q", descriptor.Digest)
	}
}

func TestLocalManifestSourceResolveByDigestSearchesVersionDirs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeLocalManifest(t, root, "acme", "cache", "1.2.3", localCacheManifest("1.2.3", "first"))
	writeLocalManifest(t, root, "acme", "cache", "1.2.4", localCacheManifest("1.2.4", "second"))

	source := newLocalManifestSourceForTest(t, root)
	versioned, err := source.ResolveDescriptor("acme/cache@1.2.4")
	if err != nil {
		t.Fatalf("ResolveDescriptor versioned: %v", err)
	}

	byDigest, err := source.ResolveDescriptor("acme/cache@" + versioned.Digest)
	if err != nil {
		t.Fatalf("ResolveDescriptor digest: %v", err)
	}

	if byDigest.Version != "1.2.4" || byDigest.Digest != versioned.Digest {
		t.Fatalf("digest resolution mismatch: got %+v want %+v", byDigest, versioned)
	}
}

func TestLocalManifestSourceUsesExplicitDigest(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	explicitDigest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	manifest := localCacheManifest("1.2.3", "explicit")
	manifest.Digest = explicitDigest
	writeLocalManifest(t, root, "acme", "cache", "", manifest)

	source := newLocalManifestSourceForTest(t, root)
	descriptor, err := source.ResolveDescriptor("acme/cache@" + explicitDigest)
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.Digest != explicitDigest {
		t.Fatalf("descriptor digest: got %q want %q", descriptor.Digest, explicitDigest)
	}
}

func TestLocalManifestSourceLoadsTombstoneStatus(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	explicitDigest := "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	manifest := localCacheManifest("1.2.3", "")
	manifest.Digest = explicitDigest
	manifest.Status = DescriptorStatusRevoked
	manifest.StatusReason = "CVE-2026-0001"
	writeLocalManifest(t, root, "acme", "cache", "", manifest)

	source := newLocalManifestSourceForTest(t, root)
	descriptor, err := source.ResolveDescriptor("acme/cache@" + explicitDigest)
	if err != nil {
		t.Fatalf("ResolveDescriptor: %v", err)
	}

	if descriptor.Digest != explicitDigest {
		t.Fatalf("descriptor digest: got %q want %q", descriptor.Digest, explicitDigest)
	}

	if descriptor.LifecycleStatus() != DescriptorStatusRevoked || descriptor.StatusReason != "CVE-2026-0001" {
		t.Fatalf("descriptor status mismatch: %+v", descriptor)
	}
}

func TestLocalManifestSourceListDescriptors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeLocalManifest(t, root, "acme", "cache", "", localCacheManifest("1.2.3", "base"))
	writeLocalManifest(t, root, "acme", "cache", "1.2.4", localCacheManifest("1.2.4", "versioned"))
	writeLocalManifest(t, root, "examples", "greet", "", LocalManifest{
		SchemaVersion: 1,
		Name:          "examples/greet",
		DisplayName:   "Greet",
		Version:       "v1",
		Runtime:       RuntimeProcess,
		RuntimeConfig: map[string]string{
			"command": `echo "Hello, ${VECTIS_INPUT_NAME}"`,
		},
		InputSchema: InputSchema{
			Fields: []InputField{{Name: "name", Type: action.FieldString, Required: true}},
		},
		Capabilities: []Capability{CapabilityProcessLaunch},
	})

	writeLocalManifest(t, root, "builtins", "fake", "", LocalManifest{
		SchemaVersion: 1,
		Name:          "builtins/fake",
		Version:       "v1",
		Runtime:       RuntimeProcess,
	})

	source := newLocalManifestSourceForTest(t, root)
	descriptors, err := source.ListDescriptors()
	if err != nil {
		t.Fatalf("ListDescriptors: %v", err)
	}

	got := make([]string, 0, len(descriptors))
	for _, descriptor := range descriptors {
		got = append(got, descriptor.CanonicalName+"@"+descriptor.Version)
		if descriptor.SourcePath == "" {
			t.Fatalf("descriptor missing source path: %+v", descriptor)
		}
	}

	want := []string{
		"acme/cache@1.2.3",
		"acme/cache@1.2.4",
		"examples/greet@v1",
	}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("listed descriptors = %v, want %v", got, want)
	}
}

func TestLocalManifestSourceRejectsInvalidManifest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		manifest LocalManifest
		want     string
	}{
		{name: "schema version", manifest: LocalManifest{Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeProcess}, want: "schema_version"},
		{name: "name mismatch", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/other", Version: "1.2.3", Runtime: RuntimeProcess}, want: "does not match reference"},
		{name: "builtin runtime", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeBuiltin}, want: "reserved for builtins"},
		{name: "unknown status", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeProcess, Status: "retired"}, want: "status"},
		{name: "absolute working directory", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeProcess, RuntimeConfig: map[string]string{"working_directory": "/tmp"}}, want: "must be relative"},
		{name: "escaping working directory", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeProcess, RuntimeConfig: map[string]string{"working_directory": "../outside"}}, want: "must stay within the action base directory"},
		{
			name: "duplicate field",
			manifest: LocalManifest{
				SchemaVersion: 1,
				Name:          "acme/cache",
				Version:       "1.2.3",
				Runtime:       RuntimeProcess,
				InputSchema: InputSchema{
					Fields: []InputField{
						{Name: "key", Type: action.FieldString},
						{Name: "key", Type: action.FieldString},
					},
				},
			},
			want: "duplicated",
		},
		{name: "unknown capability", manifest: LocalManifest{SchemaVersion: 1, Name: "acme/cache", Version: "1.2.3", Runtime: RuntimeProcess, Capabilities: []Capability{"telepathy"}}, want: "unsupported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := tt.manifest.Descriptor(mustParseLocalRef(t, "acme/cache"))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected %q error, got %v", tt.want, err)
			}
		})
	}
}

func localCacheManifest(version, entrypoint string) LocalManifest {
	return LocalManifest{
		SchemaVersion: 1,
		Name:          "acme/cache",
		Version:       version,
		Runtime:       RuntimeProcess,
		RuntimeConfig: map[string]string{
			"entrypoint": entrypoint,
		},
		InputSchema: InputSchema{
			Fields: []InputField{{Name: "key", Type: action.FieldString, Required: true}},
		},
		Capabilities: []Capability{CapabilityProcessLaunch},
	}
}

func newLocalManifestSourceForTest(t *testing.T, root string) *LocalManifestSource {
	t.Helper()

	source, err := NewLocalManifestSource(root)
	if err != nil {
		t.Fatalf("NewLocalManifestSource: %v", err)
	}

	return source
}

func writeLocalManifest(t *testing.T, root, namespace, name, version string, manifest LocalManifest) {
	t.Helper()

	dir := filepath.Join(root, namespace, name)
	if version != "" {
		dir = filepath.Join(dir, version)
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, LocalManifestFile), payload, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func mustParseLocalRef(t *testing.T, raw string) Reference {
	t.Helper()

	ref, err := ParseReference(raw)
	if err != nil {
		t.Fatalf("ParseReference: %v", err)
	}

	return ref
}
