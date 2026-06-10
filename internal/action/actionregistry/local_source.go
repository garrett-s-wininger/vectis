package actionregistry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"vectis/internal/action"
)

const LocalManifestFile = "action.json"

type LocalManifestSource struct {
	root string
}

type LocalManifest struct {
	SchemaVersion int               `json:"schema_version"`
	Name          string            `json:"name"`
	DisplayName   string            `json:"display_name,omitempty"`
	Version       string            `json:"version"`
	Digest        string            `json:"digest,omitempty"`
	Runtime       RuntimeType       `json:"runtime"`
	RuntimeConfig map[string]string `json:"runtime_config,omitempty"`
	InputSchema   InputSchema       `json:"input_schema,omitempty"`
	PortSchema    []PortSpec        `json:"port_schema,omitempty"`
	LocalOnly     bool              `json:"local_only,omitempty"`
	Capabilities  []Capability      `json:"capabilities,omitempty"`
	Status        DescriptorStatus  `json:"status,omitempty"`
	StatusReason  string            `json:"status_reason,omitempty"`
}

func NewLocalManifestSource(root string) (*LocalManifestSource, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("local action manifest root is required")
	}

	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat local action manifest root: %w", err)
	}

	if !info.IsDir() {
		return nil, fmt.Errorf("local action manifest root is not a directory: %s", root)
	}

	return &LocalManifestSource{root: root}, nil
}

func (s *LocalManifestSource) ResolveDescriptor(uses string) (Descriptor, error) {
	ref, err := ParseReference(uses)
	if err != nil {
		return Descriptor{}, err
	}

	return s.ResolveReference(ref)
}

func (s *LocalManifestSource) ResolveReference(ref Reference) (Descriptor, error) {
	if ref.Namespace == "builtins" {
		return Descriptor{}, fmt.Errorf("unknown action: %s", ref.String())
	}

	candidates, err := s.manifestCandidates(ref)
	if err != nil {
		return Descriptor{}, err
	}

	var sawManifest bool
	var lastMatchErr error
	for _, candidate := range candidates {
		descriptor, err := s.loadManifest(candidate, ref)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return Descriptor{}, err
		}

		sawManifest = true
		if err := descriptor.MatchReference(ref); err != nil {
			lastMatchErr = err
			continue
		}

		return descriptor, nil
	}

	if lastMatchErr != nil {
		return Descriptor{}, lastMatchErr
	}

	if sawManifest {
		return Descriptor{}, fmt.Errorf("action %q did not match requested selector", ref.CanonicalName())
	}

	return Descriptor{}, fmt.Errorf("unknown action: %s", ref.String())
}

func (s *LocalManifestSource) ListDescriptors() ([]Descriptor, error) {
	namespaces, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("read local action manifest root: %w", err)
	}

	descriptors := []Descriptor{}
	for _, namespaceEntry := range namespaces {
		if !namespaceEntry.IsDir() || namespaceEntry.Name() == "builtins" || !referencePartRe.MatchString(namespaceEntry.Name()) {
			continue
		}

		namespaceDir := filepath.Join(s.root, namespaceEntry.Name())
		actionEntries, err := os.ReadDir(namespaceDir)
		if err != nil {
			return nil, fmt.Errorf("read local action namespace %s: %w", namespaceEntry.Name(), err)
		}

		for _, actionEntry := range actionEntries {
			if !actionEntry.IsDir() || !referencePartRe.MatchString(actionEntry.Name()) {
				continue
			}

			listed, err := s.listActionDescriptors(namespaceEntry.Name(), actionEntry.Name())
			if err != nil {
				return nil, err
			}

			descriptors = append(descriptors, listed...)
		}
	}

	return deduplicateDescriptors(descriptors), nil
}

func (s *LocalManifestSource) listActionDescriptors(namespace, name string) ([]Descriptor, error) {
	ref := Reference{Namespace: namespace, Name: name}
	actionDir := filepath.Join(s.root, namespace, name)
	candidates := []string{filepath.Join(actionDir, LocalManifestFile)}

	entries, err := os.ReadDir(actionDir)
	if err != nil {
		return nil, fmt.Errorf("read local action directory %s/%s: %w", namespace, name, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			candidates = append(candidates, filepath.Join(actionDir, entry.Name(), LocalManifestFile))
		}
	}

	descriptors := []Descriptor{}
	for _, candidate := range candidates {
		descriptor, err := s.loadManifest(candidate, ref)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return nil, err
		}

		descriptors = append(descriptors, descriptor)
	}

	return descriptors, nil
}

func (s *LocalManifestSource) manifestCandidates(ref Reference) ([]string, error) {
	baseDir := filepath.Join(s.root, ref.Namespace, ref.Name)
	base := filepath.Join(baseDir, LocalManifestFile)

	switch ref.SelectorKind {
	case SelectorVersion:
		return []string{filepath.Join(baseDir, ref.Selector, LocalManifestFile), base}, nil
	case SelectorDigest:
		candidates := []string{base}
		entries, err := os.ReadDir(baseDir)
		if err != nil {
			if os.IsNotExist(err) {
				return candidates, nil
			}

			return nil, fmt.Errorf("read local action directory: %w", err)
		}

		versionDirs := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() {
				versionDirs = append(versionDirs, entry.Name())
			}
		}

		sort.Strings(versionDirs)
		for _, dir := range versionDirs {
			candidates = append(candidates, filepath.Join(baseDir, dir, LocalManifestFile))
		}

		return candidates, nil
	default:
		return []string{base}, nil
	}
}

func (s *LocalManifestSource) loadManifest(path string, ref Reference) (Descriptor, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return Descriptor{}, err
	}

	var manifest LocalManifest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Descriptor{}, fmt.Errorf("decode local action manifest %s: %w", path, err)
	}

	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return Descriptor{}, fmt.Errorf("decode local action manifest %s: %w", path, err)
		}

		return Descriptor{}, fmt.Errorf("decode local action manifest %s: trailing JSON data", path)
	}

	descriptor, err := manifest.Descriptor(ref)
	if err != nil {
		return Descriptor{}, err
	}

	descriptor.SourcePath = filepath.Dir(path)
	return descriptor, nil
}

func (m LocalManifest) Descriptor(ref Reference) (Descriptor, error) {
	if err := m.validate(ref); err != nil {
		return Descriptor{}, err
	}

	status := NormalizeDescriptorStatus(m.Status)
	if strings.TrimSpace(string(m.Status)) == "" {
		status = ""
	}

	descriptor := Descriptor{
		CanonicalName: strings.TrimSpace(m.Name),
		DisplayName:   strings.TrimSpace(m.DisplayName),
		Version:       strings.TrimSpace(m.Version),
		Digest:        strings.TrimSpace(m.Digest),
		Source:        SourceLocalFilesystem,
		Runtime:       m.Runtime,
		RuntimeConfig: cloneStringMap(m.RuntimeConfig),
		InputSchema:   cloneInputSchema(m.InputSchema),
		PortSchema:    append([]PortSpec(nil), m.PortSchema...),
		LocalOnly:     m.LocalOnly,
		Capabilities:  append([]Capability(nil), m.Capabilities...),
		Status:        status,
		StatusReason:  strings.TrimSpace(m.StatusReason),
	}

	if descriptor.Digest == "" {
		digest, err := DescriptorDigest(descriptor)
		if err != nil {
			return Descriptor{}, err
		}

		descriptor.Digest = digest
	}

	return descriptor, nil
}

func (m LocalManifest) validate(ref Reference) error {
	if m.SchemaVersion != 1 {
		return fmt.Errorf("local action manifest schema_version must be 1")
	}

	if strings.TrimSpace(m.Name) == "" {
		return fmt.Errorf("local action manifest name is required")
	}

	if strings.TrimSpace(m.Name) != ref.CanonicalName() {
		return fmt.Errorf("local action manifest name %q does not match reference %q", strings.TrimSpace(m.Name), ref.CanonicalName())
	}

	version := strings.TrimSpace(m.Version)
	if version == "" {
		return fmt.Errorf("local action manifest version is required")
	}

	if !selectorRe.MatchString(version) || strings.HasPrefix(version, "sha256:") {
		return fmt.Errorf("local action manifest version %q is invalid", version)
	}

	if digest := strings.TrimSpace(m.Digest); digest != "" && !sha256DigestRe.MatchString(digest) {
		return fmt.Errorf("local action manifest digest %q is invalid", digest)
	}

	if err := ValidateDescriptorStatus(m.Status); err != nil {
		return fmt.Errorf("local action manifest status: %w", err)
	}

	if err := validateLocalRuntime(m.Runtime); err != nil {
		return err
	}

	if m.Runtime == RuntimeProcess && len(m.PortSchema) > 0 {
		return fmt.Errorf("local process actions do not support port_schema")
	}

	if err := validateRuntimeConfig(m.RuntimeConfig); err != nil {
		return err
	}

	if err := validateInputSchema(m.InputSchema); err != nil {
		return err
	}

	if err := validatePortSchema(m.PortSchema); err != nil {
		return err
	}

	return validateCapabilities(m.Capabilities)
}

func validateLocalRuntime(runtime RuntimeType) error {
	switch runtime {
	case RuntimeProcess, RuntimeContainer, RuntimeWASM, RuntimeGRPC:
		return nil
	case "":
		return fmt.Errorf("local action manifest runtime is required")
	case RuntimeBuiltin:
		return fmt.Errorf("local action manifest runtime %q is reserved for builtins", runtime)
	default:
		return fmt.Errorf("local action manifest runtime %q is unsupported", runtime)
	}
}

func validateRuntimeConfig(config map[string]string) error {
	for key := range config {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("local action manifest runtime_config contains an empty key")
		}
	}

	return nil
}

func validateInputSchema(schema InputSchema) error {
	seen := map[string]struct{}{}
	for _, field := range schema.Fields {
		name := strings.TrimSpace(field.Name)
		if name == "" {
			return fmt.Errorf("local action manifest input_schema field name is required")
		}

		if name == "execution" {
			return fmt.Errorf("local action manifest input_schema field %q is reserved", name)
		}

		if _, exists := seen[name]; exists {
			return fmt.Errorf("local action manifest input_schema field %q is duplicated", name)
		}

		seen[name] = struct{}{}
		switch field.Type {
		case action.FieldString, action.FieldURL, action.FieldNumber:
		default:
			return fmt.Errorf("local action manifest input_schema field %q type %q is unsupported", name, field.Type)
		}
	}

	return nil
}

func validatePortSchema(schema []PortSpec) error {
	seen := map[string]struct{}{}
	primaryCount := 0
	for _, port := range schema {
		name := strings.TrimSpace(port.Name)
		if name == "" {
			return fmt.Errorf("local action manifest port_schema name is required")
		}

		if !referencePartRe.MatchString(name) {
			return fmt.Errorf("local action manifest port_schema name %q is invalid", name)
		}

		if _, exists := seen[name]; exists {
			return fmt.Errorf("local action manifest port_schema port %q is duplicated", name)
		}

		seen[name] = struct{}{}
		if port.Min < 0 {
			return fmt.Errorf("local action manifest port_schema port %q min must be >= 0", name)
		}

		if port.Max < action.PortUnlimited {
			return fmt.Errorf("local action manifest port_schema port %q max must be >= -1", name)
		}

		if port.Max >= 0 && port.Min > port.Max {
			return fmt.Errorf("local action manifest port_schema port %q min must be <= max", name)
		}

		if port.Primary {
			primaryCount++
		}
	}

	if primaryCount > 1 {
		return fmt.Errorf("local action manifest port_schema must contain at most one primary port")
	}

	return nil
}

func validateCapabilities(capabilities []Capability) error {
	known := map[Capability]struct{}{
		CapabilityProcessLaunch:  {},
		CapabilityNetwork:        {},
		CapabilityWorkspaceRead:  {},
		CapabilityWorkspaceWrite: {},
		CapabilitySecrets:        {},
	}

	seen := map[Capability]struct{}{}
	for _, capability := range capabilities {
		if _, ok := known[capability]; !ok {
			return fmt.Errorf("local action manifest capability %q is unsupported", capability)
		}

		if _, exists := seen[capability]; exists {
			return fmt.Errorf("local action manifest capability %q is duplicated", capability)
		}

		seen[capability] = struct{}{}
	}

	return nil
}

func cloneInputSchema(in InputSchema) InputSchema {
	out := in
	out.Fields = append([]InputField(nil), in.Fields...)
	return out
}
