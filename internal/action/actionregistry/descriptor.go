package actionregistry

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"vectis/internal/action"
)

type SourceType string

const (
	SourceBuiltin         SourceType = "builtin"
	SourceLocalFilesystem SourceType = "local_filesystem"
	SourceOCI             SourceType = "oci"
)

type RuntimeType string

const (
	RuntimeBuiltin   RuntimeType = "builtin"
	RuntimeProcess   RuntimeType = "process"
	RuntimeContainer RuntimeType = "container"
	RuntimeWASM      RuntimeType = "wasm"
	RuntimeGRPC      RuntimeType = "grpc"
)

type DescriptorStatus string

const (
	DescriptorStatusActive  DescriptorStatus = "active"
	DescriptorStatusYanked  DescriptorStatus = "yanked"
	DescriptorStatusRevoked DescriptorStatus = "revoked"
	DescriptorStatusPurged  DescriptorStatus = "purged"
)

type Capability string

const (
	CapabilityProcessLaunch  Capability = "process_launch"
	CapabilityNetwork        Capability = "network"
	CapabilityWorkspaceRead  Capability = "workspace_read"
	CapabilityWorkspaceWrite Capability = "workspace_write"
	CapabilitySecrets        Capability = "secrets"
)

type Descriptor struct {
	CanonicalName string            `json:"canonical_name"`
	DisplayName   string            `json:"display_name,omitempty"`
	Version       string            `json:"version"`
	Digest        string            `json:"digest"`
	Source        SourceType        `json:"source"`
	SourcePath    string            `json:"-"`
	Runtime       RuntimeType       `json:"runtime"`
	RuntimeConfig map[string]string `json:"runtime_config,omitempty"`
	InputSchema   InputSchema       `json:"input_schema,omitempty"`
	PortSchema    []PortSpec        `json:"port_schema,omitempty"`
	LocalOnly     bool              `json:"local_only,omitempty"`
	Capabilities  []Capability      `json:"capabilities,omitempty"`
	Status        DescriptorStatus  `json:"status,omitempty"`
	StatusReason  string            `json:"status_reason,omitempty"`
}

type InputSchema struct {
	Fields       []InputField `json:"fields,omitempty"`
	AllowUnknown bool         `json:"allow_unknown,omitempty"`
}

type InputField struct {
	Name     string           `json:"name"`
	Type     action.FieldType `json:"type"`
	Required bool             `json:"required,omitempty"`
}

type PortSpec struct {
	Name     string `json:"name"`
	Min      int    `json:"min,omitempty"`
	Max      int    `json:"max,omitempty"`
	Primary  bool   `json:"primary,omitempty"`
	Ordered  bool   `json:"ordered,omitempty"`
	Required bool   `json:"required,omitempty"`
}

func (d Descriptor) ResolvedReference() string {
	if d.CanonicalName == "" || d.Digest == "" {
		return ""
	}

	return d.CanonicalName + "@" + d.Digest
}

func (d Descriptor) LifecycleStatus() DescriptorStatus {
	return NormalizeDescriptorStatus(d.Status)
}

func (d Descriptor) MatchReference(ref Reference) error {
	if d.CanonicalName != ref.CanonicalName() {
		return fmt.Errorf("action descriptor %q does not match reference %q", d.CanonicalName, ref.CanonicalName())
	}

	switch ref.SelectorKind {
	case SelectorNone:
		return nil
	case SelectorVersion:
		if d.Version == ref.Selector {
			return nil
		}

		return fmt.Errorf("action %q selector %q does not match resolved version %q", ref.CanonicalName(), ref.Selector, d.Version)
	case SelectorDigest:
		if d.Digest == ref.Selector {
			return nil
		}

		return fmt.Errorf("action %q digest %q does not match resolved digest %q", ref.CanonicalName(), ref.Selector, d.Digest)
	default:
		return fmt.Errorf("action %q has unsupported selector kind %q", ref.CanonicalName(), ref.SelectorKind)
	}
}

func (s InputSchema) ValidateWith(with map[string]string) []action.FieldError {
	specs := s.ActionFieldSpecs()
	if len(specs) == 0 {
		if s.AllowUnknown {
			return nil
		}

		errs := make([]action.FieldError, 0, len(with))
		for key := range with {
			errs = append(errs, action.FieldError{Field: key, Message: fmt.Sprintf("unknown field %q", key)})
		}

		sortFieldErrors(errs)
		return errs
	}

	if !s.AllowUnknown {
		return action.ValidateWithSpec(with, specs)
	}

	known := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		known[spec.Name] = struct{}{}
	}

	filtered := make(map[string]string, len(specs))
	for key, value := range with {
		if _, ok := known[key]; ok {
			filtered[key] = value
		}
	}

	return action.ValidateWithSpec(filtered, specs)
}

func (s InputSchema) ActionFieldSpecs() []action.FieldSpec {
	specs := make([]action.FieldSpec, 0, len(s.Fields))
	for _, field := range s.Fields {
		specs = append(specs, action.FieldSpec{
			Name:     field.Name,
			Type:     field.Type,
			Required: field.Required,
		})
	}

	return specs
}

func (p PortSpec) ActionPortSpec() action.PortSpec {
	return action.PortSpec{
		Name:     p.Name,
		Min:      p.Min,
		Max:      p.Max,
		Primary:  p.Primary,
		Ordered:  p.Ordered,
		Required: p.Required,
	}
}

func DescriptorFromNode(node action.Node, source SourceType, runtime RuntimeType) (Descriptor, error) {
	if node == nil {
		return Descriptor{}, fmt.Errorf("action node is required")
	}

	descriptor := Descriptor{
		CanonicalName: node.Type(),
		DisplayName:   BuiltinDisplayName(node.Type()),
		Version:       "v1",
		Source:        source,
		Runtime:       runtime,
		InputSchema:   inputSchemaFromAction(action.InputSchema(node)),
		PortSchema:    portSchemaFromAction(action.PortSchema(node)),
		LocalOnly:     action.LocalOnly(node),
		Capabilities:  BuiltinCapabilities(node.Type()),
	}

	digest, err := DescriptorDigest(descriptor)
	if err != nil {
		return Descriptor{}, err
	}

	descriptor.Digest = digest
	return descriptor, nil
}

func DescriptorDigest(d Descriptor) (string, error) {
	payload := descriptorDigestPayload{
		CanonicalName: d.CanonicalName,
		Version:       d.Version,
		Source:        d.Source,
		Runtime:       d.Runtime,
		RuntimeConfig: cloneStringMap(d.RuntimeConfig),
		InputSchema:   d.InputSchema,
		PortSchema:    append([]PortSpec(nil), d.PortSchema...),
		LocalOnly:     d.LocalOnly,
		Capabilities:  append([]Capability(nil), d.Capabilities...),
	}

	sort.Slice(payload.Capabilities, func(i, j int) bool {
		return payload.Capabilities[i] < payload.Capabilities[j]
	})

	sort.Slice(payload.PortSchema, func(i, j int) bool {
		return payload.PortSchema[i].Name < payload.PortSchema[j].Name
	})

	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal action descriptor: %w", err)
	}

	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

type descriptorDigestPayload struct {
	CanonicalName string
	Version       string
	Source        SourceType
	Runtime       RuntimeType
	RuntimeConfig map[string]string `json:"RuntimeConfig,omitempty"`
	InputSchema   InputSchema
	PortSchema    []PortSpec
	LocalOnly     bool
	Capabilities  []Capability
}

func inputSchemaFromAction(specs []action.FieldSpec) InputSchema {
	if len(specs) == 0 {
		return InputSchema{AllowUnknown: true}
	}

	fields := make([]InputField, 0, len(specs))
	for _, spec := range specs {
		fields = append(fields, InputField{
			Name:     spec.Name,
			Type:     spec.Type,
			Required: spec.Required,
		})
	}

	return InputSchema{Fields: fields}
}

func portSchemaFromAction(specs []action.PortSpec) []PortSpec {
	if len(specs) == 0 {
		return nil
	}

	out := make([]PortSpec, 0, len(specs))
	for _, spec := range specs {
		out = append(out, PortSpec{
			Name:     spec.Name,
			Min:      spec.Min,
			Max:      spec.Max,
			Primary:  spec.Primary,
			Ordered:  spec.Ordered,
			Required: spec.Required,
		})
	}

	return out
}

func BuiltinDisplayName(actionType string) string {
	switch actionType {
	case "builtins/script":
		return "Script"
	case "builtins/test":
		return "Test"
	case "builtins/checkout":
		return "Checkout"
	case "builtins/sequence":
		return "Sequence"
	case "builtins/parallel":
		return "Parallel"
	case "builtins/if":
		return "If"
	case "builtins/retry":
		return "Retry"
	case "builtins/timeout":
		return "Timeout"
	case "builtins/finally":
		return "Finally"
	case "builtins/fallback":
		return "Fallback"
	default:
		return actionType
	}
}

func BuiltinCapabilities(actionType string) []Capability {
	switch actionType {
	case "builtins/script", "builtins/test":
		return []Capability{CapabilityProcessLaunch, CapabilityWorkspaceRead, CapabilityWorkspaceWrite}
	case "builtins/checkout":
		return []Capability{CapabilityNetwork, CapabilityProcessLaunch, CapabilityWorkspaceWrite}
	default:
		return nil
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

func sortFieldErrors(errs []action.FieldError) {
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].Field < errs[j].Field
	})
}
