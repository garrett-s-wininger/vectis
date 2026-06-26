package actionregistry

import (
	"fmt"

	"vectis/internal/action"
	sdkaction "vectis/sdk/action"
)

type SourceType = sdkaction.SourceType

const (
	SourceBuiltin         = sdkaction.SourceBuiltin
	SourceLocalFilesystem = sdkaction.SourceLocalFilesystem
	SourceOCI             = sdkaction.SourceOCI
)

type RuntimeType = sdkaction.RuntimeType

const (
	RuntimeBuiltin   = sdkaction.RuntimeBuiltin
	RuntimeProcess   = sdkaction.RuntimeProcess
	RuntimeContainer = sdkaction.RuntimeContainer
	RuntimeWASM      = sdkaction.RuntimeWASM
	RuntimeGRPC      = sdkaction.RuntimeGRPC
)

type DescriptorStatus = sdkaction.DescriptorStatus

const (
	DescriptorStatusActive  = sdkaction.DescriptorStatusActive
	DescriptorStatusYanked  = sdkaction.DescriptorStatusYanked
	DescriptorStatusRevoked = sdkaction.DescriptorStatusRevoked
	DescriptorStatusPurged  = sdkaction.DescriptorStatusPurged
)

type Capability = sdkaction.Capability

const (
	CapabilityProcessLaunch  = sdkaction.CapabilityProcessLaunch
	CapabilityNetwork        = sdkaction.CapabilityNetwork
	CapabilityWorkspaceRead  = sdkaction.CapabilityWorkspaceRead
	CapabilityWorkspaceWrite = sdkaction.CapabilityWorkspaceWrite
	CapabilitySecrets        = sdkaction.CapabilitySecrets
)

type Descriptor = sdkaction.Descriptor
type InputSchema = sdkaction.InputSchema
type InputField = sdkaction.InputField
type PortSpec = sdkaction.PortSpec

func DescriptorDigest(d Descriptor) (string, error) {
	return sdkaction.DescriptorDigest(d)
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
	case "builtins/gerrit-review":
		return "Gerrit Review"
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
	case "builtins/gerrit-review":
		return []Capability{CapabilityNetwork, CapabilityWorkspaceRead}
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
