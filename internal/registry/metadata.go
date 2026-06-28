package registry

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

const (
	MetadataCellID                   = "cell.id"
	MetadataQueueRole                = "queue.role"
	MetadataLogWriteState            = "log.write_state"
	MetadataArtifactWriteState       = "artifact.write_state"
	MetadataWorkerExecutionBackend   = "worker.execution.backend"
	MetadataWorkerDefaultIsolation   = "worker.execution.default_isolation"
	MetadataWorkerSupportedIsolation = "worker.execution.supported_isolation"

	DefaultCellID              = "local"
	QueueRoleIngress           = "ingress"
	QueueRolePool              = "pool"
	LogWriteStateWritable      = "writable"
	LogWriteStateReadOnly      = "read_only"
	ArtifactWriteStateWritable = "writable"
	ArtifactWriteStateReadOnly = "read_only"
)

func DefaultServiceMetadataForCell(cellID string) map[string]string {
	return map[string]string{
		MetadataCellID: normalizeMetadataCellID(cellID),
	}
}

func DefaultServiceMetadata() map[string]string {
	return DefaultServiceMetadataForCell(DefaultCellID)
}

func QueueIngressMetadataForCell(cellID string) map[string]string {
	metadata := DefaultServiceMetadataForCell(cellID)
	metadata[MetadataQueueRole] = QueueRoleIngress
	return metadata
}

func QueueIngressMetadata() map[string]string {
	return QueueIngressMetadataForCell(DefaultCellID)
}

func WorkerExecutionMetadataForCell(cellID, backend, defaultIsolation string, supportedIsolation []string) map[string]string {
	metadata := DefaultServiceMetadataForCell(cellID)
	if backend = strings.TrimSpace(backend); backend != "" {
		metadata[MetadataWorkerExecutionBackend] = backend
	}
	if defaultIsolation = action.NormalizeIsolation(defaultIsolation); defaultIsolation != "" {
		metadata[MetadataWorkerDefaultIsolation] = defaultIsolation
	}
	if supported := normalizeSupportedIsolation(supportedIsolation); supported != "" {
		metadata[MetadataWorkerSupportedIsolation] = supported
	}

	return metadata
}

func normalizeMetadataCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return DefaultCellID
	}

	return cellID
}

func ValidateComponentMetadata(component api.Component, metadata map[string]string) error {
	if err := validateKnownMetadataOwners(component, metadata); err != nil {
		return err
	}

	if err := validateCellIDMetadata(metadata); err != nil {
		return err
	}

	switch component {
	case api.Component_COMPONENT_QUEUE:
		return validateQueueMetadata(metadata)
	case api.Component_COMPONENT_LOG:
		return validateWriteStateMetadata(metadata, MetadataLogWriteState, LogWriteStateWritable, LogWriteStateReadOnly)
	case api.Component_COMPONENT_ARTIFACT:
		return validateWriteStateMetadata(metadata, MetadataArtifactWriteState, ArtifactWriteStateWritable, ArtifactWriteStateReadOnly)
	case api.Component_COMPONENT_WORKER:
		return validateWorkerMetadata(metadata)
	case api.Component_COMPONENT_ORCHESTRATOR:
	case api.Component_COMPONENT_UNKNOWN:
		if len(metadata) > 0 {
			return fmt.Errorf("metadata is not supported for unknown component")
		}
	}

	return nil
}

func validateKnownMetadataOwners(component api.Component, metadata map[string]string) error {
	for key := range metadata {
		switch key {
		case MetadataQueueRole:
			if component != api.Component_COMPONENT_QUEUE {
				return fmt.Errorf("%s metadata is only valid for queue registrations", key)
			}
		case MetadataLogWriteState:
			if component != api.Component_COMPONENT_LOG {
				return fmt.Errorf("%s metadata is only valid for log registrations", key)
			}
		case MetadataArtifactWriteState:
			if component != api.Component_COMPONENT_ARTIFACT {
				return fmt.Errorf("%s metadata is only valid for artifact registrations", key)
			}
		case MetadataWorkerExecutionBackend, MetadataWorkerDefaultIsolation, MetadataWorkerSupportedIsolation:
			if component != api.Component_COMPONENT_WORKER {
				return fmt.Errorf("%s metadata is only valid for worker registrations", key)
			}
		}
	}

	return nil
}

func validateCellIDMetadata(metadata map[string]string) error {
	cellID, ok := metadata[MetadataCellID]
	if !ok {
		return nil
	}

	if strings.TrimSpace(cellID) == "" {
		return fmt.Errorf("%s metadata must not be empty", MetadataCellID)
	}

	if cellID != normalizeMetadataCellID(cellID) {
		return fmt.Errorf("%s metadata must be normalized", MetadataCellID)
	}

	return nil
}

func validateQueueMetadata(metadata map[string]string) error {
	role, ok := metadata[MetadataQueueRole]
	if !ok {
		return nil
	}

	switch role {
	case QueueRoleIngress, QueueRolePool:
		return nil
	default:
		return fmt.Errorf("%s metadata must be %q or %q", MetadataQueueRole, QueueRoleIngress, QueueRolePool)
	}
}

func validateWriteStateMetadata(metadata map[string]string, key, writable, readOnly string) error {
	state, ok := metadata[key]
	if !ok {
		return nil
	}

	switch state {
	case writable, readOnly:
		return nil
	default:
		return fmt.Errorf("%s metadata must be %q or %q", key, writable, readOnly)
	}
}

func validateWorkerMetadata(metadata map[string]string) error {
	if backend, ok := metadata[MetadataWorkerExecutionBackend]; ok && strings.TrimSpace(backend) == "" {
		return fmt.Errorf("%s metadata must not be empty", MetadataWorkerExecutionBackend)
	}

	defaultIsolation := ""
	if value, ok := metadata[MetadataWorkerDefaultIsolation]; ok {
		defaultIsolation = value
		if err := validateIsolationMetadataValue(MetadataWorkerDefaultIsolation, value); err != nil {
			return err
		}
	}

	supported := map[string]struct{}{}
	if value, ok := metadata[MetadataWorkerSupportedIsolation]; ok {
		levels, err := parseSupportedIsolationMetadata(value)
		if err != nil {
			return err
		}

		supported = levels
	}

	if defaultIsolation != "" && len(supported) > 0 {
		if _, ok := supported[defaultIsolation]; !ok {
			return fmt.Errorf("%s metadata %q must be present in %s", MetadataWorkerDefaultIsolation, defaultIsolation, MetadataWorkerSupportedIsolation)
		}
	}

	return nil
}

func validateIsolationMetadataValue(key, value string) error {
	if value == "" {
		return fmt.Errorf("%s metadata must not be empty", key)
	}

	if value != action.NormalizeIsolation(value) {
		return fmt.Errorf("%s metadata must be normalized", key)
	}

	if !action.IsSupportedIsolationLevel(value) {
		return fmt.Errorf("%s metadata has unsupported isolation %q", key, value)
	}

	return nil
}

func parseSupportedIsolationMetadata(value string) (map[string]struct{}, error) {
	if strings.TrimSpace(value) == "" {
		return nil, fmt.Errorf("%s metadata must not be empty", MetadataWorkerSupportedIsolation)
	}

	rawLevels := strings.Split(value, ",")
	levels, err := action.NormalizeSupportedIsolationLevels(rawLevels)
	if err != nil {
		return nil, fmt.Errorf("%s metadata: %w", MetadataWorkerSupportedIsolation, err)
	}

	if len(levels) != len(rawLevels) {
		return nil, fmt.Errorf("%s metadata must not contain duplicate isolation", MetadataWorkerSupportedIsolation)
	}

	if strings.Join(levels, ",") != value {
		return nil, fmt.Errorf("%s metadata must be normalized", MetadataWorkerSupportedIsolation)
	}

	seen := make(map[string]struct{}, len(levels))
	for _, level := range levels {
		seen[level] = struct{}{}
	}

	return seen, nil
}

func normalizeSupportedIsolation(levels []string) string {
	seen := make(map[string]struct{}, len(levels))
	normalized := make([]string, 0, len(levels))
	for _, level := range levels {
		level = action.NormalizeIsolation(level)
		if level == "" {
			continue
		}
		if _, ok := seen[level]; ok {
			continue
		}

		seen[level] = struct{}{}
		normalized = append(normalized, level)
	}

	return strings.Join(normalized, ",")
}
