package registry

import "strings"

const (
	MetadataCellID                   = "cell.id"
	MetadataQueueRole                = "queue.role"
	MetadataLogWriteState            = "log.write_state"
	MetadataWorkerExecutionBackend   = "worker.execution.backend"
	MetadataWorkerDefaultIsolation   = "worker.execution.default_isolation"
	MetadataWorkerSupportedIsolation = "worker.execution.supported_isolation"

	DefaultCellID         = "local"
	QueueRoleIngress      = "ingress"
	QueueRolePool         = "pool"
	LogWriteStateWritable = "writable"
	LogWriteStateReadOnly = "read_only"
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
	if defaultIsolation = strings.TrimSpace(defaultIsolation); defaultIsolation != "" {
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

func normalizeSupportedIsolation(levels []string) string {
	seen := make(map[string]struct{}, len(levels))
	normalized := make([]string, 0, len(levels))
	for _, level := range levels {
		level = strings.TrimSpace(level)
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
