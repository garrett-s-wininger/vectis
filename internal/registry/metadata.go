package registry

import "strings"

const (
	MetadataCellID    = "cell.id"
	MetadataQueueRole = "queue.role"

	DefaultCellID    = "local"
	QueueRoleIngress = "ingress"
	QueueRolePool    = "pool"
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

func normalizeMetadataCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return DefaultCellID
	}

	return cellID
}
