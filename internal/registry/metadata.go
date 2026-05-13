package registry

const (
	MetadataCellID    = "cell.id"
	MetadataQueueRole = "queue.role"

	DefaultCellID    = "local"
	QueueRoleIngress = "ingress"
	QueueRolePool    = "pool"
)

func DefaultServiceMetadata() map[string]string {
	return map[string]string{
		MetadataCellID: DefaultCellID,
	}
}

func QueueIngressMetadata() map[string]string {
	metadata := DefaultServiceMetadata()
	metadata[MetadataQueueRole] = QueueRoleIngress
	return metadata
}
