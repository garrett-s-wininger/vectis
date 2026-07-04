package observability

import (
	"context"
	"fmt"

	"vectis/internal/artifact"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const maxInt64GaugeValue = uint64(1<<63 - 1)

type artifactStorageStatsProvider interface {
	StorageStats(context.Context) (artifact.StorageStats, error)
}

func RegisterArtifactStorageMetrics(store artifactStorageStatsProvider) error {
	if store == nil {
		return fmt.Errorf("RegisterArtifactStorageMetrics: store is nil")
	}

	m := otel.Meter("vectis/artifact")
	blobsG, err := m.Int64ObservableGauge("vectis_artifact_storage_blobs",
		metric.WithDescription("Content-addressed artifact blob files stored by this shard"),
		metric.WithUnit("{blob}"))
	if err != nil {
		return fmt.Errorf("vectis_artifact_storage_blobs: %w", err)
	}

	bytesG, err := m.Int64ObservableGauge("vectis_artifact_storage_bytes",
		metric.WithDescription("Content-addressed artifact blob bytes stored by this shard"),
		metric.WithUnit("By"))
	if err != nil {
		return fmt.Errorf("vectis_artifact_storage_bytes: %w", err)
	}

	freeBytesG, err := m.Int64ObservableGauge("vectis_artifact_storage_free_bytes",
		metric.WithDescription("Filesystem bytes available to artifact storage"),
		metric.WithUnit("By"))
	if err != nil {
		return fmt.Errorf("vectis_artifact_storage_free_bytes: %w", err)
	}

	freeInodesG, err := m.Int64ObservableGauge("vectis_artifact_storage_free_inodes",
		metric.WithDescription("Filesystem inodes available to artifact storage"),
		metric.WithUnit("{inode}"))
	if err != nil {
		return fmt.Errorf("vectis_artifact_storage_free_inodes: %w", err)
	}

	writableG, err := m.Int64ObservableGauge("vectis_artifact_storage_new_blob_writable",
		metric.WithDescription("Whether this shard currently accepts new artifact blobs: 1 writable, 0 read-only"),
		metric.WithUnit("{state}"))
	if err != nil {
		return fmt.Errorf("vectis_artifact_storage_new_blob_writable: %w", err)
	}

	_, err = m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		stats, err := store.StorageStats(ctx)
		if err != nil {
			return err
		}

		o.ObserveInt64(blobsG, stats.BlobFiles)
		o.ObserveInt64(bytesG, stats.BlobBytes)
		if stats.FreeBytesKnown {
			o.ObserveInt64(freeBytesG, int64FromUint64ForGauge(stats.FreeBytes))
		}

		if stats.FreeInodesKnown {
			o.ObserveInt64(freeInodesG, int64FromUint64ForGauge(stats.FreeInodes))
		}

		writable := int64(0)
		if stats.NewBlobWritable {
			writable = 1
		}
		o.ObserveInt64(writableG, writable)
		return nil
	}, blobsG, bytesG, freeBytesG, freeInodesG, writableG)
	if err != nil {
		return fmt.Errorf("register artifact storage metrics callback: %w", err)
	}

	return nil
}

func int64FromUint64ForGauge(v uint64) int64 {
	if v > maxInt64GaugeValue {
		return int64(maxInt64GaugeValue)
	}
	return int64(v)
}
