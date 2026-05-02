package observability

import "go.opentelemetry.io/otel/attribute"

func JobRunAttrs(jobID, runID string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 4)
	if jobID != "" {
		attrs = append(attrs,
			attribute.String("job.id", jobID),
			attribute.String("vectis.job.id", jobID),
		)
	}

	if runID != "" {
		attrs = append(attrs,
			attribute.String("run.id", runID),
			attribute.String("vectis.run.id", runID),
		)
	}

	return attrs
}

func RunIndexAttrs(runIndex int) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int("run.index", runIndex),
		attribute.Int("vectis.run.index", runIndex),
	}
}

func DeliveryAttrs(deliveryID string) []attribute.KeyValue {
	if deliveryID == "" {
		return nil
	}

	return []attribute.KeyValue{
		attribute.String("queue.delivery.id", deliveryID),
		attribute.String("vectis.queue.delivery.id", deliveryID),
	}
}
