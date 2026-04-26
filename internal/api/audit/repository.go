package audit

import (
	"context"
	"database/sql"
	"encoding/json"

	"vectis/internal/dal"
)

// DALRepository adapts the DAL auth repository to the audit Repository interface.
type DALRepository struct {
	Auth dal.AuthRepository
}

func (r *DALRepository) InsertAuditEvents(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	records := make([]*dal.AuditEventRecord, len(events))
	for i, event := range events {
		rec := &dal.AuditEventRecord{
			Type:          event.Type,
			IPAddress:     event.IPAddress,
			CorrelationID: event.CorrelationID,
		}
		if !event.Timestamp.IsZero() {
			rec.CreatedAt = sql.NullTime{Time: event.Timestamp, Valid: true}
		}

		if event.ActorID > 0 {
			rec.ActorID = sql.NullInt64{Int64: event.ActorID, Valid: true}
		}

		if event.TargetID > 0 {
			rec.TargetID = sql.NullInt64{Int64: event.TargetID, Valid: true}
		}

		if len(event.Metadata) > 0 {
			data, err := json.Marshal(event.Metadata)
			if err != nil {
				return err
			}

			rec.Metadata = data
		}

		records[i] = rec
	}

	return r.Auth.InsertAuditEvents(ctx, records)
}
