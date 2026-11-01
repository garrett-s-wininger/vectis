package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "vectis/api/gen/go"
	"vectis/internal/dispatchmeta"
)

func sameAcceptedRunPayload(frozen, incoming string) bool {
	var a, b pb.JobRequest
	if protojson.Unmarshal([]byte(frozen), &a) != nil || protojson.Unmarshal([]byte(incoming), &b) != nil {
		return false
	}

	if !normalizeAcceptedRunRequest(&a) || !normalizeAcceptedRunRequest(&b) {
		return false
	}

	return proto.Equal(&a, &b)
}

func normalizeAcceptedRunRequest(request *pb.JobRequest) bool {
	if request.GetJob() != nil {
		request.GetJob().DeliveryId = nil
	}

	if raw := request.GetMetadata()[dispatchmeta.ExecutionEnvelopeMetadataKey]; raw != "" {
		var envelope map[string]any
		decoder := json.NewDecoder(strings.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&envelope) != nil || decoder.Decode(new(any)) != io.EOF || envelope == nil {
			return false
		}

		for _, key := range []string{"root_execution_id", "task_id", "task_key", "task_name", "task_attempt_id", "task_attempt", "segment_id", "execution_id", "cell_id", "attempt", "start_deadline_unix_nano", "created_at_unix_nano"} {
			delete(envelope, key)
		}

		if metadata, ok := envelope["metadata"].(map[string]any); ok {
			for _, key := range acceptedDeliveryMetadataKeys {
				delete(metadata, key)
			}

			if len(metadata) == 0 {
				delete(envelope, "metadata")
			}
		}

		canonical, err := json.Marshal(envelope)
		if err != nil {
			return false
		}

		request.GetMetadata()[dispatchmeta.ExecutionEnvelopeMetadataKey] = string(canonical)
	}

	for _, key := range acceptedDeliveryMetadataKeys {
		delete(request.GetMetadata(), key)
	}

	return true
}

var acceptedDeliveryMetadataKeys = []string{
	dispatchmeta.ExecutionTaskKeyMetadataKey, dispatchmeta.ExecutionPayloadHashMetadataKey,
	dispatchmeta.StartDeadlineUnixNanoKey, "traceparent", "tracestate", "baggage",
}

func prepareAcceptedRunGenerationTx(ctx context.Context, tx *sql.Tx, a CellExecutionAcceptance) error {
	if err := lockRunForFinalizationTx(ctx, tx, a.RunID); err != nil {
		return err
	}

	var current int
	if err := tx.QueryRowContext(ctx, rebindQueryForPgx("SELECT COALESCE(MAX(attempt),0) FROM segment_executions WHERE run_id = ?"), a.RunID).Scan(&current); err != nil {
		return normalizeSQLError(err)
	}

	if current == 0 || a.Attempt <= current {
		return nil
	}

	if err := retireRootRetryAttemptsTx(ctx, tx, a.RunID); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, rebindQueryForPgx(`UPDATE job_runs SET status = ?, finished_at = NULL,
 failure_code = '', failure_reason = NULL, orphan_reason = '', lease_owner = NULL, lease_until = NULL,
 cancel_token = NULL, cancel_requested_at = NULL, cancel_reason = NULL, last_dispatched_at = NULL WHERE run_id = ?`), RunStatusQueued, a.RunID); err != nil {
		return normalizeSQLError(err)
	}

	for _, query := range []string{
		"DELETE FROM run_task_final_facts WHERE run_id = ?",
		"UPDATE run_tasks SET status = 'planned', updated_at = CURRENT_TIMESTAMP WHERE run_id = ?",
		"UPDATE run_segments SET status = 'planned', updated_at = CURRENT_TIMESTAMP WHERE run_id = ?",
	} {
		if _, err := tx.ExecContext(ctx, rebindQueryForPgx(query), a.RunID); err != nil {
			return normalizeSQLError(err)
		}
	}

	return clearRunHotStateOwnerTx(ctx, tx, a.RunID)
}
