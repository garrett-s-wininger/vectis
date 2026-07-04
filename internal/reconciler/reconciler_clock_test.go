package reconciler

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/interfaces/mocks"
)

func advanceClockPastLastDispatch(t *testing.T, ctx context.Context, db *sql.DB, clock *mocks.MockClock, runID string, gap time.Duration) {
	t.Helper()

	var last sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&last); err != nil {
		t.Fatalf("query last_dispatched_at for %s: %v", runID, err)
	}

	if !last.Valid || last.Int64 == 0 {
		t.Fatalf("expected last_dispatched_at for %s before advancing clock, got %v", runID, last)
	}

	clock.SetNow(time.Unix(last.Int64, 0).Add(gap + time.Second).UTC())
}
