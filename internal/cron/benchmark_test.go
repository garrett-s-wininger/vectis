package cron_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/cron"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

const cronBenchmarkJobDefinition = `{"id":"%s","root":{"uses":"builtins/shell","with":{"command":"true"}}}`

func BenchmarkCron_GetReadySchedules(b *testing.B) {
	for _, schedules := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("ready_%d", schedules), func(b *testing.B) {
			ctx := context.Background()
			db, repos := newCronBenchmarkDBAndRepos(b)
			now := cronBenchmarkNow()
			seedCronBenchmarkSchedules(b, ctx, db, repos, "bench-cron-ready", schedules, now)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ready, err := repos.Schedules().GetReady(ctx, now)
				if err != nil {
					b.Fatalf("get ready schedules: %v", err)
				}

				if len(ready) != schedules {
					b.Fatalf("ready=%d, want %d", len(ready), schedules)
				}
			}
		})
	}
}

func BenchmarkCron_ClaimCompleteSchedule(b *testing.B) {
	ctx := context.Background()
	db, repos := newCronBenchmarkDBAndRepos(b)
	now := cronBenchmarkNow()
	scheduleIDs := seedCronBenchmarkSchedules(b, ctx, db, repos, "bench-cron-claim-complete", 1, now)
	scheduleID := scheduleIDs[0]
	claimedUntil := now.Add(time.Minute)
	nextRun := now.Add(time.Minute)
	schedules := repos.Schedules()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		resetCronBenchmarkSchedule(b, ctx, db, scheduleID, now)
		b.StartTimer()

		claimToken := fmt.Sprintf("bench-claim-%d", i)
		claimed, err := schedules.ClaimDue(ctx, scheduleID, now, claimToken, claimedUntil, now)
		if err != nil {
			b.Fatalf("claim due schedule: %v", err)
		}

		if !claimed {
			b.Fatal("schedule was not claimed")
		}

		completed, err := schedules.CompleteClaim(ctx, scheduleID, claimToken, nextRun)
		if err != nil {
			b.Fatalf("complete schedule claim: %v", err)
		}

		if !completed {
			b.Fatal("schedule claim was not completed")
		}
	}
}

func BenchmarkCron_ProcessSchedules_AllDue(b *testing.B) {
	for _, schedules := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("ready_%d", schedules), func(b *testing.B) {
			ctx := context.Background()
			now := cronBenchmarkNow()

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, _ := newCronBenchmarkDBAndRepos(b)
				seedCronBenchmarkSchedules(b, ctx, db, dal.NewSQLRepositories(db), fmt.Sprintf("bench-cron-process-%d-%d", schedules, i), schedules, now)
				service := cron.NewCronService(mocks.NopLogger{}, db)
				service.SetClock(fixedCronBenchmarkClock{now: now})
				service.SetExecutionIngress(noopCronBenchmarkIngress{})
				service.SetInstanceID("bench-cron")
				b.StartTimer()

				if err := service.ProcessSchedules(ctx); err != nil {
					b.Fatalf("process schedules: %v", err)
				}

				b.StopTimer()
				_ = db.Close()
				b.StartTimer()
			}
		})
	}
}

type fixedCronBenchmarkClock struct {
	now time.Time
}

func (c fixedCronBenchmarkClock) Now() time.Time {
	return c.now
}

func (c fixedCronBenchmarkClock) Sleep(context.Context, time.Duration) error {
	return nil
}

type noopCronBenchmarkIngress struct{}

func (noopCronBenchmarkIngress) SubmitExecution(context.Context, cell.ExecutionSubmission) error {
	return nil
}

func newCronBenchmarkDBAndRepos(b *testing.B) (*sql.DB, *dal.SQLRepositories) {
	b.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}

	b.Cleanup(func() { _ = db.Close() })
	return db, dal.NewSQLRepositories(db)
}

func cronBenchmarkNow() time.Time {
	return time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
}

func seedCronBenchmarkSchedules(b *testing.B, ctx context.Context, db *sql.DB, repos *dal.SQLRepositories, jobID string, count int, nextRun time.Time) []int64 {
	b.Helper()

	definition := fmt.Sprintf(cronBenchmarkJobDefinition, jobID)
	if err := repos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		b.Fatalf("create benchmark cron job: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		b.Fatalf("begin schedule seed: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	scheduleIDs := make([]int64, 0, count)
	nextRunAt := nextRun.Format(time.RFC3339)
	for i := 0; i < count; i++ {
		triggerResult, err := tx.ExecContext(ctx,
			"INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)",
			jobID,
			dal.TriggerTypeCron,
		)

		if err != nil {
			b.Fatalf("insert benchmark cron trigger %d: %v", i, err)
		}

		triggerID, err := triggerResult.LastInsertId()
		if err != nil {
			b.Fatalf("benchmark cron trigger id: %v", err)
		}

		specResult, err := tx.ExecContext(ctx,
			"INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
			triggerID,
			"* * * * *",
			nextRunAt,
		)

		if err != nil {
			b.Fatalf("insert benchmark cron spec %d: %v", i, err)
		}

		scheduleID, err := specResult.LastInsertId()
		if err != nil {
			b.Fatalf("benchmark cron spec id: %v", err)
		}

		scheduleIDs = append(scheduleIDs, scheduleID)
	}

	if err := tx.Commit(); err != nil {
		b.Fatalf("commit schedule seed: %v", err)
	}

	return scheduleIDs
}

func resetCronBenchmarkSchedule(b *testing.B, ctx context.Context, db *sql.DB, scheduleID int64, nextRun time.Time) {
	b.Helper()

	if _, err := db.ExecContext(ctx, `
		UPDATE cron_trigger_specs
		SET next_run_at = ?,
			claim_token = NULL,
			claimed_until = NULL,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`, nextRun.Format(time.RFC3339), scheduleID); err != nil {
		b.Fatalf("reset benchmark cron schedule: %v", err)
	}
}

var _ cell.ExecutionIngress = noopCronBenchmarkIngress{}
