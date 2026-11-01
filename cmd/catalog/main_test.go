package main

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/catalog"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/testutil/dbtest"
)

func TestOpenCatalogFanInSourcesOpensConfiguredCellDBs(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(database.EnvDatabaseDriver, "sqlite3")

	dir := t.TempDir()
	globalDB := filepath.Join(dir, "global.db")
	cellADB := filepath.Join(dir, "iad-a.db")
	cellBDB := filepath.Join(dir, "pdx-b.db")
	for _, dsn := range []string{globalDB, cellADB, cellBDB} {
		if err := database.Migrate(dsn); err != nil {
			t.Fatalf("migrate %s: %v", dsn, err)
		}
	}

	viper.Set("cell_database_dsns", []string{
		"pdx-b=" + cellBDB,
		"local=" + globalDB,
		"iad-a=" + cellADB,
	})

	sources, closeSources, err := openCatalogFanInSources(context.Background(), nil, globalDB)
	if err != nil {
		t.Fatalf("openCatalogFanInSources: %v", err)
	}
	defer closeSources()

	var got []string
	for _, source := range sources {
		got = append(got, source.CellID)
		if source.Events == nil {
			t.Fatalf("source %q has nil events repository", source.CellID)
		}

		if source.Backfill == nil {
			t.Fatalf("source %q has nil backfill processor", source.CellID)
		}
	}

	want := []string{"iad-a", "pdx-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sources: got %v, want %v", got, want)
	}
}

func TestCatalogBackfillUsesAuthoritativeDatabase(t *testing.T) {
	for _, separateCell := range []bool{false, true} {
		t.Run(map[bool]string{false: "shared database", true: "separate cell database"}[separateCell], func(t *testing.T) {
			ctx := context.Background()
			global := dal.NewSQLRepositories(dbtest.NewTestDB(t))
			runID, _, err := global.Runs().CreateRun(ctx, "job", nil, 1)
			if err != nil {
				t.Fatal(err)
			}

			if err := global.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning}); err != nil {
				t.Fatal(err)
			}

			var sources []catalog.FanInSource
			if separateCell {
				source := dal.NewSQLRepositories(dbtest.NewTestDB(t))
				if err := cell.NewCatalogEventPublisher("local", source.CatalogEvents()).RecordRunStatus(ctx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed}); err != nil {
					t.Fatal(err)
				}

				sources = []catalog.FanInSource{{CellID: "local", Events: source.CatalogEvents()}}
			}

			svc := catalog.NewService(nil, global.CatalogEvents(), global.Runs())
			svc.SetBackfill(globalCatalogBackfill("local", global, sources))
			svc.SetFanIn(catalog.NewFanInProcessor(global.CatalogEvents(), sources))
			result, err := svc.Process(ctx, 100)
			if err != nil || result.Applied != 1 || result.Read != 1 {
				t.Fatalf("expected only the authoritative event, result=%+v error=%v", result, err)
			}

			want := dal.RunStatusRunning
			if separateCell {
				want = dal.RunStatusFailed
			}

			status, _, err := global.Runs().GetRunStatus(ctx, runID)
			if err != nil || status != want {
				t.Fatalf("status=%s want=%s error=%v", status, want, err)
			}
		})
	}
}
