package migrations_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"vectis/internal/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func TestMigrationBackendsHaveMatchingVersions(t *testing.T) {
	sqlite := readMigrationVersions(t, "sqlite")
	postgres := readMigrationVersions(t, "postgres")

	assertSameMigrationVersions(t, "sqlite", sqlite, "postgres", postgres)
}

func TestSQLiteMigrations_UpDownRoundTrip(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := migrations.Run(db, "sqlite3"); err != nil {
		t.Fatalf("run up migrations: %v", err)
	}

	assertTableExists(t, db, "job_runs")
	assertTableExists(t, db, "cron_schedule_fires")
	assertTableExists(t, db, "run_dispatch_events")
	assertTableExists(t, db, "run_segments")
	assertTableExists(t, db, "segment_executions")
	assertTableExists(t, db, "cell_execution_acceptances")
	assertTableExists(t, db, "job_triggers")
	assertTableExists(t, db, "cron_trigger_specs")
	assertTableExists(t, db, "trigger_invocations")
	assertTableExists(t, db, "execution_payloads")
	assertTableExists(t, db, "service_leases")

	if err := migrations.Down(db, "sqlite3"); err != nil {
		t.Fatalf("run down migrations: %v", err)
	}

	assertTableMissing(t, db, "job_runs")
	assertTableMissing(t, db, "cron_schedule_fires")
	assertTableMissing(t, db, "run_dispatch_events")
	assertTableMissing(t, db, "run_segments")
	assertTableMissing(t, db, "segment_executions")
	assertTableMissing(t, db, "cell_execution_acceptances")
	assertTableMissing(t, db, "job_triggers")
	assertTableMissing(t, db, "cron_trigger_specs")
	assertTableMissing(t, db, "trigger_invocations")
	assertTableMissing(t, db, "execution_payloads")
	assertTableMissing(t, db, "service_leases")
}

func readMigrationVersions(t *testing.T, backend string) map[string]map[string]string {
	t.Helper()

	entries, err := os.ReadDir(filepath.Join(backend))
	if err != nil {
		t.Fatalf("read %s migrations: %v", backend, err)
	}

	versions := map[string]map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		version, direction, ok := splitMigrationFilename(entry.Name())
		if !ok {
			t.Fatalf("%s migration %q does not follow NNN_name.(up|down).sql", backend, entry.Name())
		}

		if versions[version] == nil {
			versions[version] = map[string]string{}
		}
		if existing := versions[version][direction]; existing != "" {
			t.Fatalf("%s migration version %s has duplicate %s files: %s and %s", backend, version, direction, existing, entry.Name())
		}
		versions[version][direction] = entry.Name()
	}

	for version, files := range versions {
		for _, direction := range []string{"up", "down"} {
			if files[direction] == "" {
				t.Fatalf("%s migration version %s is missing %s migration", backend, version, direction)
			}
		}
	}

	return versions
}

func splitMigrationFilename(name string) (version, direction string, ok bool) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", "", false
	}

	if strings.HasSuffix(name, ".up.sql") {
		return parts[0], "up", true
	}

	if strings.HasSuffix(name, ".down.sql") {
		return parts[0], "down", true
	}

	return "", "", false
}

func assertSameMigrationVersions(t *testing.T, leftName string, left map[string]map[string]string, rightName string, right map[string]map[string]string) {
	t.Helper()

	for _, version := range sortedMigrationVersions(left) {
		if right[version] == nil {
			t.Fatalf("%s has migration version %s, but %s does not", leftName, version, rightName)
		}
	}

	for _, version := range sortedMigrationVersions(right) {
		if left[version] == nil {
			t.Fatalf("%s has migration version %s, but %s does not", rightName, version, leftName)
		}
	}
}

func sortedMigrationVersions(versions map[string]map[string]string) []string {
	out := make([]string, 0, len(versions))
	for version := range versions {
		out = append(out, version)
	}

	sort.Strings(out)
	return out
}

func assertTableExists(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err != nil {
		t.Fatalf("expected table %s to exist: %v", table, err)
	}
}

func assertTableMissing(t *testing.T, db *sql.DB, table string) {
	t.Helper()

	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if err == nil {
		t.Fatalf("expected table %s to be dropped, found %s", table, name)
	}

	if err != sql.ErrNoRows {
		t.Fatalf("query table %s: %v", table, err)
	}
}
