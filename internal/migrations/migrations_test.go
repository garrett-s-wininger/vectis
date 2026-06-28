package migrations_test

import (
	"database/sql"
	"fmt"
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
	assertTableExists(t, db, "reaction_events")
	assertTableExists(t, db, "reaction_targets")
	assertTableExists(t, db, "reaction_subscriptions")
	assertTableExists(t, db, "reaction_invocations")
	assertTableExists(t, db, "reaction_local_messages")
	assertTableExists(t, db, "run_artifacts")
	assertTableExists(t, db, "execution_security_events")
	assertTableExists(t, db, "run_segments")
	assertTableExists(t, db, "segment_executions")
	assertTableExists(t, db, "run_tasks")
	assertTableExists(t, db, "task_attempts")
	assertTableExists(t, db, "run_task_final_facts")
	assertTableExists(t, db, "cell_execution_acceptances")
	assertTableExists(t, db, "job_triggers")
	assertTableExists(t, db, "cron_trigger_specs")
	assertTableExists(t, db, "trigger_invocations")
	assertTableExists(t, db, "execution_payloads")
	assertTableExists(t, db, "service_leases")
	assertTableExists(t, db, "api_rate_limit_buckets")
	assertTableExists(t, db, "api_sessions")
	assertTableExists(t, db, "source_repositories")
	assertTableExists(t, db, "job_definition_sources")
	assertColumnExists(t, db, "job_runs", "namespace_path")
	for _, column := range []string{
		"checkout_mode",
		"authoring_mode",
		"sync_status",
		"last_sync_started_at_unix",
		"last_sync_finished_at_unix",
		"last_sync_ref",
		"last_sync_commit",
		"last_sync_error",
	} {
		assertColumnExists(t, db, "source_repositories", column)
	}
	assertSQLiteIndexColumns(t, db, "idx_run_artifacts_task", []string{"run_id", "task_id", "id"})
	assertSQLiteIndexColumns(t, db, "idx_run_artifacts_task_attempt", []string{"run_id", "task_attempt_id", "id"})
	assertSQLiteIndexColumns(t, db, "idx_run_artifacts_execution", []string{"run_id", "execution_id", "id"})
	assertSQLiteIndexColumns(t, db, "idx_reaction_targets_global_name", []string{"name"})
	assertSQLiteIndexColumns(t, db, "idx_reaction_subscriptions_global_name", []string{"name"})
	assertSQLiteForeignKeyTargetsExist(t, db)
	assertSQLiteSecurityConstraints(t, db)
	assertNamespaceExists(t, db, "/ephemeral")

	if err := migrations.Down(db, "sqlite3"); err != nil {
		t.Fatalf("run down migrations: %v", err)
	}

	assertTableMissing(t, db, "job_runs")
	assertTableMissing(t, db, "cron_schedule_fires")
	assertTableMissing(t, db, "run_dispatch_events")
	assertTableMissing(t, db, "reaction_events")
	assertTableMissing(t, db, "reaction_targets")
	assertTableMissing(t, db, "reaction_subscriptions")
	assertTableMissing(t, db, "reaction_invocations")
	assertTableMissing(t, db, "reaction_local_messages")
	assertTableMissing(t, db, "run_artifacts")
	assertTableMissing(t, db, "execution_security_events")
	assertTableMissing(t, db, "run_segments")
	assertTableMissing(t, db, "segment_executions")
	assertTableMissing(t, db, "run_tasks")
	assertTableMissing(t, db, "task_attempts")
	assertTableMissing(t, db, "run_task_final_facts")
	assertTableMissing(t, db, "cell_execution_acceptances")
	assertTableMissing(t, db, "job_triggers")
	assertTableMissing(t, db, "cron_trigger_specs")
	assertTableMissing(t, db, "trigger_invocations")
	assertTableMissing(t, db, "execution_payloads")
	assertTableMissing(t, db, "service_leases")
	assertTableMissing(t, db, "api_rate_limit_buckets")
	assertTableMissing(t, db, "api_sessions")
	assertTableMissing(t, db, "source_repositories")
	assertTableMissing(t, db, "job_definition_sources")
}

func assertNamespaceExists(t *testing.T, db *sql.DB, path string) {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM namespaces WHERE path = ?", path).Scan(&count); err != nil {
		t.Fatalf("query namespace %s: %v", path, err)
	}

	if count != 1 {
		t.Fatalf("expected namespace %s to exist once, got %d", path, count)
	}
}

func assertSQLiteSecurityConstraints(t *testing.T, db *sql.DB) {
	t.Helper()

	assertSQLiteConstraintRejects(t, db, `INSERT INTO namespaces (name, parent_id, path, break_inheritance) VALUES (?, ?, ?, ?)`, "bad-bool", 1, "/bad-bool", 2)
	assertSQLiteConstraintRejects(t, db, `INSERT INTO job_triggers (job_id, trigger_type, enabled) VALUES (?, ?, ?)`, "job-bad-enabled", "manual", 2)
	assertSQLiteConstraintRejects(t, db, `INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)`, "bad-enabled", "hash", 2)
	assertSQLiteConstraintRejects(t, db, `INSERT INTO source_repositories (repository_id, source_kind, checkout_path) VALUES (?, ?, ?)`, "bad-kind", "remote_git", "/work/bad-kind")
	assertSQLiteConstraintRejects(t, db, `INSERT INTO source_repositories (repository_id, source_kind, checkout_path, checkout_mode) VALUES (?, ?, ?, ?)`, "bad-checkout", "local_checkout", "/work/bad-checkout", "magic")
	assertSQLiteConstraintRejects(t, db, `INSERT INTO source_repositories (repository_id, source_kind, checkout_path, authoring_mode) VALUES (?, ?, ?, ?)`, "bad-authoring", "local_checkout", "/work/bad-authoring", "magic")
	assertSQLiteConstraintRejects(t, db, `INSERT INTO source_repositories (repository_id, source_kind, checkout_path, enabled) VALUES (?, ?, ?, ?)`, "bad-source-enabled", "local_checkout", "/work/bad-source-enabled", 2)
	assertSQLiteConstraintRejects(t, db, `INSERT INTO source_repositories (repository_id, source_kind, checkout_path, sync_status) VALUES (?, ?, ?, ?)`, "bad-sync-status", "local_checkout", "/work/bad-sync-status", "magic")

	res, err := db.Exec(`INSERT INTO local_users (username, password_hash) VALUES (?, ?)`, "constraint-user", "hash")
	if err != nil {
		t.Fatalf("insert constraint user: %v", err)
	}
	userID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("constraint user id: %v", err)
	}
	assertSQLiteConstraintRejects(t, db, `INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?)`, userID, 1, "owner")

	res, err = db.Exec(`INSERT INTO api_tokens (local_user_id, token_hash) VALUES (?, ?)`, userID, "constraint-token")
	if err != nil {
		t.Fatalf("insert constraint token: %v", err)
	}
	tokenID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("constraint token id: %v", err)
	}
	assertSQLiteConstraintRejects(t, db, `INSERT INTO api_token_scopes (api_token_id, action) VALUES (?, ?)`, tokenID, "setup:complete")
	assertSQLiteConstraintRejects(t, db, `INSERT INTO api_token_scopes (api_token_id, action, propagate) VALUES (?, ?, ?)`, tokenID, "job:read", 2)
}

func assertSQLiteConstraintRejects(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()

	if _, err := db.Exec(query, args...); err == nil {
		t.Fatalf("query unexpectedly satisfied security constraint: %s", query)
	}
}

func TestSQLiteMigrations_RunDeleteCascadesThroughTaskGraph(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	if err := migrations.Run(db, "sqlite3"); err != nil {
		t.Fatalf("run up migrations: %v", err)
	}

	statements := []string{
		`INSERT INTO job_runs (run_id, job_id, run_index, status, definition_hash) VALUES ('run-cascade', 'job-cascade', 1, 'queued', 'hash')`,
		`INSERT INTO run_tasks (task_id, run_id, task_key, name, status) VALUES ('task-cascade', 'run-cascade', 'root', 'root', 'pending')`,
		`INSERT INTO task_attempts (attempt_id, task_id, run_id, cell_id, status, attempt) VALUES ('attempt-cascade', 'task-cascade', 'run-cascade', 'local', 'pending', 1)`,
		`INSERT INTO run_segments (segment_id, run_id, name, status) VALUES ('segment-cascade', 'run-cascade', 'root', 'pending')`,
		`INSERT INTO segment_executions (execution_id, segment_id, run_id, task_id, task_attempt_id, cell_id, status, attempt) VALUES ('execution-cascade', 'segment-cascade', 'run-cascade', 'task-cascade', 'attempt-cascade', 'local', 'pending', 1)`,
		`DELETE FROM job_runs WHERE run_id = 'run-cascade'`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	for _, table := range []string{"run_tasks", "task_attempts", "run_segments", "segment_executions"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}

		if count != 0 {
			t.Fatalf("expected %s rows to cascade away, got %d", table, count)
		}
	}
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

func assertSQLiteForeignKeyTargetsExist(t *testing.T, db *sql.DB) {
	t.Helper()

	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
	if err != nil {
		t.Fatalf("list sqlite tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	existing := map[string]struct{}{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan sqlite table: %v", err)
		}

		tables = append(tables, table)
		existing[table] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite tables: %v", err)
	}

	for _, table := range tables {
		assertSQLiteForeignKeysReferenceExistingTables(t, db, table, existing)
	}
}

func assertSQLiteForeignKeysReferenceExistingTables(t *testing.T, db *sql.DB, table string, existing map[string]struct{}) {
	t.Helper()

	fkRows, err := db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s)", sqliteIdentifier(table)))
	if err != nil {
		t.Fatalf("list foreign keys for %s: %v", table, err)
	}

	defer func() {
		if err := fkRows.Close(); err != nil {
			t.Fatalf("close foreign keys for %s: %v", table, err)
		}
	}()

	for fkRows.Next() {
		var (
			id       int
			seq      int
			target   string
			from     string
			to       sql.NullString
			onUpdate string
			onDelete string
			match    string
		)

		if err := fkRows.Scan(&id, &seq, &target, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan foreign key for %s: %v", table, err)
		}

		if _, ok := existing[target]; !ok {
			t.Fatalf("table %s foreign key %d.%d references missing table %s", table, id, seq, target)
		}
	}

	if err := fkRows.Err(); err != nil {
		t.Fatalf("iterate foreign keys for %s: %v", table, err)
	}
}

func sqliteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func sortedMigrationVersions(versions map[string]map[string]string) []string {
	out := make([]string, 0, len(versions))
	for version := range versions {
		out = append(out, version)
	}

	sort.Strings(out)
	return out
}

func assertSQLiteIndexColumns(t *testing.T, db *sql.DB, indexName string, want []string) {
	t.Helper()

	rows, err := db.Query("PRAGMA index_info(" + indexName + ")")
	if err != nil {
		t.Fatalf("query index %s columns: %v", indexName, err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var seqno, cid int
		var name string
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			t.Fatalf("scan index %s column: %v", indexName, err)
		}
		got = append(got, name)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index %s columns: %v", indexName, err)
	}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("index %s columns = %v, want %v", indexName, got, want)
	}
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

func assertColumnExists(t *testing.T, db *sql.DB, table, column string) {
	t.Helper()

	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("read columns for %s: %v", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("scan column for %s: %v", table, err)
		}

		if name == column {
			return
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("read columns for %s: %v", table, err)
	}

	t.Fatalf("expected table %s to have column %s", table, column)
}
