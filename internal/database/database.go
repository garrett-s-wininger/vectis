package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/migrations"
	"vectis/internal/utils"
)

const (
	EnvDatabaseDriver    = "VECTIS_DATABASE_DRIVER"
	EnvDatabaseDSN       = "VECTIS_DATABASE_DSN"
	EnvGlobalDatabaseDSN = "VECTIS_GLOBAL_DATABASE_DSN"
	EnvCellDatabaseDSN   = "VECTIS_CELL_DATABASE_DSN"

	postgresMigrationAdvisoryLockKey int64 = 987654321

	postgresMigrationConnectDeadline = 2 * time.Minute
	postgresMigrationConnectTimeout  = 5 * time.Second
	postgresMigrationRetryDeadline   = 60 * time.Second
	postgresMigrationLockTimeout     = 10 * time.Second
	postgresMigrationRetrySleep      = 750 * time.Millisecond

	schemaWaitDeadline     = 5 * time.Minute
	schemaWaitPollInterval = 750 * time.Millisecond

	sqliteBusyTimeoutMillis = 10000
	sqliteDataDirPerm       = 0o700
	sqliteDataFilePerm      = 0o600
)

type Role string

const (
	RoleDefault Role = ""
	RoleGlobal  Role = "global"
	RoleCell    Role = "cell"
)

func GetDBPath() string {
	if dsn := expandDSN(os.Getenv(EnvDatabaseDSN)); dsn != "" {
		return dsn
	}

	dataHome := utils.DataHome()
	return config.DBDSN(dataHome)
}

func GetDBPathForRole(role Role) string {
	switch role {
	case RoleGlobal:
		if dsn := expandDSN(os.Getenv(EnvGlobalDatabaseDSN)); dsn != "" {
			return dsn
		}
	case RoleCell:
		if dsn := expandDSN(os.Getenv(EnvCellDatabaseDSN)); dsn != "" {
			return dsn
		}
	case RoleDefault:
	}

	return GetDBPath()
}

func GlobalAndCellDatabasesAreSplit() bool {
	return GetDBPathForRole(RoleGlobal) != GetDBPathForRole(RoleCell)
}

func expandDSN(dsn string) string {
	if dsn = strings.TrimSpace(dsn); dsn != "" {
		return strings.NewReplacer("{{data_home}}", utils.DataHome()).Replace(dsn)
	}

	return ""
}

func OpenDB(dbPath string) (*sql.DB, error) {
	driver := EffectiveDBDriver()
	switch driver {
	case "sqlite3":
		if err := prepareSQLiteDataFile(dbPath); err != nil {
			return nil, err
		}

		dbPath = sqliteDSNWithDefaults(dbPath)
	case "pgx":
		var err error
		dbPath, err = pgxDSNWithDefaults(dbPath)
		if err != nil {
			return nil, err
		}
	}

	db, err := sql.Open(driver, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if driver == "pgx" {
		if err := applyPgxPoolSettings(db); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("configure pgx pool: %w", err)
		}
	}

	return db, nil
}

func pgxDSNWithDefaults(dsn string) (string, error) {
	planCacheMode := config.DatabasePgxPlanCacheMode()
	if planCacheMode == "" {
		return dsn, nil
	}

	switch planCacheMode {
	case "auto", "force_custom_plan", "force_generic_plan":
	default:
		return "", fmt.Errorf("database.pgx.plan_cache_mode must be one of auto, force_custom_plan, or force_generic_plan (got %q)", planCacheMode)
	}

	return pgxDSNWithParam(dsn, "plan_cache_mode", planCacheMode), nil
}

func pgxDSNWithParam(dsn, key, value string) string {
	if pgxDSNHasParam(dsn, key) {
		return dsn
	}

	if parsed, err := url.Parse(dsn); err == nil && parsed.Scheme != "" {
		query := parsed.Query()
		query.Set(key, value)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}

	sep := " "
	if strings.TrimSpace(dsn) == "" {
		sep = ""
	}
	return dsn + sep + key + "=" + value
}

func pgxDSNHasParam(dsn, key string) bool {
	if parsed, err := url.Parse(dsn); err == nil && parsed.Scheme != "" {
		_, ok := parsed.Query()[key]
		return ok
	}

	for field := range strings.FieldsSeq(dsn) {
		name, _, _ := strings.Cut(field, "=")
		if name == key {
			return true
		}
	}

	return false
}

func sqliteDSNWithDefaults(dsn string) string {
	if dsn == "" || dsn == ":memory:" {
		return dsn
	}

	if !sqliteDSNHasAnyParam(dsn, "_foreign_keys", "_fk") {
		dsn = sqliteDSNWithParam(dsn, "_foreign_keys", "on")
	}
	dsn = sqliteDSNWithParam(dsn, "_busy_timeout", strconv.Itoa(sqliteBusyTimeoutMillis))
	dsn = sqliteDSNWithParam(dsn, "_journal_mode", "WAL")
	return sqliteDSNWithParam(dsn, "_txlock", "immediate")
}

func prepareSQLiteDataFile(dsn string) error {
	path, ok := sqliteFilesystemPath(dsn)
	if !ok {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, sqliteDataDirPerm); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	mode := sqliteDSNMode(dsn)
	if mode == "ro" || mode == "rw" {
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to inspect sqlite database file: %w", err)
		}
		return chmodSQLiteDataFile(path)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, sqliteDataFilePerm)
	if err != nil {
		return fmt.Errorf("failed to create sqlite database file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close sqlite database file: %w", err)
	}

	return chmodSQLiteDataFile(path)
}

func chmodSQLiteDataFile(path string) error {
	if err := os.Chmod(path, sqliteDataFilePerm); err != nil {
		return fmt.Errorf("failed to set sqlite database file permissions: %w", err)
	}

	return nil
}

func sqliteFilesystemPath(dsn string) (string, bool) {
	raw := strings.TrimSpace(dsn)
	if raw == "" || raw == ":memory:" || sqliteDSNMode(raw) == "memory" {
		return "", false
	}

	path, _, _ := strings.Cut(raw, "?")
	if !strings.HasPrefix(path, "file:") {
		return path, true
	}

	rest := strings.TrimPrefix(path, "file:")
	if rest == "" || rest == ":memory:" {
		return "", false
	}

	if !strings.HasPrefix(rest, "//") {
		return rest, true
	}

	parsed, err := url.Parse(path)
	if err != nil || parsed.Path == "" {
		return "", false
	}
	if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
		return "", false
	}

	return parsed.Path, true
}

func sqliteDSNWithParam(dsn, key, value string) string {
	if sqliteDSNHasParam(dsn, key) {
		return dsn
	}

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}

	return dsn + sep + url.QueryEscape(key) + "=" + url.QueryEscape(value)
}

func sqliteDSNHasAnyParam(dsn string, keys ...string) bool {
	for _, key := range keys {
		if sqliteDSNHasParam(dsn, key) {
			return true
		}
	}

	return false
}

func sqliteDSNHasParam(dsn, key string) bool {
	_, rawQuery, ok := strings.Cut(dsn, "?")
	if !ok {
		return false
	}

	values, err := url.ParseQuery(rawQuery)
	if err == nil {
		_, ok = values[key]
		return ok
	}

	for part := range strings.SplitSeq(rawQuery, "&") {
		name, _, _ := strings.Cut(part, "=")
		if name == key {
			return true
		}
	}

	return false
}

func sqliteDSNMode(dsn string) string {
	_, rawQuery, ok := strings.Cut(dsn, "?")
	if !ok {
		return ""
	}

	values, err := url.ParseQuery(rawQuery)
	if err == nil {
		return strings.ToLower(strings.TrimSpace(values.Get("mode")))
	}

	for part := range strings.SplitSeq(rawQuery, "&") {
		name, value, _ := strings.Cut(part, "=")
		if name == "mode" {
			return strings.ToLower(strings.TrimSpace(value))
		}
	}

	return ""
}

func OpenReadyDB(log interfaces.Logger) (*sql.DB, string, error) {
	return OpenReadyDBForRole(log, RoleDefault)
}

func OpenReadyDBForRole(log interfaces.Logger, role Role) (*sql.DB, string, error) {
	dbPath := GetDBPathForRole(role)
	if log != nil {
		log.Info("Using database: %s", dbPath)
	}

	db, err := OpenDB(dbPath)
	if err != nil {
		return nil, dbPath, err
	}

	if err := WaitForMigrations(db, log); err != nil {
		_ = db.Close()
		return nil, dbPath, err
	}

	return db, dbPath, nil
}

func EffectiveDBDriver() string {
	if driver := os.Getenv(EnvDatabaseDriver); driver != "" {
		return driver
	}

	return config.DBDriver()
}

func Migrate(dbPath string) error {
	db, err := OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	return MigrateDB(db)
}

func MigrateDB(db *sql.DB) error {
	driver := EffectiveDBDriver()

	if driver == "pgx" {
		if err := waitForMigrationDB(db); err != nil {
			return err
		}

		deadline := time.Now().Add(postgresMigrationRetryDeadline)
		var lastErr error

		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), postgresMigrationLockTimeout)
			lastErr = migrateWithLock(ctx, db, postgresMigrationAdvisoryLockKey, driver)
			cancel()

			if lastErr == nil {
				return nil
			}

			time.Sleep(postgresMigrationRetrySleep)
		}

		return fmt.Errorf("failed to run migrations (postgres): %w", lastErr)
	}

	if err := migrations.Run(db, driver); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

func waitForMigrationDB(db *sql.DB) error {
	deadline := time.Now().Add(postgresMigrationConnectDeadline)
	var lastErr error

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), postgresMigrationConnectTimeout)
		lastErr = db.PingContext(ctx)
		cancel()

		if lastErr == nil {
			return nil
		}

		time.Sleep(postgresMigrationRetrySleep)
	}

	if schemaWaitConnectFailure(lastErr) {
		return fmt.Errorf("database did not become reachable for migrations within %s: %w", postgresMigrationConnectDeadline, lastErr)
	}

	return fmt.Errorf("database readiness check failed before migrations: %w", lastErr)
}

func migrateWithLock(ctx context.Context, db *sql.DB, advisoryLockKey int64, driver string) error {
	if _, err := db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", advisoryLockKey) }()

	return migrations.Run(db, driver)
}

func WaitForMigrations(db *sql.DB, log interfaces.Logger) error {
	driver := EffectiveDBDriver()
	switch driver {
	case "pgx", "sqlite3":
	default:
		return nil
	}

	deadline := time.Now().Add(schemaWaitDeadline)
	var warned bool
	var hadFailure bool

	for time.Now().Before(deadline) {
		err := schemaMigrationsReady(db)
		if err == nil {
			if hadFailure && log != nil {
				log.Info("Database schema is ready")
			}

			return nil
		}

		hadFailure = true
		if log != nil {
			if !warned {
				if schemaWaitConnectFailure(err) {
					log.Warn("Cannot connect to the database (retries at debug; check DSN, network/DNS, and credentials)")
				} else {
					log.Warn("Database is reachable but schema is not ready (retries at debug; apply migrations, e.g. vectis-cli database migrate)")
				}

				log.Debug("database schema poll error: %v", err)
				warned = true
			} else {
				log.Debug("database schema still not ready: %v", err)
			}
		}

		time.Sleep(schemaWaitPollInterval)
	}

	return fmt.Errorf("timed out waiting for database readiness; check connectivity and apply migrations with vectis-cli database migrate (same VECTIS_DATABASE_DRIVER / VECTIS_DATABASE_DSN)")
}

func schemaMigrationsReady(db *sql.DB) error {
	var dirty bool
	if err := db.QueryRowContext(context.Background(), "SELECT dirty FROM schema_migrations ORDER BY version DESC LIMIT 1").Scan(&dirty); err != nil {
		return err
	}

	if dirty {
		return fmt.Errorf("database migration version is dirty")
	}

	return nil
}
