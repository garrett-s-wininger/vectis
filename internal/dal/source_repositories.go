package dal

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type SQLSourcesRepository struct {
	db *sql.DB
}

func (r *SQLSourcesRepository) CreateRepository(ctx context.Context, rec SourceRepositoryRecord) (SourceRepositoryRecord, error) {
	rec, err := normalizeSourceRepositoryRecord(rec)
	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	var id int64
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		INSERT INTO source_repositories (
			global_id,
			repository_id,
			namespace_id,
			source_kind,
			checkout_path,
			checkout_mode,
			canonical_url,
			default_ref,
			credential_ref,
			enabled
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`),
		newGlobalID(),
		rec.RepositoryID,
		rec.NamespaceID,
		rec.SourceKind,
		rec.CheckoutPath,
		rec.CheckoutMode,
		rec.CanonicalURL,
		rec.DefaultRef,
		rec.CredentialRef,
		rec.Enabled,
	).Scan(&id); err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	return r.GetRepository(ctx, rec.RepositoryID)
}

func (r *SQLSourcesRepository) UpdateRepository(ctx context.Context, rec SourceRepositoryRecord) (SourceRepositoryRecord, error) {
	rec, err := normalizeSourceRepositoryRecord(rec)
	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE source_repositories
		SET
			source_kind = ?,
			checkout_path = ?,
			checkout_mode = ?,
			canonical_url = ?,
			default_ref = ?,
			credential_ref = ?,
			enabled = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE repository_id = ?
	`),
		rec.SourceKind,
		rec.CheckoutPath,
		rec.CheckoutMode,
		rec.CanonicalURL,
		rec.DefaultRef,
		rec.CredentialRef,
		rec.Enabled,
		rec.RepositoryID,
	)

	if err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: source repository %s", ErrNotFound, rec.RepositoryID)
	}

	return r.GetRepository(ctx, rec.RepositoryID)
}

func (r *SQLSourcesRepository) BeginRepositorySync(ctx context.Context, rec SourceRepositorySyncRecord) (SourceRepositoryRecord, bool, error) {
	rec.Status = SourceSyncStatusRunning
	rec.FinishedAtUnix = 0
	rec.Commit = ""
	rec.Error = ""

	rec, err := normalizeSourceRepositorySyncRecord(rec)
	if err != nil {
		return SourceRepositoryRecord{}, false, err
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE source_repositories
		SET
			sync_status = ?,
			last_sync_started_at_unix = ?,
			last_sync_finished_at_unix = 0,
			last_sync_ref = ?,
			last_sync_commit = '',
			last_sync_error = '',
			updated_at = CURRENT_TIMESTAMP
		WHERE repository_id = ?
			AND (
				COALESCE(sync_status, '') <> ?
				OR (? > 0 AND last_sync_started_at_unix <= ?)
			)
	`),
		rec.Status,
		rec.StartedAtUnix,
		rec.Ref,
		rec.RepositoryID,
		SourceSyncStatusRunning,
		rec.RunningStaleBeforeUnix,
		rec.RunningStaleBeforeUnix,
	)

	if err != nil {
		return SourceRepositoryRecord{}, false, normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return SourceRepositoryRecord{}, false, normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		current, err := r.GetRepository(ctx, rec.RepositoryID)
		if err != nil {
			return SourceRepositoryRecord{}, false, err
		}

		return current, false, nil
	}

	current, err := r.GetRepository(ctx, rec.RepositoryID)
	if err != nil {
		return SourceRepositoryRecord{}, false, err
	}

	return current, true, nil
}

func (r *SQLSourcesRepository) UpdateRepositorySync(ctx context.Context, rec SourceRepositorySyncRecord) (SourceRepositoryRecord, error) {
	rec, err := normalizeSourceRepositorySyncRecord(rec)
	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`
		UPDATE source_repositories
		SET
			sync_status = ?,
			last_sync_started_at_unix = ?,
			last_sync_finished_at_unix = ?,
			last_sync_ref = ?,
			last_sync_commit = ?,
			last_sync_error = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE repository_id = ?
	`),
		rec.Status,
		rec.StartedAtUnix,
		rec.FinishedAtUnix,
		rec.Ref,
		rec.Commit,
		rec.Error,
		rec.RepositoryID,
	)

	if err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	if rowsAffected == 0 {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: source repository %s", ErrNotFound, rec.RepositoryID)
	}

	return r.GetRepository(ctx, rec.RepositoryID)
}

func (r *SQLSourcesRepository) GetRepository(ctx context.Context, repositoryID string) (SourceRepositoryRecord, error) {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: repository_id is required", ErrNotFound)
	}

	query := `
		SELECT
			id,
			COALESCE(global_id, ''),
			repository_id,
			namespace_id,
			source_kind,
			checkout_path,
			COALESCE(checkout_mode, ''),
			canonical_url,
			default_ref,
			credential_ref,
			enabled,
			COALESCE(sync_status, ''),
			last_sync_started_at_unix,
			last_sync_finished_at_unix,
			last_sync_ref,
			last_sync_commit,
			last_sync_error
		FROM source_repositories
		WHERE repository_id = ?`

	rec, err := r.scanRepositoryRow(r.db.QueryRowContext(ctx, rebindQueryForPgx(query), repositoryID))
	if err != nil {
		if err == sql.ErrNoRows {
			return SourceRepositoryRecord{}, fmt.Errorf("%w: source repository %s", ErrNotFound, repositoryID)
		}

		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

func (r *SQLSourcesRepository) ListRepositories(ctx context.Context, namespaceID int64) ([]SourceRepositoryRecord, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			id,
			COALESCE(global_id, ''),
			repository_id,
			namespace_id,
			source_kind,
			checkout_path,
			COALESCE(checkout_mode, ''),
			canonical_url,
			default_ref,
			credential_ref,
			enabled,
			COALESCE(sync_status, ''),
			last_sync_started_at_unix,
			last_sync_finished_at_unix,
			last_sync_ref,
			last_sync_commit,
			last_sync_error
		FROM source_repositories
		WHERE namespace_id = ?
		ORDER BY repository_id
	`), namespaceID)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []SourceRepositoryRecord
	for rows.Next() {
		rec, err := r.scanRepositoryRows(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func normalizeSourceRepositoryRecord(rec SourceRepositoryRecord) (SourceRepositoryRecord, error) {
	rec.RepositoryID = strings.TrimSpace(rec.RepositoryID)
	rec.SourceKind = strings.TrimSpace(rec.SourceKind)
	rec.CheckoutPath = strings.TrimSpace(rec.CheckoutPath)
	rec.CheckoutMode = strings.TrimSpace(rec.CheckoutMode)
	rec.CanonicalURL = strings.TrimSpace(rec.CanonicalURL)
	rec.DefaultRef = strings.TrimSpace(rec.DefaultRef)
	rec.CredentialRef = strings.TrimSpace(rec.CredentialRef)
	rec.SyncStatus = strings.TrimSpace(rec.SyncStatus)
	rec.LastSyncRef = strings.TrimSpace(rec.LastSyncRef)
	rec.LastSyncCommit = strings.TrimSpace(rec.LastSyncCommit)
	rec.LastSyncError = strings.TrimSpace(rec.LastSyncError)

	if rec.RepositoryID == "" {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: repository_id is required", ErrConflict)
	}

	if rec.SourceKind == "" {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: source_kind is required", ErrConflict)
	}

	if rec.SourceKind != SourceKindLocalCheckout {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: unsupported source_kind %q", ErrConflict, rec.SourceKind)
	}

	if rec.CheckoutMode == "" {
		rec.CheckoutMode = SourceCheckoutModeExternal
	}

	if !validSourceCheckoutMode(rec.CheckoutMode) {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: unsupported checkout_mode %q", ErrConflict, rec.CheckoutMode)
	}

	if rec.CheckoutPath == "" {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: checkout_path is required for %s", ErrConflict, SourceKindLocalCheckout)
	}

	if rec.SyncStatus == "" {
		rec.SyncStatus = SourceSyncStatusNever
	}

	if !validSourceSyncStatus(rec.SyncStatus) {
		return SourceRepositoryRecord{}, fmt.Errorf("%w: unsupported sync_status %q", ErrConflict, rec.SyncStatus)
	}

	if rec.NamespaceID <= 0 {
		rec.NamespaceID = 1
	}

	return rec, nil
}

func normalizeSourceRepositorySyncRecord(rec SourceRepositorySyncRecord) (SourceRepositorySyncRecord, error) {
	rec.RepositoryID = strings.TrimSpace(rec.RepositoryID)
	rec.Status = strings.TrimSpace(rec.Status)
	rec.Ref = strings.TrimSpace(rec.Ref)
	rec.Commit = strings.TrimSpace(rec.Commit)
	rec.Error = strings.TrimSpace(rec.Error)

	if rec.RepositoryID == "" {
		return SourceRepositorySyncRecord{}, fmt.Errorf("%w: repository_id is required", ErrConflict)
	}

	if rec.Status == "" {
		rec.Status = SourceSyncStatusNever
	}

	if !validSourceSyncStatus(rec.Status) {
		return SourceRepositorySyncRecord{}, fmt.Errorf("%w: unsupported sync_status %q", ErrConflict, rec.Status)
	}

	if rec.StartedAtUnix < 0 {
		rec.StartedAtUnix = 0
	}

	if rec.FinishedAtUnix < 0 {
		rec.FinishedAtUnix = 0
	}

	if rec.RunningStaleBeforeUnix < 0 {
		rec.RunningStaleBeforeUnix = 0
	}

	return rec, nil
}

func validSourceCheckoutMode(mode string) bool {
	switch mode {
	case SourceCheckoutModeExternal, SourceCheckoutModeManaged:
		return true
	default:
		return false
	}
}

func validSourceSyncStatus(status string) bool {
	switch status {
	case SourceSyncStatusNever, SourceSyncStatusRunning, SourceSyncStatusSucceeded, SourceSyncStatusFailed:
		return true
	default:
		return false
	}
}

func (r *SQLSourcesRepository) RecordDefinitionSource(ctx context.Context, rec JobDefinitionSourceRecord) error {
	rec, err := normalizeDefinitionSourceRecord(rec)
	if err != nil {
		return err
	}

	if exists, err := r.sourceRepositoryExists(ctx, rec.RepositoryID); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("%w: source repository %s", ErrConflict, rec.RepositoryID)
	}

	if exists, err := r.jobDefinitionExists(ctx, rec.JobID, rec.Version); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("%w: job %s version %d", ErrConflict, rec.JobID, rec.Version)
	}

	_, err = r.db.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO job_definition_sources (
			job_id,
			version,
			repository_id,
			requested_ref,
			resolved_commit,
			definition_path,
			blob_sha
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`),
		rec.JobID,
		rec.Version,
		rec.RepositoryID,
		rec.RequestedRef,
		rec.ResolvedCommit,
		rec.DefinitionPath,
		rec.BlobSHA,
	)

	return normalizeSQLError(err)
}

func normalizeDefinitionSourceRecord(rec JobDefinitionSourceRecord) (JobDefinitionSourceRecord, error) {
	rec.JobID = strings.TrimSpace(rec.JobID)
	rec.RepositoryID = strings.TrimSpace(rec.RepositoryID)
	rec.RequestedRef = strings.TrimSpace(rec.RequestedRef)
	rec.ResolvedCommit = strings.TrimSpace(rec.ResolvedCommit)
	rec.DefinitionPath = strings.TrimSpace(rec.DefinitionPath)
	rec.BlobSHA = strings.TrimSpace(rec.BlobSHA)

	if rec.JobID == "" {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: job_id is required", ErrConflict)
	}

	if rec.Version <= 0 {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: version must be positive", ErrConflict)
	}

	if rec.RepositoryID == "" {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: repository_id is required", ErrConflict)
	}

	if rec.RequestedRef == "" {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: requested_ref is required", ErrConflict)
	}

	if rec.ResolvedCommit == "" {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: resolved_commit is required", ErrConflict)
	}

	if rec.DefinitionPath == "" {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: definition_path is required", ErrConflict)
	}

	return rec, nil
}

func (r *SQLSourcesRepository) sourceRepositoryExists(ctx context.Context, repositoryID string) (bool, error) {
	var exists int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT 1 FROM source_repositories WHERE repository_id = ?"),
		repositoryID,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return true, nil
}

func (r *SQLSourcesRepository) jobDefinitionExists(ctx context.Context, jobID string, version int) (bool, error) {
	var exists int
	if err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT 1 FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return true, nil
}

func insertDefinitionSourceTx(ctx context.Context, tx *sql.Tx, rec JobDefinitionSourceRecord) error {
	rec, err := normalizeDefinitionSourceRecord(rec)
	if err != nil {
		return err
	}

	if exists, err := sourceRepositoryExistsTx(ctx, tx, rec.RepositoryID); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("%w: source repository %s", ErrConflict, rec.RepositoryID)
	}

	if exists, err := jobDefinitionExistsTx(ctx, tx, rec.JobID, rec.Version); err != nil {
		return err
	} else if !exists {
		return fmt.Errorf("%w: job %s version %d", ErrConflict, rec.JobID, rec.Version)
	}

	_, err = tx.ExecContext(ctx, rebindQueryForPgx(`
		INSERT INTO job_definition_sources (
			job_id,
			version,
			repository_id,
			requested_ref,
			resolved_commit,
			definition_path,
			blob_sha
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`),
		rec.JobID,
		rec.Version,
		rec.RepositoryID,
		rec.RequestedRef,
		rec.ResolvedCommit,
		rec.DefinitionPath,
		rec.BlobSHA,
	)

	return normalizeSQLError(err)
}

func sourceRepositoryExistsTx(ctx context.Context, tx *sql.Tx, repositoryID string) (bool, error) {
	var exists int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT 1 FROM source_repositories WHERE repository_id = ?"),
		repositoryID,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return true, nil
}

func jobDefinitionExistsTx(ctx context.Context, tx *sql.Tx, jobID string, version int) (bool, error) {
	var exists int
	if err := tx.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT 1 FROM job_definitions WHERE job_id = ? AND version = ?"),
		jobID,
		version,
	).Scan(&exists); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}

		return false, normalizeSQLError(err)
	}

	return true, nil
}

func (r *SQLSourcesRepository) GetDefinitionSource(ctx context.Context, jobID string, version int) (JobDefinitionSourceRecord, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || version <= 0 {
		return JobDefinitionSourceRecord{}, fmt.Errorf("%w: job definition source", ErrNotFound)
	}

	var rec JobDefinitionSourceRecord
	if err := r.db.QueryRowContext(ctx, rebindQueryForPgx(`
		SELECT
			job_id,
			version,
			repository_id,
			requested_ref,
			resolved_commit,
			definition_path,
			blob_sha
		FROM job_definition_sources
		WHERE job_id = ? AND version = ?
	`), jobID, version).Scan(
		&rec.JobID,
		&rec.Version,
		&rec.RepositoryID,
		&rec.RequestedRef,
		&rec.ResolvedCommit,
		&rec.DefinitionPath,
		&rec.BlobSHA,
	); err != nil {
		if err == sql.ErrNoRows {
			return JobDefinitionSourceRecord{}, fmt.Errorf("%w: job %s version %d source", ErrNotFound, jobID, version)
		}

		return JobDefinitionSourceRecord{}, normalizeSQLError(err)
	}

	return rec, nil
}

func (r *SQLSourcesRepository) GetDefinitionSources(ctx context.Context, jobID string, versions []int) (map[int]JobDefinitionSourceRecord, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || len(versions) == 0 {
		return map[int]JobDefinitionSourceRecord{}, nil
	}

	uniqueVersions := make([]int, 0, len(versions))
	seen := make(map[int]bool, len(versions))
	for _, version := range versions {
		if version <= 0 || seen[version] {
			continue
		}

		seen[version] = true
		uniqueVersions = append(uniqueVersions, version)
	}

	if len(uniqueVersions) == 0 {
		return map[int]JobDefinitionSourceRecord{}, nil
	}

	placeholders := make([]string, len(uniqueVersions))
	args := make([]any, 0, 1+len(uniqueVersions))
	args = append(args, jobID)
	for i, version := range uniqueVersions {
		placeholders[i] = "?"
		args = append(args, version)
	}

	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`
		SELECT
			job_id,
			version,
			repository_id,
			requested_ref,
			resolved_commit,
			definition_path,
			blob_sha
		FROM job_definition_sources
		WHERE job_id = ? AND version IN (`+strings.Join(placeholders, ",")+`)
	`), args...)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	out := make(map[int]JobDefinitionSourceRecord, len(uniqueVersions))
	for rows.Next() {
		var rec JobDefinitionSourceRecord
		if err := rows.Scan(
			&rec.JobID,
			&rec.Version,
			&rec.RepositoryID,
			&rec.RequestedRef,
			&rec.ResolvedCommit,
			&rec.DefinitionPath,
			&rec.BlobSHA,
		); err != nil {
			return nil, normalizeSQLError(err)
		}

		out[rec.Version] = rec
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLSourcesRepository) scanRepositoryRow(row *sql.Row) (SourceRepositoryRecord, error) {
	var rec SourceRepositoryRecord
	var enabledRaw any
	err := row.Scan(
		&rec.ID,
		&rec.GlobalID,
		&rec.RepositoryID,
		&rec.NamespaceID,
		&rec.SourceKind,
		&rec.CheckoutPath,
		&rec.CheckoutMode,
		&rec.CanonicalURL,
		&rec.DefaultRef,
		&rec.CredentialRef,
		&enabledRaw,
		&rec.SyncStatus,
		&rec.LastSyncStartedAtUnix,
		&rec.LastSyncFinishedAtUnix,
		&rec.LastSyncRef,
		&rec.LastSyncCommit,
		&rec.LastSyncError,
	)

	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	enabled, err := scanSQLBool(enabledRaw)
	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	rec.Enabled = enabled
	if rec.CheckoutMode == "" {
		rec.CheckoutMode = SourceCheckoutModeExternal
	}

	if rec.SyncStatus == "" {
		rec.SyncStatus = SourceSyncStatusNever
	}

	return rec, nil
}

func (r *SQLSourcesRepository) scanRepositoryRows(rows *sql.Rows) (SourceRepositoryRecord, error) {
	var rec SourceRepositoryRecord
	var enabledRaw any
	if err := rows.Scan(
		&rec.ID,
		&rec.GlobalID,
		&rec.RepositoryID,
		&rec.NamespaceID,
		&rec.SourceKind,
		&rec.CheckoutPath,
		&rec.CheckoutMode,
		&rec.CanonicalURL,
		&rec.DefaultRef,
		&rec.CredentialRef,
		&enabledRaw,
		&rec.SyncStatus,
		&rec.LastSyncStartedAtUnix,
		&rec.LastSyncFinishedAtUnix,
		&rec.LastSyncRef,
		&rec.LastSyncCommit,
		&rec.LastSyncError,
	); err != nil {
		return SourceRepositoryRecord{}, normalizeSQLError(err)
	}

	enabled, err := scanSQLBool(enabledRaw)
	if err != nil {
		return SourceRepositoryRecord{}, err
	}

	rec.Enabled = enabled
	if rec.CheckoutMode == "" {
		rec.CheckoutMode = SourceCheckoutModeExternal
	}

	if rec.SyncStatus == "" {
		rec.SyncStatus = SourceSyncStatusNever
	}

	return rec, nil
}

func scanSQLBool(v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case int64:
		return b != 0, nil
	case int32:
		return b != 0, nil
	case int:
		return b != 0, nil
	case []byte:
		return parseSQLBoolString(string(b))
	case string:
		return parseSQLBoolString(b)
	default:
		return false, fmt.Errorf("unsupported boolean value type %T", v)
	}
}

func parseSQLBoolString(raw string) (bool, error) {
	parsed, err := strconv.ParseBool(raw)
	if err == nil {
		return parsed, nil
	}

	i, iErr := strconv.ParseInt(raw, 10, 64)
	if iErr == nil {
		return i != 0, nil
	}

	return false, fmt.Errorf("invalid boolean value %q", raw)
}

var _ SourcesRepository = (*SQLSourcesRepository)(nil)
