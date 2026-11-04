package dal

import (
	"context"
	"database/sql"
	"fmt"
)

func (r *SQLSourcesRepository) GrantRepositoryNamespace(ctx context.Context, repositoryID string, namespaceID int64) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO source_repository_namespaces(repository_id,namespace_id) VALUES (?,?) ON CONFLICT(repository_id,namespace_id) DO NOTHING`), repositoryID, namespaceID)
	return normalizeSQLError(err)
}

func (r *SQLSourcesRepository) RevokeRepositoryNamespace(ctx context.Context, repositoryID string, namespaceID int64) error {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`DELETE FROM source_repository_namespaces WHERE repository_id = ? AND namespace_id = ?`), repositoryID, namespaceID)
	return normalizeSQLError(err)
}

func (r *SQLSourcesRepository) RepositoryNamespaces(ctx context.Context, repositoryID string) ([]NamespaceRecord, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`SELECT ns.id,ns.name,ns.path FROM namespaces ns JOIN source_repository_namespaces access ON access.namespace_id=ns.id WHERE access.repository_id=? ORDER BY ns.path`), repositoryID)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = rows.Close() }()

	out := []NamespaceRecord{}
	for rows.Next() {
		var row NamespaceRecord
		if err := rows.Scan(&row.ID, &row.Name, &row.Path); err != nil {
			return nil, normalizeSQLError(err)
		}

		out = append(out, row)
	}

	return out, normalizeSQLError(rows.Err())
}

func (r *SQLSourcesRepository) ListAllRepositories(ctx context.Context) ([]SourceRepositoryRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id,
			COALESCE(global_id, ''),
			repository_id,
			source_kind,
			checkout_path,
			COALESCE(checkout_mode, ''),
			COALESCE(authoring_mode, ''),
			COALESCE(worker_cache_mode, ''),
			canonical_url,
			COALESCE(fallback_remote_urls, ''),
			COALESCE(worker_cache_warm_refspecs, ''),
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
		ORDER BY repository_id`)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	out := []SourceRepositoryRecord{}
	for rows.Next() {
		row, err := r.scanRepositoryRows(rows)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}

		out = append(out, row)
	}

	scanErr := rows.Err()
	_ = rows.Close()
	if scanErr != nil {
		return nil, scanErr
	}

	grants, err := r.db.QueryContext(ctx, `SELECT access.repository_id,ns.path FROM source_repository_namespaces access JOIN namespaces ns ON ns.id=access.namespace_id ORDER BY ns.path`)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = grants.Close() }()

	byID := map[string]int{}
	for i, row := range out {
		byID[row.RepositoryID] = i
	}

	for grants.Next() {
		var id, path string
		if err := grants.Scan(&id, &path); err != nil {
			return nil, err
		}

		if i, ok := byID[id]; ok {
			out[i].NamespacePaths = append(out[i].NamespacePaths, path)
		}
	}

	return out, normalizeSQLError(grants.Err())
}

func namespacePathForRepositoryGrantTx(ctx context.Context, tx *sql.Tx, repositoryID string) (string, error) {
	rows, err := tx.QueryContext(ctx, rebindQueryForPgx(`SELECT ns.path FROM source_repository_namespaces access JOIN namespaces ns ON ns.id=access.namespace_id WHERE access.repository_id=? ORDER BY ns.path`), repositoryID)
	if err != nil {
		return "", err
	}
	defer func() { _ = rows.Close() }()

	var path string
	count := 0
	for rows.Next() {
		if err := rows.Scan(&path); err != nil {
			return "", err
		}

		count++
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	if count != 1 {
		return "", fmt.Errorf("%w: explicit namespace required for shared repository", ErrConflict)
	}

	return path, nil
}
