package dal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

type SourceJobHotfix struct {
	Commit     string `json:"commit"`
	Reason     string `json:"reason"`
	Actor      string `json:"actor"`
	BaseCommit string `json:"base_commit"`
	BaseRef    string `json:"base_ref"`
	BasePath   string `json:"base_path"`
	CreatedAt  int64  `json:"created_at"`
	OneOff     bool   `json:"one_off,omitempty"`
}

type SourceJobRecord struct {
	RepositoryID string           `json:"repository_id"`
	JobID        string           `json:"job_id"`
	NamespaceID  int64            `json:"namespace_id"`
	Namespace    string           `json:"namespace"`
	Path         string           `json:"path"`
	Ref          string           `json:"ref"`
	Revision     int              `json:"revision"`
	HotfixStore  string           `json:"-"`
	Hotfix       *SourceJobHotfix `json:"hotfix,omitempty"`
}

type SourceJobAutomation struct {
	JobID     string `json:"job_id"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Enabled   bool   `json:"enabled"`
}

func (r *SQLSourcesRepository) RegisterSourceJob(ctx context.Context, rec SourceJobRecord) (SourceJobRecord, error) {
	_, err := r.db.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO source_jobs(repository_id,job_id,namespace_id,definition_path,source_ref)
 SELECT ?,?,?,?,? WHERE EXISTS(SELECT 1 FROM source_repository_namespaces WHERE repository_id=? AND namespace_id=?)
 ON CONFLICT(repository_id,job_id) DO NOTHING`), rec.RepositoryID, rec.JobID, rec.NamespaceID, rec.Path, rec.Ref, rec.RepositoryID, rec.NamespaceID)

	if err != nil {
		return SourceJobRecord{}, normalizeSQLError(err)
	}

	saved, err := r.GetSourceJob(ctx, rec.RepositoryID, rec.JobID)
	if err != nil {
		return SourceJobRecord{}, err
	}

	if saved.NamespaceID != rec.NamespaceID || saved.Path != rec.Path || saved.Ref != rec.Ref {
		return SourceJobRecord{}, fmt.Errorf("%w: job already has an owner and source location", ErrConflict)
	}

	return saved, nil
}

func scanSourceJob(row interface{ Scan(...any) error }) (SourceJobRecord, error) {
	var rec SourceJobRecord
	var raw string
	err := row.Scan(&rec.RepositoryID, &rec.JobID, &rec.NamespaceID, &rec.Namespace, &rec.Path, &rec.Ref, &rec.Revision, &rec.HotfixStore, &raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rec, ErrNotFound
		}

		return rec, normalizeSQLError(err)
	}

	if raw != "" {
		rec.Hotfix = &SourceJobHotfix{}
		if err = json.Unmarshal([]byte(raw), rec.Hotfix); err != nil {
			return rec, err
		}
	}

	return rec, nil
}

const sourceJobColumns = `sj.repository_id,sj.job_id,sj.namespace_id,n.path,sj.definition_path,sj.source_ref,sj.revision,sj.hotfix_store,sj.hotfix_json`

func (r *SQLSourcesRepository) GetSourceJob(ctx context.Context, repoID, jobID string) (SourceJobRecord, error) {
	return scanSourceJob(r.db.QueryRowContext(ctx, rebindQueryForPgx(`SELECT `+sourceJobColumns+` FROM source_jobs sj JOIN namespaces n ON n.id=sj.namespace_id WHERE sj.repository_id=? AND sj.job_id=?`), repoID, jobID))
}

func (r *SQLSourcesRepository) ListSourceJobs(ctx context.Context, repoID string) ([]SourceJobRecord, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`SELECT `+sourceJobColumns+` FROM source_jobs sj JOIN namespaces n ON n.id=sj.namespace_id WHERE sj.repository_id=? ORDER BY n.path,sj.job_id`), repoID)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]SourceJobRecord, 0)
	for rows.Next() {
		row, err := scanSourceJob(rows)
		if err != nil {
			return nil, err
		}

		out = append(out, row)
	}

	return out, rows.Err()
}

func (r *SQLSourcesRepository) SetSourceJobHotfix(ctx context.Context, rec SourceJobRecord, expectedRevision int) (SourceJobRecord, error) {
	raw := ""
	if rec.Hotfix != nil {
		b, err := json.Marshal(rec.Hotfix)
		if err != nil {
			return rec, err
		}

		raw = string(b)
	}

	res, err := r.db.ExecContext(ctx, rebindQueryForPgx(`UPDATE source_jobs SET hotfix_store=?,hotfix_json=?,revision=revision+1 WHERE repository_id=? AND job_id=? AND revision=?`), rec.HotfixStore, raw, rec.RepositoryID, rec.JobID, expectedRevision)
	if err != nil {
		return rec, normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return rec, err
	}

	if n != 1 {
		return rec, fmt.Errorf("%w: job changed; reload its definition", ErrConflict)
	}

	return r.GetSourceJob(ctx, rec.RepositoryID, rec.JobID)
}

func (r *SQLSourcesRepository) SourceJobAutomations(ctx context.Context, repoID string) ([]SourceJobAutomation, error) {
	rows, err := r.db.QueryContext(ctx, rebindQueryForPgx(`SELECT jt.job_id,n.path,jt.trigger_type,jt.enabled FROM job_triggers jt JOIN namespaces n ON n.id=jt.namespace_id WHERE jt.source_repository_id=? ORDER BY n.path,jt.job_id,jt.trigger_type`), repoID)
	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]SourceJobAutomation, 0)
	for rows.Next() {
		var row SourceJobAutomation
		if err := rows.Scan(&row.JobID, &row.Namespace, &row.Kind, &row.Enabled); err != nil {
			return nil, err
		}

		out = append(out, row)
	}

	return out, rows.Err()
}

func (r *SQLJobsRepository) SourceRepositories() SourcesRepository {
	return &SQLSourcesRepository{db: r.db}
}

func (r *SQLJobsRepository) SnapshotSourceDefinition(ctx context.Context, jobID, definitionJSON string, source JobDefinitionSourceRecord) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var version int
	if err = tx.QueryRowContext(ctx, rebindQueryForPgx(`SELECT COALESCE(MAX(version),0)+1 FROM job_definitions WHERE job_id=?`), jobID).Scan(&version); err != nil {
		return 0, normalizeSQLError(err)
	}

	if _, err = tx.ExecContext(ctx, rebindQueryForPgx(`INSERT INTO job_definitions(global_id,job_id,version,definition_json,definition_hash) VALUES(?,?,?,?,?)`), newGlobalID(), jobID, version, definitionJSON, DefinitionHash(definitionJSON)); err != nil {
		return 0, normalizeSQLError(err)
	}

	source.JobID = jobID
	source.Version = version
	if err = insertDefinitionSourceTx(ctx, tx, source); err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, err
	}

	return version, nil
}
