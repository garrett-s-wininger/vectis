package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var validNamespaceName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

var ErrInvalidNamespaceName = errors.New("invalid namespace name: must match [a-zA-Z0-9_-]+")

type NamespaceRecord struct {
	ID               int64
	Name             string
	ParentID         *int64
	Path             string
	BreakInheritance bool
	CreatedAt        sql.NullTime
}

type NamespacesRepository interface {
	Create(ctx context.Context, name string, parentID *int64) (*NamespaceRecord, error)
	GetByID(ctx context.Context, id int64) (*NamespaceRecord, error)
	GetByPath(ctx context.Context, path string) (*NamespaceRecord, error)
	List(ctx context.Context) ([]NamespaceRecord, error)
	ListChildren(ctx context.Context, parentID int64) ([]NamespaceRecord, error)
	Delete(ctx context.Context, id int64) error
	HasChildren(ctx context.Context, id int64) (bool, error)
	HasJobs(ctx context.Context, id int64) (bool, error)
}

type SQLNamespacesRepository struct {
	db *sql.DB
}

func NewSQLNamespacesRepository(db *sql.DB) *SQLNamespacesRepository {
	return &SQLNamespacesRepository{db: db}
}

func (r *SQLNamespacesRepository) Create(ctx context.Context, name string, parentID *int64) (*NamespaceRecord, error) {
	if name == "" || name == "/" || name == ".." || !validNamespaceName.MatchString(name) {
		return nil, ErrInvalidNamespaceName
	}

	var path string
	if parentID == nil || *parentID == 0 {
		path = "/" + name
		parentID = nil
	} else {
		parent, err := r.GetByID(ctx, *parentID)
		if err != nil {
			return nil, err
		}

		path = parent.Path + "/" + name
	}

	var id int64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("INSERT INTO namespaces (name, parent_id, path) VALUES (?, ?, ?) RETURNING id"),
		name, parentID, path,
	).Scan(&id)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	return r.GetByID(ctx, id)
}

func (r *SQLNamespacesRepository) GetByID(ctx context.Context, id int64) (*NamespaceRecord, error) {
	var rec NamespaceRecord
	var parentID sql.NullInt64
	var breakInheritance int
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT id, name, parent_id, path, break_inheritance, created_at FROM namespaces WHERE id = ?"),
		id,
	).Scan(&rec.ID, &rec.Name, &parentID, &rec.Path, &breakInheritance, &rec.CreatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: namespace %d", ErrNotFound, id)
		}

		return nil, normalizeSQLError(err)
	}

	if parentID.Valid {
		rec.ParentID = &parentID.Int64
	}

	rec.BreakInheritance = breakInheritance != 0
	return &rec, nil
}

func (r *SQLNamespacesRepository) GetByPath(ctx context.Context, path string) (*NamespaceRecord, error) {
	var rec NamespaceRecord
	var parentID sql.NullInt64
	var breakInheritance int
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT id, name, parent_id, path, break_inheritance, created_at FROM namespaces WHERE path = ?"),
		path,
	).Scan(&rec.ID, &rec.Name, &parentID, &rec.Path, &breakInheritance, &rec.CreatedAt)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: namespace %s", ErrNotFound, path)
		}

		return nil, normalizeSQLError(err)
	}

	if parentID.Valid {
		rec.ParentID = &parentID.Int64
	}

	rec.BreakInheritance = breakInheritance != 0
	return &rec, nil
}

func (r *SQLNamespacesRepository) List(ctx context.Context) ([]NamespaceRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT id, name, parent_id, path, break_inheritance, created_at FROM namespaces ORDER BY path"),
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []NamespaceRecord
	for rows.Next() {
		var rec NamespaceRecord
		var parentID sql.NullInt64
		var breakInheritance int

		if err := rows.Scan(&rec.ID, &rec.Name, &parentID, &rec.Path, &breakInheritance, &rec.CreatedAt); err != nil {
			return nil, err
		}

		if parentID.Valid {
			rec.ParentID = &parentID.Int64
		}

		rec.BreakInheritance = breakInheritance != 0
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLNamespacesRepository) ListChildren(ctx context.Context, parentID int64) ([]NamespaceRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT id, name, parent_id, path, break_inheritance, created_at FROM namespaces WHERE parent_id = ? ORDER BY path"),
		parentID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []NamespaceRecord
	for rows.Next() {
		var rec NamespaceRecord
		var pid sql.NullInt64
		var breakInheritance int

		if err := rows.Scan(&rec.ID, &rec.Name, &pid, &rec.Path, &breakInheritance, &rec.CreatedAt); err != nil {
			return nil, err
		}

		if pid.Valid {
			rec.ParentID = &pid.Int64
		}

		rec.BreakInheritance = breakInheritance != 0
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLNamespacesRepository) Delete(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("DELETE FROM namespaces WHERE id = ?"),
		id,
	)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: namespace %d", ErrNotFound, id)
	}

	return nil
}

func (r *SQLNamespacesRepository) HasChildren(ctx context.Context, id int64) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COUNT(*) FROM namespaces WHERE parent_id = ?"),
		id,
	).Scan(&count)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return count > 0, nil
}

func (r *SQLNamespacesRepository) HasJobs(ctx context.Context, id int64) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT COUNT(*) FROM stored_jobs WHERE namespace_id = ?"),
		id,
	).Scan(&count)

	if err != nil {
		return false, normalizeSQLError(err)
	}

	return count > 0, nil
}

func BuildNamespacePath(parentPath, name string) string {
	if parentPath == "/" {
		return "/" + name
	}

	return parentPath + "/" + name
}

func ParseNamespacePath(path string) []string {
	if path == "/" {
		return []string{"/"}
	}

	parts := strings.Split(path, "/")
	result := []string{"/"}
	for i := 1; i < len(parts); i++ {
		result = append(result, strings.Join(parts[:i+1], "/"))
	}

	return result
}
