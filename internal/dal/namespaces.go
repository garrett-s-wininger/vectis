package dal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var validNamespaceName = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

var ErrInvalidNamespaceName = errors.New("invalid namespace name: must match [a-zA-Z0-9_-]+")

const (
	RootNamespaceID        int64  = 1
	RootNamespacePath      string = "/"
	EphemeralNamespaceID          = 2
	EphemeralNamespaceName        = "ephemeral"
	EphemeralNamespacePath        = "/ephemeral"
)

type NamespaceRecord struct {
	ID               int64
	GlobalID         string
	Name             string
	ParentID         *int64
	Path             string
	BreakInheritance bool
	HomeCell         string
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
	db     *sql.DB
	cellID string
}

func NewSQLNamespacesRepository(db *sql.DB) *SQLNamespacesRepository {
	return NewSQLNamespacesRepositoryWithCellID(db, DefaultCellID)
}

func NewSQLNamespacesRepositoryWithCellID(db *sql.DB, cellID string) *SQLNamespacesRepository {
	return &SQLNamespacesRepository{db: db, cellID: normalizeCellID(cellID)}
}

func (r *SQLNamespacesRepository) currentCellID() string {
	return normalizeCellID(r.cellID)
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

		path = BuildNamespacePath(parent.Path, name)
	}

	var id int64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("INSERT INTO namespaces (global_id, name, parent_id, path, home_cell) VALUES (?, ?, ?, ?, ?) RETURNING id"),
		newGlobalID(), name, parentID, path, r.currentCellID(),
	).Scan(&id)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	return r.GetByID(ctx, id)
}

func (r *SQLNamespacesRepository) GetByID(ctx context.Context, id int64) (*NamespaceRecord, error) {
	var rec NamespaceRecord
	var parentID sql.NullInt64
	var breakInheritanceRaw any
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT id, COALESCE(global_id, ''), name, parent_id, path, break_inheritance, home_cell, created_at FROM namespaces WHERE id = ?"),
		id,
	).Scan(&rec.ID, &rec.GlobalID, &rec.Name, &parentID, &rec.Path, &breakInheritanceRaw, &rec.HomeCell, &rec.CreatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: namespace %d", ErrNotFound, id)
		}

		return nil, normalizeSQLError(err)
	}

	if parentID.Valid {
		rec.ParentID = &parentID.Int64
	}

	breakInheritance, err := scanBreakInheritance(breakInheritanceRaw)
	if err != nil {
		return nil, err
	}

	rec.BreakInheritance = breakInheritance
	return &rec, nil
}

func (r *SQLNamespacesRepository) GetByPath(ctx context.Context, path string) (*NamespaceRecord, error) {
	var rec NamespaceRecord
	var parentID sql.NullInt64
	var breakInheritanceRaw any
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("SELECT id, COALESCE(global_id, ''), name, parent_id, path, break_inheritance, home_cell, created_at FROM namespaces WHERE path = ?"),
		path,
	).Scan(&rec.ID, &rec.GlobalID, &rec.Name, &parentID, &rec.Path, &breakInheritanceRaw, &rec.HomeCell, &rec.CreatedAt)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: namespace %s", ErrNotFound, path)
		}

		return nil, normalizeSQLError(err)
	}

	if parentID.Valid {
		rec.ParentID = &parentID.Int64
	}

	breakInheritance, err := scanBreakInheritance(breakInheritanceRaw)
	if err != nil {
		return nil, err
	}

	rec.BreakInheritance = breakInheritance
	return &rec, nil
}

func (r *SQLNamespacesRepository) List(ctx context.Context) ([]NamespaceRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT id, COALESCE(global_id, ''), name, parent_id, path, break_inheritance, home_cell, created_at FROM namespaces ORDER BY path"),
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = rows.Close() }()

	var out []NamespaceRecord
	for rows.Next() {
		var rec NamespaceRecord
		var parentID sql.NullInt64
		var breakInheritanceRaw any

		if err := rows.Scan(&rec.ID, &rec.GlobalID, &rec.Name, &parentID, &rec.Path, &breakInheritanceRaw, &rec.HomeCell, &rec.CreatedAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		if parentID.Valid {
			rec.ParentID = &parentID.Int64
		}

		breakInheritance, err := scanBreakInheritance(breakInheritanceRaw)
		if err != nil {
			return nil, err
		}

		rec.BreakInheritance = breakInheritance
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
	}

	return out, nil
}

func (r *SQLNamespacesRepository) ListChildren(ctx context.Context, parentID int64) ([]NamespaceRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT id, COALESCE(global_id, ''), name, parent_id, path, break_inheritance, home_cell, created_at FROM namespaces WHERE parent_id = ? ORDER BY path"),
		parentID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer func() { _ = rows.Close() }()

	var out []NamespaceRecord
	for rows.Next() {
		var rec NamespaceRecord
		var pid sql.NullInt64
		var breakInheritanceRaw any

		if err := rows.Scan(&rec.ID, &rec.GlobalID, &rec.Name, &pid, &rec.Path, &breakInheritanceRaw, &rec.HomeCell, &rec.CreatedAt); err != nil {
			return nil, normalizeSQLError(err)
		}

		if pid.Valid {
			rec.ParentID = &pid.Int64
		}

		breakInheritance, err := scanBreakInheritance(breakInheritanceRaw)
		if err != nil {
			return nil, err
		}

		rec.BreakInheritance = breakInheritance
		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, normalizeSQLError(err)
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
		rebindQueryForPgx("SELECT COUNT(*) FROM source_repositories WHERE namespace_id = ?"),
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

func scanBreakInheritance(v any) (bool, error) {
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
		parsed, err := strconv.ParseBool(string(b))
		if err == nil {
			return parsed, nil
		}

		i, iErr := strconv.ParseInt(string(b), 10, 64)
		if iErr == nil {
			return i != 0, nil
		}

		return false, fmt.Errorf("invalid break_inheritance value %q", string(b))
	case string:
		parsed, err := strconv.ParseBool(b)
		if err == nil {
			return parsed, nil
		}

		i, iErr := strconv.ParseInt(b, 10, 64)
		if iErr == nil {
			return i != 0, nil
		}

		return false, fmt.Errorf("invalid break_inheritance value %q", b)
	default:
		return false, fmt.Errorf("unsupported break_inheritance value type %T", v)
	}
}
