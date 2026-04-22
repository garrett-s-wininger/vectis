package dal

import (
	"context"
	"database/sql"
	"fmt"
)

type RoleBindingRecord struct {
	ID          int64
	LocalUserID int64
	NamespaceID int64
	Role        string
	CreatedAt   sql.NullTime
}

type RoleBindingsRepository interface {
	Create(ctx context.Context, localUserID, namespaceID int64, role string) (*RoleBindingRecord, error)
	Delete(ctx context.Context, localUserID, namespaceID int64, role string) error
	ListByNamespace(ctx context.Context, namespaceID int64) ([]RoleBindingRecord, error)
	ListByUser(ctx context.Context, localUserID int64) ([]RoleBindingRecord, error)
	GetUserRolesInNamespace(ctx context.Context, localUserID, namespaceID int64) ([]string, error)
}

type SQLRoleBindingsRepository struct {
	db *sql.DB
}

func NewSQLRoleBindingsRepository(db *sql.DB) *SQLRoleBindingsRepository {
	return &SQLRoleBindingsRepository{db: db}
}

func (r *SQLRoleBindingsRepository) Create(ctx context.Context, localUserID, namespaceID int64, role string) (*RoleBindingRecord, error) {
	var id int64
	err := r.db.QueryRowContext(ctx,
		rebindQueryForPgx("INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?) RETURNING id"),
		localUserID, namespaceID, role,
	).Scan(&id)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	return &RoleBindingRecord{
		ID:          id,
		LocalUserID: localUserID,
		NamespaceID: namespaceID,
		Role:        role,
	}, nil
}

func (r *SQLRoleBindingsRepository) Delete(ctx context.Context, localUserID, namespaceID int64, role string) error {
	res, err := r.db.ExecContext(ctx,
		rebindQueryForPgx("DELETE FROM role_bindings WHERE local_user_id = ? AND namespace_id = ? AND role = ?"),
		localUserID, namespaceID, role,
	)

	if err != nil {
		return normalizeSQLError(err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if n == 0 {
		return fmt.Errorf("%w: binding not found", ErrNotFound)
	}

	return nil
}

func (r *SQLRoleBindingsRepository) ListByNamespace(ctx context.Context, namespaceID int64) ([]RoleBindingRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`
			SELECT id, local_user_id, namespace_id, role, created_at
			FROM role_bindings
			WHERE namespace_id = ?
			ORDER BY role, local_user_id
		`),
		namespaceID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RoleBindingRecord
	for rows.Next() {
		var rec RoleBindingRecord
		if err := rows.Scan(&rec.ID, &rec.LocalUserID, &rec.NamespaceID, &rec.Role, &rec.CreatedAt); err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLRoleBindingsRepository) ListByUser(ctx context.Context, localUserID int64) ([]RoleBindingRecord, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx(`
			SELECT id, local_user_id, namespace_id, role, created_at
			FROM role_bindings
			WHERE local_user_id = ?
			ORDER BY namespace_id, role
		`),
		localUserID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}
	defer rows.Close()

	var out []RoleBindingRecord
	for rows.Next() {
		var rec RoleBindingRecord
		if err := rows.Scan(&rec.ID, &rec.LocalUserID, &rec.NamespaceID, &rec.Role, &rec.CreatedAt); err != nil {
			return nil, err
		}

		out = append(out, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (r *SQLRoleBindingsRepository) GetUserRolesInNamespace(ctx context.Context, localUserID, namespaceID int64) ([]string, error) {
	rows, err := r.db.QueryContext(ctx,
		rebindQueryForPgx("SELECT role FROM role_bindings WHERE local_user_id = ? AND namespace_id = ?"),
		localUserID, namespaceID,
	)

	if err != nil {
		return nil, normalizeSQLError(err)
	}

	defer rows.Close()

	var out []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}

		out = append(out, role)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
