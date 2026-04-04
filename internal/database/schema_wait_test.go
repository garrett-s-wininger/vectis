package database

import (
	"database/sql"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestSchemaWaitConnectFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "ErrNoRows", err: sql.ErrNoRows, want: false},
		{
			name: "postgres_missing_migrations_table",
			err:  &pgconn.PgError{Code: "42P01", TableName: "schema_migrations"},
			want: false,
		},
		{
			name: "postgres_other_undefined_table",
			err:  &pgconn.PgError{Code: "42P01", TableName: "other"},
			want: false,
		},
		{
			name: "pgx_connect_error_wrapped",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: true,
		},
		{
			name: "dns_op_error",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", IsNotFound: true}},
			want: true,
		},
		{
			name: "password_auth_sqlstate",
			err:  &pgconn.PgError{Code: "28P01"},
			want: true,
		},
		{
			name: "role_or_auth_sqlstate",
			err:  &pgconn.PgError{Code: "28000"},
			want: true,
		},
		{
			name: "database_missing_sqlstate",
			err:  &pgconn.PgError{Code: "3D000"},
			want: true,
		},
		{
			name: "unknown_defaults_to_schema_path",
			err:  errors.New("some opaque driver failure"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := schemaWaitConnectFailure(tt.err); got != tt.want {
				t.Fatalf("schemaWaitConnectFailure(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
