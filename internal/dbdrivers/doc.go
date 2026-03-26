// Package dbdrivers registers database/sql drivers used by Vectis binaries.
//
// Postgres (pgx) is always linked. SQLite is included unless the build tag
// "nosqlite" is set — use that for container images that only connect to Postgres.
package dbdrivers
