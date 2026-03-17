-- NOTE(garrett): Requires SQLite 3.35+ for DROP COLUMN.
ALTER TABLE job_runs DROP COLUMN failure_reason;
