-- Requires SQLite 3.35.0+ (ALTER TABLE ... DROP COLUMN).
DROP TABLE job_definitions;

ALTER TABLE job_runs DROP COLUMN definition_version;
