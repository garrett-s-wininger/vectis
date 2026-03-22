CREATE TABLE job_definitions (
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version)
);

ALTER TABLE job_runs ADD COLUMN definition_version INTEGER NOT NULL DEFAULT 1;
