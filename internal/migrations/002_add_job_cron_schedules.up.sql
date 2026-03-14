CREATE TABLE job_cron_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES stored_jobs(job_id),
    cron_spec TEXT NOT NULL,
    next_run_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, cron_spec)
);

CREATE INDEX idx_cron_next_run ON job_cron_schedules(next_run_at);
