ALTER TABLE job_runs ADD COLUMN lease_owner TEXT;
ALTER TABLE job_runs ADD COLUMN lease_until INTEGER;
ALTER TABLE job_runs ADD COLUMN last_dispatched_at INTEGER;
