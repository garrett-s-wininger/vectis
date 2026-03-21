-- SQLite 3.35+
ALTER TABLE job_runs DROP COLUMN lease_owner;
ALTER TABLE job_runs DROP COLUMN lease_until;
ALTER TABLE job_runs DROP COLUMN last_dispatched_at;
