CREATE TABLE namespaces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    parent_id INTEGER REFERENCES namespaces(id),
    path TEXT UNIQUE NOT NULL,
    break_inheritance INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO namespaces (id, name, path, break_inheritance) VALUES (1, 'root', '/', 0);

CREATE TABLE stored_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT UNIQUE NOT NULL,
    namespace_id INTEGER NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    definition_json TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_cron_schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES stored_jobs(job_id),
    cron_spec TEXT NOT NULL,
    next_run_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, cron_spec)
);

CREATE INDEX idx_cron_next_run ON job_cron_schedules(next_run_at);

CREATE TABLE job_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    job_id TEXT NOT NULL,
    run_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    orphan_reason TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_reason TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    cancel_token TEXT,
    lease_owner TEXT,
    lease_until INTEGER,
    last_dispatched_at INTEGER,
    definition_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_job_runs_job_id_run_index ON job_runs (job_id, run_index DESC);

CREATE TABLE job_definitions (
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version)
);

CREATE TABLE auth_instance_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    setup_completed_at TIMESTAMP
);

INSERT INTO auth_instance_state (id, setup_completed_at) VALUES (1, NULL);

CREATE TABLE local_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE api_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    local_user_id INTEGER NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP
);

CREATE UNIQUE INDEX idx_api_tokens_token_hash ON api_tokens(token_hash);

CREATE TABLE role_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    local_user_id INTEGER NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    namespace_id INTEGER NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(local_user_id, namespace_id, role)
);

CREATE TABLE api_token_scopes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    api_token_id INTEGER NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE,
    action TEXT NOT NULL,
    namespace_id INTEGER REFERENCES namespaces(id),
    propagate INTEGER NOT NULL DEFAULT 1,
    UNIQUE(api_token_id, action, namespace_id)
);

CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    actor_id INTEGER REFERENCES local_users(id),
    target_id INTEGER,
    metadata TEXT,
    ip_address TEXT,
    correlation_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_log_event_type ON audit_log(event_type);
CREATE INDEX idx_audit_log_actor_id ON audit_log(actor_id);
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);

CREATE INDEX idx_stored_jobs_namespace ON stored_jobs(namespace_id);
CREATE INDEX idx_job_runs_status_dispatched ON job_runs(status, last_dispatched_at);
CREATE INDEX idx_job_runs_lease_until ON job_runs(lease_until);
CREATE INDEX idx_job_runs_status ON job_runs(status);
CREATE INDEX idx_audit_log_target_id ON audit_log(target_id);
CREATE INDEX idx_role_bindings_user ON role_bindings(local_user_id);
CREATE INDEX idx_role_bindings_namespace ON role_bindings(namespace_id);
