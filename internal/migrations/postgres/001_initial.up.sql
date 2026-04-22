CREATE TABLE namespaces (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id BIGINT REFERENCES namespaces(id),
    path TEXT UNIQUE NOT NULL,
    break_inheritance BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO namespaces (id, name, path, break_inheritance) VALUES (1, 'root', '/', false);

CREATE TABLE stored_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT UNIQUE NOT NULL,
    namespace_id BIGINT NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    definition_json TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_cron_schedules (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES stored_jobs(job_id),
    cron_spec TEXT NOT NULL,
    next_run_at TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(job_id, cron_spec)
);

CREATE INDEX idx_cron_next_run ON job_cron_schedules(next_run_at);

CREATE TABLE job_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT UNIQUE NOT NULL,
    job_id TEXT NOT NULL,
    run_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    orphan_reason TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_reason TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    lease_owner TEXT,
    lease_until BIGINT,
    last_dispatched_at BIGINT,
    definition_version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_job_runs_job_id_run_index ON job_runs (job_id, run_index DESC);

CREATE TABLE job_definitions (
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version)
);

CREATE TABLE auth_instance_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    setup_completed_at TIMESTAMPTZ
);

INSERT INTO auth_instance_state (id, setup_completed_at) VALUES (1, NULL);

CREATE TABLE local_users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE api_tokens (
    id BIGSERIAL PRIMARY KEY,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_api_tokens_token_hash ON api_tokens(token_hash);

CREATE TABLE role_bindings (
    id BIGSERIAL PRIMARY KEY,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    namespace_id BIGINT NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(local_user_id, namespace_id, role)
);

CREATE INDEX idx_stored_jobs_namespace ON stored_jobs(namespace_id);
CREATE INDEX idx_role_bindings_user ON role_bindings(local_user_id);
CREATE INDEX idx_role_bindings_namespace ON role_bindings(namespace_id);
