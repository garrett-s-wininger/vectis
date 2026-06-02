CREATE TABLE namespaces (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    global_id TEXT UNIQUE,
    name TEXT NOT NULL,
    parent_id INTEGER REFERENCES namespaces(id),
    path TEXT UNIQUE NOT NULL,
    break_inheritance INTEGER NOT NULL DEFAULT 0,
    home_cell TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO namespaces (id, global_id, name, path, break_inheritance, home_cell) VALUES (1, 'namespace-root', 'root', '/', 0, 'local');

CREATE TABLE stored_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    global_id TEXT UNIQUE,
    job_id TEXT UNIQUE NOT NULL,
    namespace_id INTEGER NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    current_version INTEGER NOT NULL DEFAULT 1,
    home_cell TEXT NOT NULL DEFAULT 'local',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES stored_jobs(job_id),
    trigger_type TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    home_cell TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_triggers_job_type ON job_triggers(job_id, trigger_type);

CREATE TABLE cron_trigger_specs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_id INTEGER NOT NULL REFERENCES job_triggers(id) ON DELETE CASCADE,
    cron_spec TEXT NOT NULL,
    next_run_at TIMESTAMP NOT NULL,
    claim_token TEXT,
    claimed_until TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trigger_id),
    UNIQUE(cron_spec, trigger_id)
);

CREATE INDEX idx_cron_next_run ON cron_trigger_specs(next_run_at);
CREATE INDEX idx_cron_claimed_until ON cron_trigger_specs(claimed_until);

CREATE TABLE trigger_invocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation_id TEXT UNIQUE NOT NULL,
    trigger_id INTEGER REFERENCES job_triggers(id) ON DELETE SET NULL,
    job_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_payload_hash TEXT NOT NULL DEFAULT '',
    requested_cells TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trigger_invocations_job_created ON trigger_invocations(job_id, created_at, id);

CREATE TABLE job_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    job_id TEXT NOT NULL,
    run_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    orphan_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_reason TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    claim_token TEXT,
    cancel_token TEXT,
    cancel_requested_at INTEGER,
    cancel_reason TEXT,
    lease_owner TEXT,
    lease_until INTEGER,
    last_dispatched_at INTEGER,
    log_shard_id TEXT NOT NULL DEFAULT '',
    log_shard_assigned_at INTEGER,
    definition_version INTEGER NOT NULL DEFAULT 1,
    definition_hash TEXT NOT NULL DEFAULT '',
    owning_cell TEXT NOT NULL DEFAULT 'local',
    replay_of_run_id TEXT REFERENCES job_runs(run_id),
    trigger_invocation_id TEXT REFERENCES trigger_invocations(invocation_id),
    execution_payload_hash TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_job_runs_job_id_run_index ON job_runs (job_id, run_index DESC);
CREATE INDEX idx_job_runs_replay_of_run_id ON job_runs (replay_of_run_id);

CREATE TABLE run_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT UNIQUE NOT NULL,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    parent_task_id TEXT REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    spec_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, task_key)
);

CREATE INDEX idx_run_tasks_run_id ON run_tasks(run_id);
CREATE INDEX idx_run_tasks_status ON run_tasks(status);
CREATE INDEX idx_run_tasks_parent ON run_tasks(parent_task_id);

CREATE TABLE task_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT UNIQUE NOT NULL,
    task_id TEXT NOT NULL REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    accepted_at TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    last_observed_at INTEGER,
    event_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, attempt)
);

CREATE INDEX idx_task_attempts_task_id ON task_attempts(task_id);
CREATE INDEX idx_task_attempts_run_id ON task_attempts(run_id);
CREATE INDEX idx_task_attempts_cell_status ON task_attempts(cell_id, status);

CREATE TABLE run_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id TEXT UNIQUE NOT NULL,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_run_segments_run_id ON run_segments(run_id);
CREATE INDEX idx_run_segments_status ON run_segments(status);

CREATE TABLE segment_executions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT UNIQUE NOT NULL,
    segment_id TEXT NOT NULL REFERENCES run_segments(segment_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    task_attempt_id TEXT NOT NULL REFERENCES task_attempts(attempt_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt INTEGER NOT NULL DEFAULT 1,
    accepted_at TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    last_observed_at INTEGER,
    event_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(segment_id, cell_id, attempt)
);

CREATE INDEX idx_segment_executions_segment_id ON segment_executions(segment_id);
CREATE INDEX idx_segment_executions_run_id ON segment_executions(run_id);
CREATE INDEX idx_segment_executions_task_id ON segment_executions(task_id);
CREATE INDEX idx_segment_executions_task_attempt_id ON segment_executions(task_attempt_id);
CREATE INDEX idx_segment_executions_cell_status ON segment_executions(cell_id, status);

CREATE TABLE execution_payloads (
    payload_hash TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    definition_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cell_execution_acceptances (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT UNIQUE NOT NULL,
    acceptance_hash TEXT NOT NULL,
    run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    run_index INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    segment_name TEXT NOT NULL DEFAULT '',
    cell_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    definition_version INTEGER NOT NULL,
    definition_hash TEXT NOT NULL,
    execution_payload_hash TEXT NOT NULL REFERENCES execution_payloads(payload_hash),
    enqueued_at INTEGER,
    last_enqueue_attempt_at INTEGER,
    enqueue_attempts INTEGER NOT NULL DEFAULT 0,
    last_enqueue_error TEXT,
    accepted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cell_execution_acceptances_cell ON cell_execution_acceptances(cell_id, accepted_at);
CREATE INDEX idx_cell_execution_acceptances_run ON cell_execution_acceptances(run_id);

CREATE TABLE cron_schedule_fires (
    schedule_id INTEGER NOT NULL REFERENCES job_cron_schedules(id) ON DELETE CASCADE,
    scheduled_for TIMESTAMP NOT NULL,
    run_id TEXT NOT NULL UNIQUE REFERENCES job_runs(run_id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (schedule_id, scheduled_for)
);

CREATE INDEX idx_cron_schedule_fires_run_id ON cron_schedule_fires(run_id);

CREATE TABLE run_dispatch_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_run_dispatch_events_run_id_created_at ON run_dispatch_events(run_id, created_at, id);
CREATE INDEX idx_run_dispatch_events_type ON run_dispatch_events(event_type);

CREATE TABLE cell_catalog_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_cell TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    received_at INTEGER NOT NULL,
    applied_at INTEGER,
    updated_at INTEGER NOT NULL,
    UNIQUE(source_cell, event_key)
);

CREATE INDEX idx_cell_catalog_events_status_id ON cell_catalog_events(status, id);
CREATE INDEX idx_cell_catalog_events_source_received ON cell_catalog_events(source_cell, received_at, id);

CREATE TABLE service_leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    lease_until INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX idx_service_leases_lease_until ON service_leases(lease_until);

CREATE TABLE idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(scope, key)
);

CREATE TABLE job_definitions (
    global_id TEXT UNIQUE,
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    definition_hash TEXT NOT NULL DEFAULT '',
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
    global_id TEXT UNIQUE,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE api_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    global_id TEXT UNIQUE,
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
    global_id TEXT UNIQUE,
    local_user_id INTEGER NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    namespace_id INTEGER NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(local_user_id, namespace_id, role)
);

CREATE TABLE api_token_scopes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    global_id TEXT UNIQUE,
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
