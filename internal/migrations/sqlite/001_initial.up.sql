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

CREATE TABLE job_triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    source_repository_id TEXT NOT NULL DEFAULT '',
    source_ref TEXT NOT NULL DEFAULT '',
    source_path TEXT NOT NULL DEFAULT '',
    source_override_ref TEXT NOT NULL DEFAULT '',
    source_override_path TEXT NOT NULL DEFAULT '',
    source_override_reason TEXT NOT NULL DEFAULT '',
    source_override_created_at_unix INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    home_cell TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_triggers_job_type ON job_triggers(job_id, trigger_type);
CREATE INDEX idx_job_triggers_source_job ON job_triggers(source_repository_id, job_id);

CREATE TABLE cron_trigger_specs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trigger_id INTEGER NOT NULL REFERENCES job_triggers(id) ON DELETE CASCADE,
    schedule_id TEXT NOT NULL DEFAULT '',
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
CREATE UNIQUE INDEX uidx_cron_schedule_id ON cron_trigger_specs(schedule_id) WHERE schedule_id <> '';

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

CREATE TABLE run_hot_state_owners (
    run_id TEXT PRIMARY KEY REFERENCES job_runs(run_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    owner_epoch TEXT NOT NULL,
    lease_until INTEGER NOT NULL,
    last_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_run_hot_state_owners_lease_until ON run_hot_state_owners(lease_until);
CREATE INDEX idx_run_hot_state_owners_cell_owner ON run_hot_state_owners(cell_id, owner_id);

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
    lease_owner TEXT,
    lease_until INTEGER,
    start_deadline_unix_nano INTEGER,
    claim_token TEXT,
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
CREATE UNIQUE INDEX idx_segment_executions_task_attempt_id ON segment_executions(task_attempt_id);
CREATE INDEX idx_segment_executions_cell_status ON segment_executions(cell_id, status);
CREATE INDEX idx_segment_executions_lease_until ON segment_executions(lease_until);
CREATE INDEX idx_segment_executions_start_deadline ON segment_executions(start_deadline_unix_nano);

CREATE TABLE execution_security_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT UNIQUE,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    task_id TEXT REFERENCES run_tasks(task_id) ON DELETE SET NULL,
    task_attempt_id TEXT REFERENCES task_attempts(attempt_id) ON DELETE SET NULL,
    execution_id TEXT REFERENCES segment_executions(execution_id) ON DELETE SET NULL,
    event_type TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    provider TEXT,
    secret_count INTEGER,
    file_count INTEGER,
    created_at INTEGER NOT NULL
);

CREATE INDEX idx_execution_security_events_run_id ON execution_security_events(run_id, id);
CREATE INDEX idx_execution_security_events_attempt ON execution_security_events(task_attempt_id, id);
CREATE INDEX idx_execution_security_events_execution ON execution_security_events(execution_id, id);

CREATE TABLE run_task_final_facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    task_id TEXT UNIQUE NOT NULL,
    parent_task_id TEXT,
    task_key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    spec_hash TEXT NOT NULL DEFAULT '',
    task_attempt_id TEXT NOT NULL,
    execution_id TEXT UNIQUE NOT NULL,
    execution_status TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    accepted_at_unix_nano INTEGER,
    started_at_unix_nano INTEGER,
    finished_at_unix_nano INTEGER,
    last_observed_at INTEGER,
    event_sequence INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, task_key)
);

CREATE INDEX idx_run_task_final_facts_run_id_id ON run_task_final_facts(run_id, id);
CREATE INDEX idx_run_task_final_facts_task ON run_task_final_facts(run_id, task_id, id);
CREATE INDEX idx_run_task_final_facts_attempt ON run_task_final_facts(run_id, task_attempt_id, id);
CREATE INDEX idx_run_task_final_facts_execution ON run_task_final_facts(run_id, execution_id, id);

CREATE TABLE run_artifacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    task_id TEXT REFERENCES run_tasks(task_id) ON DELETE SET NULL,
    task_attempt_id TEXT REFERENCES task_attempts(attempt_id) ON DELETE SET NULL,
    execution_id TEXT REFERENCES segment_executions(execution_id) ON DELETE SET NULL,
    cell_id TEXT NOT NULL DEFAULT 'local',
    name TEXT NOT NULL,
    path TEXT NOT NULL DEFAULT '',
    content_type TEXT NOT NULL DEFAULT '',
    blob_key TEXT NOT NULL,
    blob_algorithm TEXT NOT NULL,
    blob_digest TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
    artifact_shard_id TEXT NOT NULL,
    metadata_json TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(run_id, name)
);

CREATE INDEX idx_run_artifacts_run_id_id ON run_artifacts(run_id, id);
CREATE INDEX idx_run_artifacts_blob_key ON run_artifacts(blob_key);
CREATE INDEX idx_run_artifacts_artifact_shard ON run_artifacts(artifact_shard_id, id);
CREATE INDEX idx_run_artifacts_task ON run_artifacts(run_id, task_id, id);
CREATE INDEX idx_run_artifacts_task_attempt ON run_artifacts(run_id, task_attempt_id, id);
CREATE INDEX idx_run_artifacts_execution ON run_artifacts(run_id, execution_id, id);

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
    schedule_id INTEGER NOT NULL REFERENCES cron_trigger_specs(id) ON DELETE CASCADE,
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
    resource_type TEXT NOT NULL DEFAULT '',
    resource_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(scope, key)
);

CREATE INDEX idx_idempotency_keys_resource ON idempotency_keys(resource_type, resource_id);

CREATE TABLE job_definitions (
    global_id TEXT UNIQUE,
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    definition_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version)
);

CREATE TABLE source_repositories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    global_id TEXT UNIQUE,
    repository_id TEXT UNIQUE NOT NULL,
    namespace_id INTEGER NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    source_kind TEXT NOT NULL,
    checkout_path TEXT NOT NULL DEFAULT '',
    checkout_mode TEXT NOT NULL DEFAULT 'external',
    authoring_mode TEXT NOT NULL DEFAULT 'read_only',
    canonical_url TEXT NOT NULL DEFAULT '',
    default_ref TEXT NOT NULL DEFAULT '',
    credential_ref TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    sync_status TEXT NOT NULL DEFAULT 'never',
    last_sync_started_at_unix INTEGER NOT NULL DEFAULT 0,
    last_sync_finished_at_unix INTEGER NOT NULL DEFAULT 0,
    last_sync_ref TEXT NOT NULL DEFAULT '',
    last_sync_commit TEXT NOT NULL DEFAULT '',
    last_sync_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_source_repositories_namespace ON source_repositories(namespace_id);
CREATE UNIQUE INDEX uidx_source_repositories_kind_checkout_path ON source_repositories(source_kind, checkout_path);

CREATE TABLE job_definition_sources (
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    repository_id TEXT NOT NULL REFERENCES source_repositories(repository_id),
    requested_ref TEXT NOT NULL,
    resolved_commit TEXT NOT NULL,
    definition_path TEXT NOT NULL,
    blob_sha TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version),
    FOREIGN KEY (job_id, version) REFERENCES job_definitions(job_id, version) ON DELETE CASCADE
);

CREATE INDEX idx_job_definition_sources_repository ON job_definition_sources(repository_id);

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

CREATE TABLE api_rate_limit_buckets (
    bucket_key TEXT PRIMARY KEY,
    tokens REAL NOT NULL,
    last_refill_unix_nano INTEGER NOT NULL,
    last_access_unix_nano INTEGER NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_api_rate_limit_buckets_last_access ON api_rate_limit_buckets(last_access_unix_nano);

CREATE TABLE api_sessions (
    session_hash TEXT PRIMARY KEY,
    csrf_token_hash TEXT NOT NULL,
    local_user_id INTEGER NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    expires_at_unix_nano INTEGER NOT NULL,
    last_used_unix_nano INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP
);

CREATE INDEX idx_api_sessions_user ON api_sessions(local_user_id);
CREATE INDEX idx_api_sessions_expires ON api_sessions(expires_at_unix_nano);

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

CREATE INDEX idx_job_runs_status_dispatched ON job_runs(status, last_dispatched_at);
CREATE INDEX idx_job_runs_lease_until ON job_runs(lease_until);
CREATE INDEX idx_job_runs_status ON job_runs(status);
CREATE INDEX idx_audit_log_target_id ON audit_log(target_id);
CREATE INDEX idx_role_bindings_user ON role_bindings(local_user_id);
CREATE INDEX idx_role_bindings_namespace ON role_bindings(namespace_id);
