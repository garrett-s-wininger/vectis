CREATE TABLE namespaces (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    parent_id BIGINT REFERENCES namespaces(id),
    path TEXT UNIQUE NOT NULL,
    break_inheritance BOOLEAN NOT NULL DEFAULT false,
    home_cell TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO namespaces (id, global_id, name, description, path, break_inheritance, home_cell) VALUES (1, 'namespace-root', 'root', 'Default namespace boundary.', '/', false, 'local');
INSERT INTO namespaces (id, global_id, name, parent_id, path, break_inheritance, home_cell) VALUES (2, 'namespace-ephemeral', 'ephemeral', 1, '/ephemeral', false, 'local');
SELECT setval(pg_get_serial_sequence('namespaces', 'id'), (SELECT MAX(id) FROM namespaces));

CREATE TABLE stored_jobs (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    job_id TEXT UNIQUE NOT NULL,
    namespace_id BIGINT NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    current_version INTEGER NOT NULL DEFAULT 1,
    home_cell TEXT NOT NULL DEFAULT 'local',
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_triggers (
    id BIGSERIAL PRIMARY KEY,
    job_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    trigger_key TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    source_repository_id TEXT NOT NULL DEFAULT '',
    source_ref TEXT NOT NULL DEFAULT '',
    source_path TEXT NOT NULL DEFAULT '',
    source_override_ref TEXT NOT NULL DEFAULT '',
    source_override_path TEXT NOT NULL DEFAULT '',
    source_override_reason TEXT NOT NULL DEFAULT '',
    source_override_created_at_unix BIGINT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT true,
    home_cell TEXT NOT NULL DEFAULT 'local',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_triggers_job_type ON job_triggers(job_id, trigger_type);
CREATE INDEX idx_job_triggers_source_job ON job_triggers(source_repository_id, job_id);
CREATE INDEX idx_job_triggers_job_key ON job_triggers(job_id, trigger_key);

CREATE TABLE cron_trigger_specs (
    id BIGSERIAL PRIMARY KEY,
    trigger_id BIGINT NOT NULL REFERENCES job_triggers(id) ON DELETE CASCADE,
    schedule_id TEXT NOT NULL DEFAULT '',
    cron_spec TEXT NOT NULL,
    next_run_at TEXT NOT NULL,
    claim_token TEXT,
    claimed_until TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trigger_id),
    UNIQUE(cron_spec, trigger_id)
);

CREATE INDEX idx_cron_next_run ON cron_trigger_specs(next_run_at);
CREATE INDEX idx_cron_claimed_until ON cron_trigger_specs(claimed_until);
CREATE UNIQUE INDEX uidx_cron_schedule_id ON cron_trigger_specs(schedule_id) WHERE schedule_id <> '';

CREATE TABLE scm_poll_trigger_specs (
    id BIGSERIAL PRIMARY KEY,
    trigger_id BIGINT NOT NULL REFERENCES job_triggers(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    base_url TEXT NOT NULL DEFAULT '',
    project TEXT NOT NULL,
    branch TEXT NOT NULL DEFAULT '',
    query TEXT NOT NULL DEFAULT '',
    interval_seconds INTEGER NOT NULL DEFAULT 60,
    next_poll_at TEXT NOT NULL,
    cursor TEXT NOT NULL DEFAULT '',
    claim_token TEXT,
    claimed_until TEXT,
    last_poll_at TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trigger_id)
);

CREATE INDEX idx_scm_poll_next_poll ON scm_poll_trigger_specs(next_poll_at);
CREATE INDEX idx_scm_poll_claimed_until ON scm_poll_trigger_specs(claimed_until);
CREATE INDEX idx_scm_poll_provider_project ON scm_poll_trigger_specs(provider, project, branch);

CREATE TABLE trigger_invocations (
    id BIGSERIAL PRIMARY KEY,
    invocation_id TEXT UNIQUE NOT NULL,
    trigger_id BIGINT REFERENCES job_triggers(id) ON DELETE SET NULL,
    job_id TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    source_instance TEXT NOT NULL DEFAULT '',
    trigger_payload_hash TEXT NOT NULL DEFAULT '',
    requested_cells TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_trigger_invocations_job_created ON trigger_invocations(job_id, created_at, id);

CREATE TABLE job_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT UNIQUE NOT NULL,
    job_id TEXT NOT NULL,
    run_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    orphan_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    failure_code TEXT NOT NULL DEFAULT '',
    failure_reason TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    cancel_token TEXT,
    cancel_requested_at BIGINT,
    cancel_reason TEXT,
    lease_owner TEXT,
    lease_until BIGINT,
    last_dispatched_at BIGINT,
    log_shard_id TEXT NOT NULL DEFAULT '',
    log_shard_assigned_at BIGINT,
    definition_version INTEGER NOT NULL DEFAULT 1,
    definition_hash TEXT NOT NULL DEFAULT '',
    owning_cell TEXT NOT NULL DEFAULT 'local',
    replay_of_run_id TEXT REFERENCES job_runs(run_id),
    trigger_invocation_id TEXT,
    execution_payload_hash TEXT NOT NULL DEFAULT '',
    namespace_path TEXT NOT NULL DEFAULT '/'
);

CREATE INDEX idx_job_runs_job_id_run_index ON job_runs (job_id, run_index DESC);
CREATE INDEX idx_job_runs_replay_of_run_id ON job_runs (replay_of_run_id);

CREATE TABLE run_hot_state_owners (
    run_id TEXT PRIMARY KEY REFERENCES job_runs(run_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    owner_epoch TEXT NOT NULL,
    lease_until BIGINT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_run_hot_state_owners_lease_until ON run_hot_state_owners(lease_until);
CREATE INDEX idx_run_hot_state_owners_cell_owner ON run_hot_state_owners(cell_id, owner_id);

CREATE TABLE run_tasks (
    id BIGSERIAL PRIMARY KEY,
    task_id TEXT UNIQUE NOT NULL,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    parent_task_id TEXT REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    task_key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    spec_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, task_key)
);

CREATE INDEX idx_run_tasks_run_id ON run_tasks(run_id);
CREATE INDEX idx_run_tasks_status ON run_tasks(status);
CREATE INDEX idx_run_tasks_parent ON run_tasks(parent_task_id);

CREATE TABLE task_attempts (
    id BIGSERIAL PRIMARY KEY,
    attempt_id TEXT UNIQUE NOT NULL,
    task_id TEXT NOT NULL REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    cell_id TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    accepted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_observed_at BIGINT,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(task_id, attempt)
);

CREATE INDEX idx_task_attempts_task_id ON task_attempts(task_id);
CREATE INDEX idx_task_attempts_run_id ON task_attempts(run_id);
CREATE INDEX idx_task_attempts_cell_status ON task_attempts(cell_id, status);

CREATE TABLE run_segments (
    id BIGSERIAL PRIMARY KEY,
    segment_id TEXT UNIQUE NOT NULL,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_run_segments_run_id ON run_segments(run_id);
CREATE INDEX idx_run_segments_status ON run_segments(status);

CREATE TABLE segment_executions (
    id BIGSERIAL PRIMARY KEY,
    execution_id TEXT UNIQUE NOT NULL,
    segment_id TEXT NOT NULL REFERENCES run_segments(segment_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES run_tasks(task_id) ON DELETE CASCADE,
    task_attempt_id TEXT NOT NULL REFERENCES task_attempts(attempt_id) ON DELETE CASCADE,
    cell_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt INTEGER NOT NULL DEFAULT 1,
    lease_owner TEXT,
    lease_until BIGINT,
    start_deadline_unix_nano BIGINT,
    claim_token TEXT,
    accepted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_observed_at BIGINT,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
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
    id BIGSERIAL PRIMARY KEY,
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
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_execution_security_events_run_id ON execution_security_events(run_id, id);
CREATE INDEX idx_execution_security_events_attempt ON execution_security_events(task_attempt_id, id);
CREATE INDEX idx_execution_security_events_execution ON execution_security_events(execution_id, id);

CREATE TABLE run_task_final_facts (
    id BIGSERIAL PRIMARY KEY,
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
    accepted_at_unix_nano BIGINT,
    started_at_unix_nano BIGINT,
    finished_at_unix_nano BIGINT,
    last_observed_at BIGINT,
    event_sequence BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, task_key)
);

CREATE INDEX idx_run_task_final_facts_run_id_id ON run_task_final_facts(run_id, id);
CREATE INDEX idx_run_task_final_facts_task ON run_task_final_facts(run_id, task_id, id);
CREATE INDEX idx_run_task_final_facts_attempt ON run_task_final_facts(run_id, task_attempt_id, id);
CREATE INDEX idx_run_task_final_facts_execution ON run_task_final_facts(run_id, execution_id, id);

CREATE TABLE run_artifacts (
    id BIGSERIAL PRIMARY KEY,
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
    size_bytes BIGINT NOT NULL CHECK(size_bytes >= 0),
    artifact_shard_id TEXT NOT NULL,
    metadata_json TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
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
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cell_execution_acceptances (
    id BIGSERIAL PRIMARY KEY,
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
    enqueued_at BIGINT,
    last_enqueue_attempt_at BIGINT,
    enqueue_attempts INTEGER NOT NULL DEFAULT 0,
    last_enqueue_error TEXT,
    accepted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cell_execution_acceptances_cell ON cell_execution_acceptances(cell_id, accepted_at);
CREATE INDEX idx_cell_execution_acceptances_run ON cell_execution_acceptances(run_id);

CREATE TABLE cron_schedule_fires (
    schedule_id BIGINT NOT NULL REFERENCES cron_trigger_specs(id) ON DELETE CASCADE,
    scheduled_for TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE REFERENCES job_runs(run_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (schedule_id, scheduled_for)
);

CREATE INDEX idx_cron_schedule_fires_run_id ON cron_schedule_fires(run_id);

CREATE TABLE scm_trigger_events (
    trigger_id BIGINT NOT NULL REFERENCES job_triggers(id) ON DELETE CASCADE,
    event_key TEXT NOT NULL,
    run_id TEXT UNIQUE REFERENCES job_runs(run_id) ON DELETE SET NULL,
    payload_json TEXT NOT NULL DEFAULT '',
    discovered_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trigger_id, event_key)
);

CREATE INDEX idx_scm_trigger_events_run_id ON scm_trigger_events(run_id);

CREATE TABLE run_dispatch_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES job_runs(run_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    source_instance TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    message TEXT,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_run_dispatch_events_run_id_created_at ON run_dispatch_events(run_id, created_at, id);
CREATE INDEX idx_run_dispatch_events_type ON run_dispatch_events(event_type);

CREATE TABLE reaction_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT UNIQUE NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    namespace_id BIGINT REFERENCES namespaces(id) ON DELETE SET NULL,
    job_id TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    actor TEXT NOT NULL DEFAULT '',
    payload_json JSONB NOT NULL,
    source_cell TEXT NOT NULL DEFAULT 'local',
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_reaction_events_type_created ON reaction_events(event_type, created_at, id);
CREATE INDEX idx_reaction_events_run_created ON reaction_events(run_id, created_at, id);
CREATE INDEX idx_reaction_events_job_created ON reaction_events(job_id, created_at, id);
CREATE INDEX idx_reaction_events_namespace_created ON reaction_events(namespace_id, created_at, id);

CREATE TABLE reaction_targets (
    id BIGSERIAL PRIMARY KEY,
    target_id TEXT UNIQUE NOT NULL,
    namespace_id BIGINT REFERENCES namespaces(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    uses TEXT NOT NULL,
    config_json JSONB NOT NULL DEFAULT '{}',
    secret_refs_json JSONB NOT NULL DEFAULT '[]',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE(namespace_id, name)
);

CREATE INDEX idx_reaction_targets_kind ON reaction_targets(kind);
CREATE INDEX idx_reaction_targets_enabled ON reaction_targets(enabled);
CREATE UNIQUE INDEX idx_reaction_targets_global_name ON reaction_targets(name) WHERE namespace_id IS NULL;

CREATE TABLE reaction_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    subscription_id TEXT UNIQUE NOT NULL,
    namespace_id BIGINT REFERENCES namespaces(id) ON DELETE CASCADE,
    target_id TEXT NOT NULL REFERENCES reaction_targets(target_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    job_id TEXT NOT NULL DEFAULT '',
    run_status TEXT NOT NULL DEFAULT '',
    trigger_type TEXT NOT NULL DEFAULT '',
    owning_cell TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE(namespace_id, name)
);

CREATE INDEX idx_reaction_subscriptions_target ON reaction_subscriptions(target_id);
CREATE INDEX idx_reaction_subscriptions_event ON reaction_subscriptions(event_type, enabled);
CREATE UNIQUE INDEX idx_reaction_subscriptions_global_name ON reaction_subscriptions(name) WHERE namespace_id IS NULL;

CREATE TABLE reaction_invocations (
    id BIGSERIAL PRIMARY KEY,
    invocation_id TEXT UNIQUE NOT NULL,
    event_id TEXT NOT NULL REFERENCES reaction_events(event_id) ON DELETE CASCADE,
    target_id TEXT NOT NULL REFERENCES reaction_targets(target_id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    action_uses TEXT NOT NULL,
    action_descriptor_json JSONB NOT NULL DEFAULT '{}',
    action_digest TEXT NOT NULL DEFAULT '',
    target_config_json JSONB NOT NULL DEFAULT '{}',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at BIGINT NOT NULL,
    claimed_by TEXT NOT NULL DEFAULT '',
    claim_until BIGINT,
    last_error TEXT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT,
    UNIQUE(event_id, target_id)
);

CREATE INDEX idx_reaction_invocations_ready ON reaction_invocations(status, next_attempt_at, id);
CREATE INDEX idx_reaction_invocations_claims ON reaction_invocations(status, claim_until, id);
CREATE INDEX idx_reaction_invocations_event ON reaction_invocations(event_id, id);
CREATE INDEX idx_reaction_invocations_target ON reaction_invocations(target_id, id);

CREATE TABLE reaction_local_messages (
    id BIGSERIAL PRIMARY KEY,
    message_id TEXT UNIQUE NOT NULL,
    event_id TEXT NOT NULL REFERENCES reaction_events(event_id) ON DELETE CASCADE,
    invocation_id TEXT NOT NULL REFERENCES reaction_invocations(invocation_id) ON DELETE CASCADE,
    mailbox TEXT NOT NULL DEFAULT 'default',
    payload_json JSONB NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE INDEX idx_reaction_local_messages_mailbox_id ON reaction_local_messages(mailbox, id);
CREATE INDEX idx_reaction_local_messages_event ON reaction_local_messages(event_id, id);
CREATE UNIQUE INDEX idx_reaction_local_messages_invocation ON reaction_local_messages(invocation_id);

CREATE TABLE cell_catalog_events (
    id BIGSERIAL PRIMARY KEY,
    source_cell TEXT NOT NULL,
    event_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    received_at BIGINT NOT NULL,
    applied_at BIGINT,
    updated_at BIGINT NOT NULL,
    UNIQUE(source_cell, event_key)
);

CREATE INDEX idx_cell_catalog_events_status_id ON cell_catalog_events(status, id);
CREATE INDEX idx_cell_catalog_events_source_received ON cell_catalog_events(source_cell, received_at, id);

CREATE TABLE service_leases (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    lease_until BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX idx_service_leases_lease_until ON service_leases(lease_until);

CREATE TABLE idempotency_keys (
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT,
    resource_type TEXT NOT NULL DEFAULT '',
    resource_id TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(scope, key)
);

CREATE INDEX idx_idempotency_keys_resource ON idempotency_keys(resource_type, resource_id);

CREATE TABLE job_definitions (
    global_id TEXT UNIQUE,
    job_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    definition_json TEXT NOT NULL,
    definition_hash TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version)
);

CREATE TABLE source_repositories (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    repository_id TEXT UNIQUE NOT NULL,
    namespace_id BIGINT NOT NULL DEFAULT 1 REFERENCES namespaces(id),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('local_checkout')),
    checkout_path TEXT NOT NULL DEFAULT '',
    checkout_mode TEXT NOT NULL DEFAULT 'external' CHECK (checkout_mode IN ('external', 'managed')),
    authoring_mode TEXT NOT NULL DEFAULT 'read_only' CHECK (authoring_mode IN ('read_only', 'local_commit', 'external_change_request')),
    worker_cache_mode TEXT NOT NULL DEFAULT 'ephemeral' CHECK (worker_cache_mode IN ('ephemeral', 'persistent')),
    canonical_url TEXT NOT NULL DEFAULT '',
    fallback_remote_urls TEXT NOT NULL DEFAULT '',
    worker_cache_warm_refspecs TEXT NOT NULL DEFAULT '',
    default_ref TEXT NOT NULL DEFAULT '',
    credential_ref TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT true,
    sync_status TEXT NOT NULL DEFAULT 'never' CHECK (sync_status IN ('never', 'running', 'succeeded', 'failed')),
    last_sync_started_at_unix BIGINT NOT NULL DEFAULT 0,
    last_sync_finished_at_unix BIGINT NOT NULL DEFAULT 0,
    last_sync_ref TEXT NOT NULL DEFAULT '',
    last_sync_commit TEXT NOT NULL DEFAULT '',
    last_sync_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
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
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, version),
    FOREIGN KEY (job_id, version) REFERENCES job_definitions(job_id, version) ON DELETE CASCADE
);

CREATE INDEX idx_job_definition_sources_repository ON job_definition_sources(repository_id);

CREATE TABLE auth_instance_state (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    setup_completed_at TIMESTAMPTZ
);

INSERT INTO auth_instance_state (id, setup_completed_at) VALUES (1, NULL);

CREATE TABLE local_users (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    password_auth_enabled BOOLEAN NOT NULL DEFAULT true,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE auth_providers (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    provider_id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE external_identities (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    auth_provider_id BIGINT NOT NULL REFERENCES auth_providers(id) ON DELETE CASCADE,
    subject TEXT NOT NULL,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(auth_provider_id, subject),
    UNIQUE(local_user_id, auth_provider_id)
);

CREATE INDEX idx_external_identities_local_user ON external_identities(local_user_id);
CREATE INDEX idx_external_identities_provider ON external_identities(auth_provider_id);

CREATE TABLE api_tokens (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_api_tokens_token_hash ON api_tokens(token_hash);

CREATE TABLE api_rate_limit_buckets (
    bucket_key TEXT PRIMARY KEY,
    tokens DOUBLE PRECISION NOT NULL,
    last_refill_unix_nano BIGINT NOT NULL,
    last_access_unix_nano BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_api_rate_limit_buckets_last_access ON api_rate_limit_buckets(last_access_unix_nano);

CREATE TABLE api_sessions (
    session_hash TEXT PRIMARY KEY,
    csrf_token_hash TEXT NOT NULL,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    expires_at_unix_nano BIGINT NOT NULL,
    last_used_unix_nano BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ
);

CREATE INDEX idx_api_sessions_user ON api_sessions(local_user_id);
CREATE INDEX idx_api_sessions_expires ON api_sessions(expires_at_unix_nano);

CREATE TABLE role_bindings (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    local_user_id BIGINT NOT NULL REFERENCES local_users(id) ON DELETE CASCADE,
    namespace_id BIGINT NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    role TEXT NOT NULL CHECK (role IN ('viewer', 'trigger', 'operator', 'admin')),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(local_user_id, namespace_id, role)
);

CREATE TABLE api_token_scopes (
    id BIGSERIAL PRIMARY KEY,
    global_id TEXT UNIQUE,
    api_token_id BIGINT NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE,
    action TEXT NOT NULL CHECK (action IN ('job:read', 'job:write', 'run:trigger', 'run:read', 'run:operator', 'admin:*', 'catalog:ingest', 'user:admin', 'api:any')),
    namespace_id BIGINT REFERENCES namespaces(id),
    propagate BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(api_token_id, action, namespace_id)
);

CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    actor_id BIGINT REFERENCES local_users(id),
    target_id BIGINT,
    metadata JSONB,
    ip_address INET,
    correlation_id TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_log_event_type ON audit_log(event_type);
CREATE INDEX idx_audit_log_actor_id ON audit_log(actor_id);
CREATE INDEX idx_audit_log_created_at ON audit_log(created_at);

CREATE TABLE retention_holds (
    id BIGSERIAL PRIMARY KEY,
    hold_id TEXT UNIQUE NOT NULL,
    scope TEXT NOT NULL,
    target_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    owner TEXT NOT NULL,
    external_ref TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ,
    released_by TEXT NOT NULL DEFAULT '',
    release_reason TEXT NOT NULL DEFAULT '',
    released_at TIMESTAMPTZ
);

CREATE INDEX idx_retention_holds_scope_target ON retention_holds(scope, target_id);
CREATE INDEX idx_retention_holds_active ON retention_holds(scope, target_id, released_at, expires_at);
CREATE INDEX idx_retention_holds_expires_at ON retention_holds(expires_at);

CREATE INDEX idx_job_runs_status_dispatched ON job_runs(status, last_dispatched_at);
CREATE INDEX idx_job_runs_lease_until ON job_runs(lease_until);
CREATE INDEX idx_job_runs_status ON job_runs(status);
CREATE INDEX idx_audit_log_target_id ON audit_log(target_id);
CREATE INDEX idx_role_bindings_user ON role_bindings(local_user_id);
CREATE INDEX idx_role_bindings_namespace ON role_bindings(namespace_id);
