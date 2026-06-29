# SCM SDK

`sdk/scm` owns the provider-neutral source-control contract used by SCM
integrations.

The SDK defines:

- the generic change query shape;
- the generic change and revision-ref model;
- a provider interface for querying changes;
- a provider-neutral poll-trigger contract for durable SCM events;
- a polling helper that waits for a matching change without knowing the backing
  SCM system's REST API or query language.

Provider-specific translation belongs outside core Vectis. Generic ref polling
lives under `extensions/scm/git`; Gerrit change polling lives under
`extensions/scm/gerrit`; both plug into `vectis-scm-poller` through
`sdk/scm.PollProvider`. Gerrit review actions live separately under
`extensions/actions/gerrit`.
