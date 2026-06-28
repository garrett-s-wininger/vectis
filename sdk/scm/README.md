# SCM SDK

`sdk/scm` owns the provider-neutral source-control contract used by SCM
integrations.

The SDK defines:

- the generic change query shape;
- the generic change and revision-ref model;
- a provider interface for querying changes;
- a polling helper that waits for a matching change without knowing the backing
  SCM system's REST API or query language.

Provider-specific translation belongs outside core Vectis. For example,
`extensions/actions/gerrit` maps `sdk/scm.Query` into Gerrit's REST query
syntax and decodes Gerrit's XSSI-prefixed responses into `sdk/scm.Change`.
