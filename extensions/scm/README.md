# SCM Extensions

This directory contains source-control provider implementations that plug into
the `sdk/scm` contracts.

The control plane owns trigger persistence, claim coordination, event dedupe,
run creation, and dispatch. SCM extensions only translate provider state into
stable poll events and cursors.

| Provider | Package | Poll behavior |
| --- | --- | --- |
| `git` | `extensions/scm/git` | Polls remote refs with `git ls-remote` and emits one event per changed ref hash. |
| `gerrit` | `extensions/scm/gerrit` | Queries Gerrit changes and emits one event per changed current revision. |
