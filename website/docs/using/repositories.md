---
title: Repository diagnostics in the console
---

Repositories are registered once per Vectis instance. A connection holds its checkout, credentials, default ref, and sync state. Explicit namespace grants let several namespaces use that connection without duplicating it.

The namespace switcher at the top of the sidebar sets the context for **Jobs**, **Runs**, and **Repositories**. Open it to search full namespace paths or expand their hierarchy. Ancestors shown for context cannot be selected unless your account has access to them. Your effective access and **View namespace** link are inside this menu. Namespace administrators also have **Manage namespaces** here. The selected leaf and its parent path keep deep namespaces recognizable.

The sidebar separates namespace work from instance administration. It can collapse to an icon rail on desktop; that preference is saved in this browser. On smaller screens, use **Open navigation** to show a drawer. Workspace URLs preserve the namespace for sharing and browser history. **Root** means the root namespace itself; it does not aggregate all namespaces.

Open **Repositories** to inspect connections available in the selected namespace. A repository detail URL includes its unique repository ID and workspace namespace. Namespace roles still determine whether someone may inspect or trigger its jobs.

The list shows whether each repository is enabled, its default ref, its last sync result, and the time that sync attempt finished. Select a repository to inspect:

- **Source:** repository ID, ref, revision, remote when configured, availability, and the latest sync result. These diagnostics share one card. A successful past sync does not guarantee that the checkout is currently available.
- **Discovered jobs:** valid definitions and invalid files with their validation errors. The revision in Source identifies the snapshot used by the job listing. **Load more definitions** keeps pagination on that commit.

When the checkout changes after discovery, the console reports the difference and shows the newer checkout revision separately. Refresh to scan that revision. Matching checkout, discovery, and sync revisions appear once.

Selecting a discovered job opens its job page with repository context, including when another repository contains a job with the same ID. The job's Source section links back to its repository and shows its file, ref, and resolved commit when reported by the API.

## Refresh and sync

**Refresh** reloads diagnostics and discovered definitions; viewers may use it without changing the shared checkout. **Sync now** requires instance administration (Admin on Root). A namespace grant permits use of a connection; it does not delegate changes to a shared checkout, credentials, or source definitions.

For an externally maintained local checkout, sync validates the checkout on disk. Update that checkout yourself to make new commits available. For a managed checkout, sync fetches from its configured source. Sync uses the repository's configured default ref.

An already-running sync is monitored while the page is visible. Idle pages do not poll. After sync, the console refreshes checkout health and definition discovery. Existing information remains visible while a refresh is pending, and failures are reported separately from empty results.

## Missing or invalid definitions

If a pushed definition is absent, inspect the configured default ref, scanned commit, checkout errors, and invalid-file list. Fix validation errors in source control, update or sync the checkout, then refresh discovery. Disabled repositories do not discover jobs.

Repository registration and configuration use the existing configuration or [API](./api-reference.md) workflows.

## Shared connection administration

Instance administrators open **Instance → Connections** to see every registered connection and its granted namespaces, including connections that currently have no grants. Select a connection to inspect its source and manage **Namespace access**. Grant an existing namespace access, or click **Revoke access** twice to remove a grant.

A grant does not assign user roles. Viewers can inspect available definitions, Trigger users and Operators can start runs, and namespace administrators manage namespace access and schedules. Shared connection settings, sync, and definition authoring require Admin on Root. Non-root users do not receive private checkout paths or credential references.

Runs keep the namespace where they were started. The same source can execute in two namespaces with separate run authorization, history, logs, artifacts, and live run notifications. Revoking a grant stops new source runs, including scheduled fires, and leaves historical run attribution and access intact. Namespace grants are explicit; granting a parent namespace does not automatically grant its descendants.

Registration and connection settings remain available through configuration or the API. Configure `source.repositories[].namespaces` to grant multiple existing namespaces to one `repository_id`. Configuration grants are additive and are reapplied during startup reconciliation; remove a grant from configuration before revoking it in the console. A shared source schedule must specify its execution `namespace`.
