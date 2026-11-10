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

The console describes the source as a **Remote Git repository** or a **Local checkout**. Remote repositories remain hosted by their Git provider; Vectis maintains a local checkout to read their definitions.

For an externally maintained local checkout, **Check checkout** validates the checkout on disk. Update that checkout yourself to make new commits available. For a managed checkout, sync fetches from its configured source. Sync uses the repository's configured default ref.

Instance administrators may deliberately sync or check a disabled connection to prepare it for re-enabling. This leaves it disabled, does not submit jobs, and preserves active hotfixes. Automatic synchronization skips disabled connections.

An already-running sync is monitored while the page is visible. Idle pages do not poll. After sync, the console refreshes checkout health and definition discovery. Existing information remains visible while a refresh is pending, and failures are reported separately from empty results.

## Missing or invalid definitions

If a pushed definition is absent, inspect the configured default ref, scanned commit, checkout errors, and invalid-file list. Fix validation errors in source control, update or sync the checkout, then refresh discovery. Disabled repositories do not discover jobs.

## Add or edit a connection

Instance administrators open **Instance → Connections → Add connection**. Enter a unique repository ID, a Git URL, and a default ref. **HEAD** follows the remote's default branch. Use **Credential → None — Public access** for unauthenticated Git, or search for a saved credential by name, full reference, or provider. Provider information appears beneath each saved choice. The list contains reference names from the configured source credential provider; the form does not collect or expose passwords or tokens. If no credentials are available, configure one on the server before connecting a private repository. Select the initial namespace that may use the connection.

Use **Validate connection** to check Git access and the selected ref without saving. Errors remain on the editable form. A successful check shows the full resolved commit with a copy control, so an administrator can verify the revision before saving. **Revalidate** refreshes the result. **Save** reuses successful validation for the current form values; changing a setting marks it stale. Previous results stay visible during revalidation. Manual validation also works for a disabled connection.

**Save** checks Git access and resolves the selected ref before persisting the connection settings, unless the current settings already have a successful validation. Validation errors stay on the editable form; failed validation neither registers a new connection nor changes an existing connection or checkout. After saving, Vectis prepares the working checkout and discovers job files. **View jobs** opens the repository in a granted namespace, where jobs can be inspected and triggered. Use **Manage connection** to grant additional namespaces access.

If checkout preparation fails after a successful save, **Retry validation** reuses the saved connection; **Edit settings** lets you correct its URL, ref, or credential reference. Saving a disabled connection skips Git validation, allowing offline setup and maintenance. Connections declared in server configuration show **Managed by configuration** and must be edited there. The API also rejects updates to these settings, avoiding changes that would be overwritten at restart.

### Delete a connection

Open a saved connection under **Instance → Connections** and select **Delete connection**, then **Confirm deletion**. Escape or leaving the button cancels confirmation. Deletion removes the connection and its namespace grants; it does not delete the remote Git repository. Configuration-owned connections must first be removed from server configuration. Connections with recorded source references cannot be deleted; disable them through **Edit connection** to prevent further use while preserving history.

### Job file conventions

- **`.vectis.json`** is the repository's primary CI definition and appears as job **`ci`**.
- **`.vectis/<name>.json`** defines a named job. For example, `.vectis/lint.json` appears as **`lint`** and `.vectis/build.linux.json` as **`build.linux`**.
- Existing **`.vectis/jobs/`** definitions remain supported, including nested legacy paths. New definitions use the conventions above; updating a legacy definition preserves its existing file.

The primary CI filename does not enable automatic execution. Start it manually or configure a trigger or schedule. Run the same file locally with `bin/vectis-worker run-local .vectis.json --workspace .`. Vectis itself keeps its CI entrypoint in `.vectis.json`; the public Git example will discover it after this change is published.

Do not define the same job ID in more than one supported location. Discovery reports conflicting paths, and implicit execution rejects ambiguous jobs. Auxiliary JSON in other `.vectis/` subdirectories is not discovered as jobs. Explicit API `path` overrides remain available for inspecting or running a chosen file.

Registration and advanced checkout configuration also remain available through the [API](./api-reference.md).

## Shared connection administration

Instance administrators open **Instance → Connections** to see every registered connection and its granted namespaces, including connections that currently have no grants. Select a connection to inspect its source and manage **Namespace access**. Grant an existing namespace access, or click **Revoke access** twice to remove a grant.

A grant does not assign user roles. Viewers can inspect available definitions, Trigger users and Operators can start runs, and namespace administrators manage namespace access and schedules. Shared connection settings, sync, and definition authoring require Admin on Root. Non-root users do not receive private checkout paths or credential references.

Runs keep the namespace where they were started. The same source can execute in two namespaces with separate run authorization, history, logs, artifacts, and live run notifications. Revoking a grant stops new source runs, including scheduled fires, and leaves historical run attribution and access intact. Namespace grants are explicit; granting a parent namespace does not automatically grant its descendants.

Registration and connection settings remain available through configuration or the API. Configure `source.repositories[].namespaces` to grant multiple existing namespaces to one `repository_id`. Configuration grants are additive and are reapplied during startup reconciliation; remove a grant from configuration before revoking it in the console. A shared source schedule must specify its execution `namespace`.

## Job ownership and emergency definitions

A source job may be registered with one owning namespace. Instance administrators open a job's **Definition** card, select its owning namespace, and choose **Assign owner**. The namespace must have access to the connection. A repository can contain jobs owned by different teams; registering a job does not create a separate copy in every namespace. Registered jobs are listed only in their owner's namespace. Existing unassigned definitions remain discoverable for compatibility and should be assigned before using hotfix controls.

On laptop and desktop layouts, the job page places **Provenance**, up to five **Recent runs**, and **Triggers** in separate sections of a side column beside the definition. Each numbered run links directly to its details, with status, duration, and start time when available; **All** opens the run history. Narrower layouts stack those sections below the definition in the same order. With an active hotfix, the Definition card leads with its reason, activation details, and **Edit** / **Revert** actions, followed by the effective definition. **Provenance** shows repository, ref, definition file, namespace, and commit on separate full-width rows. Commit hashes are shortened for display; hover reveals the full hash and the copy control copies its complete value. The card header holds the status pill, **Effective** / **Diff** switcher, copy control, and available hotfix action; there is no separate contents header. The status pill reads **Source** or **Hotfix**. The viewer defaults to **Effective**. With an active hotfix, **Effective** / **Diff** switches to a unified diff between current source and effective JSON, with line numbers and added/removed lines. Viewing, editing, and review keep the header and content frame in place. The reason field appears above the JSON within the editing area. The copy icon copies effective JSON. The owning namespace's administrators can choose **Hotfix** on read-only sources (**Edit** when one is active), enter a reason in the initially focused field above the definition editor, and choose **Review**. The editor’s **Reset** action reloads the current effective definition and revision and clears the local draft and reason; a failed reset keeps the entered changes. Validation and request errors explain which step needs attention. The shared review shows the diff, with **Activate** as the primary action and **Run once** as an alternative. Operators with permission to trigger runs can open **Run with changes**, review the diff, and choose **Run once** for one execution. A one-off run does not change the active hotfix. Run details label both persistent and one-off changes **Hotfix**, show its reason separately from the run actor, and link Job ID to the job definition (or the captured definition for Run Once). Both workflows validate the replacement and capture the exact definition used by the run.

Hotfixes are immutable commits in a separate Vectis-owned Git store under `source.checkout_root/.hotfixes`. They do not require remote write credentials and are never pushed upstream. Sync continues updating the source checkout and does not replace the active hotfix. API, cron, and SCM trigger services need access to the same hotfix storage at the same filesystem path when they share source jobs. Include this directory in backups together with the database and source checkouts.

An active hotfix records its reason, actor, creation time, and source baseline. That attribution is copied into each run's captured provenance, so clearing a hotfix does not erase history. Existing runs and replays keep their captured definition. To return future runs to Git, choose **Revert**, inspect the diff from the effective definition to current upstream source, then choose **Revert** and **Confirm revert**. Escape or leaving the confirmation button resets it. Writes use both a job revision and source commit; a concurrent edit or sync requires another review.

Before disabling an enabled connection, the form lists registered jobs and owners, active hotfixes, configured automation, unassigned definitions, and every namespace grant. Review the inventory before saving. A stale inventory is rejected by the API. If discovery is unavailable or exceeds its bound, the preview explicitly marks the inventory incomplete and still lists known registrations and automation. Disabling blocks new source submissions without cancelling existing runs or deleting hotfix history. Re-enabling preserves any active hotfixes; review them before resuming work.
