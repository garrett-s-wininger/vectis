# Gerrit Local Integration

This directory documents the Gerrit proof path for Vectis:

1. discover an open Gerrit change through the Gerrit REST query path;
2. checkout the discovered Gerrit change ref into the run workspace;
3. run ordinary Vectis job steps against that checkout;
4. post a Gerrit review message and optional label vote from the run;
5. consume Gerrit's SSH event stream through the managed stream bridge.

It intentionally does not add webhook ingestion or a general source-control
abstraction yet.

## Start Gerrit

The Makefile uses the same container runtime variable as the rest of the repo:

```sh
make gerrit-up
make gerrit-logs
```

Defaults:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `CONTAINER_CMD` | `podman` | Container runtime. |
| `GERRIT_IMAGE` | `docker.io/gerritcodereview/gerrit:3.14.1-ubuntu24` | Local Gerrit image. |
| `GERRIT_CONTAINER` | `vectis-gerrit` | Reusable container name. |
| `GERRIT_HTTP_PORT` | `18088` | Host HTTP port. |
| `GERRIT_SSH_PORT` | `29418` | Host SSH port. |

Stop the container without deleting its state:

```sh
make gerrit-down
```

## Real Service Smoke

The real-service smoke automates the local proof:

```sh
make gerrit-smoke
```

The smoke logs into Gerrit's development admin account, generates an HTTP token,
creates a project, pushes a real review change, polls Gerrit's REST query API
until the open change is discoverable, verifies that `extensions/scm/gerrit`
emits an SCM event for the change's current revision, checks out the discovered
Gerrit change ref with `builtins/checkout`, posts a `gerrit/review@v1` message
and label vote through the action-extension process runtime, verifies that a
wrong password is rejected, then runs a small Vectis job definition through the
job executor to check out the review and post another `gerrit/review@v1`
message using the normal workspace secret-file materialization path. Finally, it
proves the poller backstop by seeding a temporary migrated SQLite database with
an SCM poll trigger, polling the live Gerrit review as a missed event, and
requiring the normal SCM trigger processor to dispatch exactly one run through a
recording execution ingress.

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `GERRIT_SMOKE_URL` | `http://127.0.0.1:18088` | Gerrit base URL passed to the smoke. |
| `GERRIT_SMOKE_ACCOUNT_ID` | `1000000` | Development auth admin account id. |
| `GERRIT_SMOKE_USERNAME` | `admin` | Gerrit username used for generated HTTP token auth. |
| `GERRIT_SMOKE_PROJECT` | empty | Project to use; generated when empty. |
| `GERRIT_SMOKE_PROJECT_PREFIX` | `vectis-smoke` | Prefix for generated project names. |
| `GERRIT_SMOKE_LABEL` | `Code-Review` | Review label posted by the action. |
| `GERRIT_SMOKE_VALUE` | `+1` | Review label vote posted by the action. |
| `GERRIT_SMOKE_TIMEOUT` | `90s` | Maximum wait for setup and smoke operations. |
| `GERRIT_SMOKE_GIT` | `git` | Git executable used to push and checkout the change. |

To run only the check against an already running Gerrit:

```sh
make gerrit-smoke-check
```

## Managed Stream Smoke

The stream smoke validates the managed SSH path used by
`vectis-scm-gerrit-stream`:

```sh
make gerrit-stream-smoke
```

It logs into the development admin account, creates a project, generates a
temporary SSH key, uploads the public key to Gerrit, records the live SSH host
key into a temporary `known_hosts` file, starts `gerrit stream-events` through
the shared SSH stream transport, pushes a review change, and requires the
matching stream event to arrive with the same current revision and fetch ref
reported by Gerrit's REST API. It then routes that live stream event through
the shared SCM trigger pipeline, runs the Gerrit poller against the same
temporary database, and verifies the poller observes the same event without
dispatching a second run.

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `GERRIT_STREAM_SMOKE_URL` | `http://127.0.0.1:18088` | Gerrit base URL passed to the smoke. |
| `GERRIT_STREAM_SMOKE_ACCOUNT_ID` | `1000000` | Development auth admin account id. |
| `GERRIT_STREAM_SMOKE_USERNAME` | `admin` | Gerrit username used for HTTP and SSH auth. |
| `GERRIT_STREAM_SMOKE_PROJECT` | empty | Project to use; generated when empty. |
| `GERRIT_STREAM_SMOKE_PROJECT_PREFIX` | `vectis-stream-smoke` | Prefix for generated project names. |
| `GERRIT_STREAM_SMOKE_SSH_HOST` | `127.0.0.1` | Hostname used for the managed SSH stream. |
| `GERRIT_STREAM_SMOKE_SSH_PORT` | `29418` | Host SSH port. |
| `GERRIT_STREAM_SMOKE_TIMEOUT` | `90s` | Maximum wait for setup, SSH auth, stream readiness, and the event. |
| `GERRIT_STREAM_SMOKE_GIT` | `git` | Git executable used to push the change. |

## Seed A Change

The manual flow below is still useful when debugging a specific Gerrit instance.
Open `http://localhost:18088`, sign in through Gerrit's development account
flow, and create a project named `vectis-smoke`.

Create an HTTP credential for the `admin` account from Gerrit's user settings
and keep it in `GERRIT_HTTP_PASSWORD`. The same credential is used to push the
review change and later by `gerrit/review@v1`.

Create and push a review change:

```sh
git clone http://localhost:18088/vectis-smoke /tmp/vectis-gerrit-smoke
cd /tmp/vectis-gerrit-smoke
printf 'hello from Gerrit\n' > README.md
git add README.md
git commit -m $'Smoke change\n\nChange-Id: Ideadbeefdeadbeefdeadbeefdeadbeefdeadbeef'
git push http://admin@localhost:18088/a/vectis-smoke HEAD:refs/for/master
```

Gerrit prints the change number and change ref after the push. For the first
local change they are normally `change=1` and `ref=refs/changes/01/1/1`, which
match `examples/e2e-gerrit-change.json`. If your local instance uses a
different value, copy the example and adjust `checkout.with.ref`,
`report.with.change`, and `report.with.revision`.

## Store The Review Credential

Store the same Gerrit HTTP credential through Vectis' encryptedfs secret path:

```sh
printf '%s' "$GERRIT_HTTP_PASSWORD" > /tmp/gerrit-http-password

./bin/vectis-cli secrets encryptedfs put encryptedfs://gerrit/http-password \
  --from-file /tmp/gerrit-http-password \
  --root "$XDG_DATA_HOME/vectis/cells/local/secrets" \
  --key-file "$XDG_DATA_HOME/vectis/cells/local/secrets.key" \
  --force
```

The example job delivers that secret only to the `report` task, where
`gerrit/review@v1` reads it from the workspace-relative path
`.vectis/secrets/gerrit/http-password`.

## Run The Proof Job

With `vectis-local` running against the same `XDG_DATA_HOME`, configure the
standard action-extension root and submit:

```sh
export VECTIS_ACTION_REGISTRY_LOCAL_ROOTS="$PWD/extensions/actions"
```

```sh
./bin/vectis-cli jobs run examples/e2e-gerrit-change.json --follow
```

Expected proof:

| Check | Expected result |
| --- | --- |
| Discovery | The shared SCM polling contract returns the open change and current revision ref from Gerrit's REST query path. |
| SCM provider event | `extensions/scm/gerrit` emits the live change revision as a stable trigger event. |
| Checkout | `builtins/checkout` clones `http://localhost:18088/vectis-smoke`, fetches the Gerrit change ref, and checks out `FETCH_HEAD`. |
| Job step | The shell step sees `README.md` and prints `gerrit-change-smoke-ok`. |
| Review | `gerrit/review@v1` posts a message and `Code-Review +1` to the current revision using Gerrit's authenticated REST path. |

Generic SCM contracts live in `sdk/scm`. Gerrit-specific source-control REST
behavior lives under `extensions/scm/gerrit`; review action behavior lives under
`extensions/actions/gerrit`; and the review action is resolved from the standard
action-extension root instead of the builtin registry.

## Action Contract

`builtins/checkout` accepts:

| Input | Required | Purpose |
| --- | --- | --- |
| `url` | yes | Credential-free Git clone URL. |
| `ref` | no | Optional refspec fetched from `origin` after clone, then checked out detached via `FETCH_HEAD`. |

`gerrit/review@v1` accepts:

| Input | Required | Purpose |
| --- | --- | --- |
| `url` | yes | Gerrit base URL without embedded credentials. |
| `change` | yes | Gerrit change ID or numeric change number. |
| `revision` | no | Revision ID; defaults to `current`. |
| `message` | yes | Review message. |
| `label` | no | Optional Gerrit label, such as the stock `Code-Review` label or a site-specific `Verified` label. |
| `value` | with `label` | Integer vote for the label, such as `+1` or `-1`. |
| `tag` | no | Gerrit review tag; defaults to `autogenerated:vectis`. |
| `username` | yes | Gerrit HTTP auth username. |
| `password_file` | yes | Workspace-relative file containing the Gerrit HTTP password. |
