# Source hotfix exercise

Create a small local Git fixture under the API's configured `source.checkout_root`. The Linux default is shown below; adjust the path if your deployment overrides it. This leaves your project's checkout untouched and needs no remote Git credentials.

```sh
VECTIS_DEMO_CHECKOUT="${XDG_DATA_HOME:-$HOME/.local/share}/vectis/source-checkouts/hotfix-demo"
mkdir -p "$VECTIS_DEMO_CHECKOUT"
cp examples/source-hotfix/job.json "$VECTIS_DEMO_CHECKOUT/.vectis.json"
git -C "$VECTIS_DEMO_CHECKOUT" init -b main
git -C "$VECTIS_DEMO_CHECKOUT" add .vectis.json
git -C "$VECTIS_DEMO_CHECKOUT" -c user.name=Demo -c user.email=demo@example.invalid commit -m 'Add failing demo'
./bin/vectis-cli sources register hotfix-demo "$VECTIS_DEMO_CHECKOUT" --namespace / --default-ref main
./bin/vectis-cli sources sync hotfix-demo
```

Use your normal CLI API URL, certificate trust, and instance-admin authentication settings. Register through the API/CLI as above so connection settings remain editable in the UI. Then open **Root → Repositories → hotfix-demo → ci**.

1. Open its `ci` job and assign an owning namespace in the Definition card.
2. Run the source definition. It prints a message, pauses for five seconds, and fails with exit code 7.
3. Choose **Make changes**, change `exit 7` to `exit 0`, and enter a reason. Review and submit a one-off run. It succeeds while the job still uses the original definition.
4. Activate the same edit as a hotfix. A normal run now succeeds and its captured definition records the hotfix reason and actor.
5. Sync the connection and run again. The hotfix remains active.
6. Commit another source change in the fixture. Review source restoration: the preview must show the new upstream contents. Clear the hotfix and verify a normal run uses that source definition.
7. Try disabling the connection. Review its owning job, hotfix state, and namespace access before confirming. Existing runs and captured definitions remain available.

For shared deployments, put the API's hotfix Git storage on storage also available to source cron consumers. Keep the database and that storage together in backups. This pre-1.0 schema update changes the initial migration, so recreate disposable development databases before testing it.
