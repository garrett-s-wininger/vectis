#!/bin/sh
set -eu

status="$(git status --porcelain)"
if [ -n "$status" ]; then
  echo "ci-quick requires a clean git tree. Commit, stash, or discard local changes before running it." >&2
  echo "$status" >&2
  exit 1
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/vectis-ci-quick.XXXXXX")"
worktree="$tmpdir/worktree"
added=0

cleanup() {
  if [ "$added" -eq 1 ]; then
    git worktree remove --force "$worktree" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmpdir"
}
trap cleanup EXIT INT TERM

git worktree add --detach "$worktree" HEAD
added=1

cd "$worktree"
go run ./cmd/worker run-local .vectis/ci.json --workspace .
