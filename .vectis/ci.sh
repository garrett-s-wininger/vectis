#!/bin/sh
set -eu

prek run --all-files

make proto
git diff --exit-code -- api/gen/go

make lint
make test-quick
make build-container
