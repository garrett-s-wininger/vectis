#!/bin/sh
set -eu

pre-commit run --all-files

make proto
git diff --exit-code -- api/gen/go

make lint
make test-quick
make build-container
