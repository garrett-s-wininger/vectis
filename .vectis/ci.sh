#!/bin/sh
set -eu

GOPATH="$HOME/.cache/go"
export GOPATH

GIT_CEILING_DIRECTORIES="$TMPDIR"
export GIT_CEILING_DIRECTORIES

prek run --all-files

go test -tags=mage .
mage proto
git diff --exit-code -- api/gen/go

mage lint
mage testQuick
mage buildContainer
