#!/bin/sh
set -eu

prek run --all-files

go test -tags=mage .
mage proto
git diff --exit-code -- api/gen/go

mage lint
mage testQuick
mage buildContainer
