#!/bin/sh
set -eu

prek run --all-files

mage proto
git diff --exit-code -- api/gen/go

mage lint
mage testQuick
mage buildContainer
