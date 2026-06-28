# SCM Extensions

This directory contains source-control provider implementations that plug into
the `sdk/scm` contracts.

The control plane owns trigger persistence, claim coordination, event dedupe,
run creation, and dispatch. SCM extensions only translate provider state into
stable poll events and cursors.
