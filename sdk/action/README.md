# Action SDK

`sdk/action` owns the extension-facing action descriptor model: references,
runtime/source enums, input and port schemas, lifecycle status, capabilities,
and stable descriptor digests.

This package does not expose the in-process action execution interface. Go code
that implements Vectis worker internals still lives under `internal/action`.
Reusable action integrations should start with descriptors and worker-supported
runtimes; a public Go execution SDK can be added later if provider integrations
prove a stable shape.
