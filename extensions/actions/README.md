# Action Extensions

Action extensions expand the job vocabulary. They are for domain side effects
where the external system is the source of truth, such as posting a code review,
opening an issue, or invoking a deployment controller.

The current reusable action contract is descriptor based. `sdk/action` owns the
public descriptor model: references, input and port schemas, source/runtime
types, lifecycle status, capabilities, and descriptor digests. Manifests are
resolved through the action registry, frozen into execution envelopes, and
executed by a worker-supported runtime.

Standard action implementations should live here once their runtime/package
shape is explicit enough to reuse outside core Vectis. A public Go execution SDK
is intentionally not exposed yet; provider integrations should use descriptors
and worker-supported runtimes until that interface is deliberate.

`builtins/` remains the place for small core actions that Vectis itself needs to
run jobs. Provider-specific integrations should graduate here instead of growing
the builtin registry indefinitely.
