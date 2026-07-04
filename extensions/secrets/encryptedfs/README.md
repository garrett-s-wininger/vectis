# EncryptedFS Secret Provider

This standard secret provider implements the `sdk/secrets.Provider` contract for
local encrypted-file envelopes. It owns the `encryptedfs://` reference parser,
AES-GCM envelope format, and key-file helpers used by local development and
`vectis-cli secrets encryptedfs put`.

The cell-local `vectis-secrets` broker still owns workload identity validation,
access policy, provider dispatch, audit classification, and file
materialization. This package is intentionally limited to encryptedfs storage and
resolution mechanics so it can be reused without importing broker internals.
