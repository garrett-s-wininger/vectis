# Artifact Storage Extensions

Artifact storage extensions implement the public `sdk/artifact` store contract.
They are composed by `vectis-artifact`, which keeps upload/read gRPC behavior,
service identity, registry publication, and metrics owned by the Vectis service
while allowing blob storage providers to live outside core internals.

Current implementations:

| Provider | Package | Notes |
| --- | --- | --- |
| S3-compatible object storage | `extensions/artifacts/s3` | Uses SigV4 signing and path-style URLs by default for local S3-compatible services. |
