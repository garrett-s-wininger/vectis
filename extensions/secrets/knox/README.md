# Knox Secret Provider

This standard secret-provider extension implements `sdk/secrets.Provider` for
Knox primary-version reads.

The provider accepts `knox:` and `knox://` references that resolve to Knox key
IDs, fetches the primary key version, and returns file material for the
cell-local `vectis-secrets` broker to deliver to workers.
