# Generic Git SCM Provider

The generic Git provider implements `sdk/scm.PollProvider` by running
`git ls-remote` against a configured remote and comparing ref hashes to the
stored cursor.

Use it for ref-level triggers where a branch or tag update is enough to decide
that a Vectis job should run. Provider-specific review systems such as Gerrit
can build richer providers on the same poll contract when they need change
metadata, review labels, event streams, or provider-native query syntax.
