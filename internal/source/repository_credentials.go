package source

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
	"vectis/internal/secrets"
)

type RepositoryCredentialResolver func(context.Context, dal.SourceRepositoryRecord) (GitCredentials, error)

func RepositoryGitCredentials(ctx context.Context, rec dal.SourceRepositoryRecord, resolver RepositoryCredentialResolver) (GitCredentials, error) {
	if strings.TrimSpace(rec.CredentialRef) == "" {
		return GitCredentials{}, nil
	}

	if resolver == nil {
		return GitCredentials{}, fmt.Errorf("credential_ref is configured but source credential resolver is not configured")
	}

	return resolver(ctx, rec)
}

func NewRepositoryCredentialResolverFromSecrets(resolver secrets.Resolver) RepositoryCredentialResolver {
	if resolver == nil {
		return nil
	}

	return func(ctx context.Context, rec dal.SourceRepositoryRecord) (GitCredentials, error) {
		ref := strings.TrimSpace(rec.CredentialRef)
		if ref == "" {
			return GitCredentials{}, nil
		}

		bundle, err := resolver.Resolve(ctx, secrets.ResolveRequest{
			Scope: secrets.ExecutionScope{
				JobID: "source-repository:" + strings.TrimSpace(rec.RepositoryID),
			},
			Secrets: []secrets.Reference{{
				ID:       "git-credential",
				Ref:      ref,
				Delivery: secrets.Delivery{Type: secrets.DeliveryTypeFile, Path: "git-credential"},
			}},
		})

		if err != nil {
			return GitCredentials{}, fmt.Errorf("resolve source repository credential %q: %w", rec.RepositoryID, err)
		}

		if len(bundle.Files) != 1 {
			return GitCredentials{}, fmt.Errorf("resolve source repository credential %q: expected 1 secret, got %d", rec.RepositoryID, len(bundle.Files))
		}

		credentials, err := ParseGitCredentials(bundle.Files[0].Data)
		if err != nil {
			return GitCredentials{}, fmt.Errorf("parse source repository credential %q: %w", rec.RepositoryID, err)
		}

		return credentials, nil
	}
}
