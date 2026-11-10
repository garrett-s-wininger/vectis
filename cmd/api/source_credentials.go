package main

import (
	"context"

	"github.com/spf13/viper"
	encryptedfs "vectis/extensions/secrets/encryptedfs"
	"vectis/internal/dal"
)

func configuredSourceCredentialReferences(ctx context.Context, resolver sourceRepositoryCredentialResolver) ([]string, error) {
	refs := []string{}
	if resolver == nil {
		return refs, nil
	}

	provider, err := encryptedfs.ConfigFromViper(viper.GetViper()).NewProvider()
	if err != nil {
		return nil, err
	}

	candidates, err := provider.ListReferences(ctx)
	if err != nil {
		return nil, err
	}

	for _, ref := range candidates {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if _, err := resolver(ctx, dal.SourceRepositoryRecord{RepositoryID: "credential-picker", CredentialRef: ref}); err == nil {
			refs = append(refs, ref)
		}
	}

	return refs, nil
}
