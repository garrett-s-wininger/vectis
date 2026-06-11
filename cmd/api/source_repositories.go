package main

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	sourcepkg "vectis/internal/source"
	"vectis/internal/utils"
)

func reconcileConfiguredSourceRepositories(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger) error {
	decls, err := config.SourceRepositoryDeclarations()
	if err != nil {
		return err
	}

	if len(decls) == 0 {
		return nil
	}

	for _, decl := range decls {
		rec, namespacePath, err := configuredSourceRepositoryRecord(ctx, repos, decl)
		if err != nil {
			return err
		}

		existing, err := repos.Sources().GetRepository(ctx, rec.RepositoryID)
		if err != nil {
			if !dal.IsNotFound(err) {
				return fmt.Errorf("get configured source repository %q: %w", rec.RepositoryID, err)
			}

			created, err := repos.Sources().CreateRepository(ctx, rec)
			if err != nil {
				return fmt.Errorf("create configured source repository %q: %w", rec.RepositoryID, err)
			}

			logConfiguredSourceRepository(logger, "created", created, namespacePath)
			continue
		}

		if existing.NamespaceID != rec.NamespaceID {
			return fmt.Errorf("configured source repository %q is already registered in another namespace", rec.RepositoryID)
		}

		if configuredSourceRepositoryEqual(existing, rec) {
			logConfiguredSourceRepository(logger, "unchanged", existing, namespacePath)
			continue
		}

		updated, err := repos.Sources().UpdateRepository(ctx, rec)
		if err != nil {
			return fmt.Errorf("update configured source repository %q: %w", rec.RepositoryID, err)
		}

		logConfiguredSourceRepository(logger, "updated", updated, namespacePath)
	}

	return nil
}

func configuredSourceRepositoryRecord(ctx context.Context, repos *dal.SQLRepositories, decl config.SourceRepositoryDeclaration) (dal.SourceRepositoryRecord, string, error) {
	namespacePath := strings.TrimSpace(decl.Namespace)
	if namespacePath == "" {
		namespacePath = "/"
	}

	ns, err := repos.Namespaces().GetByPath(ctx, namespacePath)
	if err != nil {
		return dal.SourceRepositoryRecord{}, "", fmt.Errorf("configured source repository %q namespace %q: %w", decl.RepositoryID, namespacePath, err)
	}

	sourceKind := strings.TrimSpace(decl.SourceKind)
	if sourceKind == "" {
		sourceKind = dal.SourceKindLocalCheckout
	}

	checkoutMode := strings.TrimSpace(decl.CheckoutMode)
	if checkoutMode == "" {
		checkoutMode = dal.SourceCheckoutModeExternal
	}

	authoringMode := strings.TrimSpace(decl.AuthoringMode)
	if authoringMode == "" {
		authoringMode = dal.SourceAuthoringModeReadOnly
	}

	checkoutPath := strings.TrimSpace(decl.CheckoutPath)
	if checkoutPath == "" && checkoutMode == dal.SourceCheckoutModeManaged {
		store, err := sourcepkg.NewCheckoutStore(config.SourceCheckoutRoot(utils.DataHome()))
		if err != nil {
			return dal.SourceRepositoryRecord{}, "", fmt.Errorf("configured source repository %q checkout root: %w", decl.RepositoryID, err)
		}

		checkoutPath, err = store.Path(decl.RepositoryID)
		if err != nil {
			return dal.SourceRepositoryRecord{}, "", fmt.Errorf("configured source repository %q checkout path: %w", decl.RepositoryID, err)
		}
	}

	enabled := true
	if decl.Enabled != nil {
		enabled = *decl.Enabled
	}

	return dal.SourceRepositoryRecord{
		RepositoryID:  decl.RepositoryID,
		NamespaceID:   ns.ID,
		SourceKind:    sourceKind,
		CheckoutPath:  checkoutPath,
		CheckoutMode:  checkoutMode,
		AuthoringMode: authoringMode,
		CanonicalURL:  strings.TrimSpace(decl.CanonicalURL),
		DefaultRef:    strings.TrimSpace(decl.DefaultRef),
		CredentialRef: strings.TrimSpace(decl.CredentialRef),
		Enabled:       enabled,
	}, namespacePath, nil
}

func configuredSourceRepositoryEqual(existing, desired dal.SourceRepositoryRecord) bool {
	return existing.SourceKind == desired.SourceKind &&
		existing.CheckoutPath == desired.CheckoutPath &&
		existing.CheckoutMode == desired.CheckoutMode &&
		existing.AuthoringMode == desired.AuthoringMode &&
		existing.CanonicalURL == desired.CanonicalURL &&
		existing.DefaultRef == desired.DefaultRef &&
		existing.CredentialRef == desired.CredentialRef &&
		existing.Enabled == desired.Enabled
}

func logConfiguredSourceRepository(logger interfaces.Logger, action string, rec dal.SourceRepositoryRecord, namespacePath string) {
	if logger == nil {
		return
	}

	logger.Info("Configured source repository %s: repository_id=%s namespace=%s checkout_mode=%s enabled=%t",
		action,
		rec.RepositoryID,
		namespacePath,
		rec.CheckoutMode,
		rec.Enabled,
	)
}
