package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	sourcepkg "vectis/internal/source"
	"vectis/internal/utils"
)

type sourceRepositorySyncStatusFunc func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus

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

func syncConfiguredSourceRepositories(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger) error {
	return syncConfiguredSourceRepositoriesWithStatus(ctx, repos, logger, configuredSourceRepositorySyncCheckoutStatus)
}

func syncConfiguredSourceRepositoriesWithStatus(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger, statusFn sourceRepositorySyncStatusFunc) error {
	if !config.SourceSyncConfiguredRepositoriesOnStartup() {
		return nil
	}

	if statusFn == nil {
		statusFn = configuredSourceRepositorySyncCheckoutStatus
	}

	decls, err := config.SourceRepositoryDeclarations()
	if err != nil {
		return err
	}

	if len(decls) == 0 {
		return nil
	}

	for _, decl := range decls {
		rec, err := repos.Sources().GetRepository(ctx, decl.RepositoryID)
		if err != nil {
			return fmt.Errorf("get configured source repository %q for startup sync: %w", decl.RepositoryID, err)
		}

		if !rec.Enabled {
			logConfiguredSourceRepository(logger, "sync skipped disabled", rec, "")
			continue
		}

		if err := syncConfiguredSourceRepository(ctx, repos, logger, rec, statusFn); err != nil {
			return err
		}
	}

	return nil
}

func syncConfiguredSourceRepository(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger, rec dal.SourceRepositoryRecord, statusFn sourceRepositorySyncStatusFunc) error {
	syncRef := configuredSourceRepositorySyncRef(rec)
	startedAt := time.Now().Unix()
	running, began, err := repos.Sources().BeginRepositorySync(ctx, dal.SourceRepositorySyncRecord{
		RepositoryID:           rec.RepositoryID,
		StartedAtUnix:          startedAt,
		Ref:                    syncRef,
		RunningStaleBeforeUnix: configuredSourceSyncStaleBeforeUnix(startedAt),
	})

	if err != nil {
		return fmt.Errorf("begin configured source repository sync %q: %w", rec.RepositoryID, err)
	}

	if !began {
		logConfiguredSourceRepository(logger, "sync already running", running, "")
		return nil
	}

	syncRecord := dal.SourceRepositorySyncRecord{
		RepositoryID:  rec.RepositoryID,
		StartedAtUnix: startedAt,
		Ref:           syncRef,
	}

	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutStatus := statusFn(ctx, rec, syncRef)
		if checkoutStatus.ErrorCode != "" {
			syncRecord.Status = dal.SourceSyncStatusFailed
			syncRecord.Error = configuredSourceRepositoryStatusSyncError(checkoutStatus)
		} else {
			syncRecord.Status = dal.SourceSyncStatusSucceeded
			syncRecord.Commit = checkoutStatus.ResolvedCommit
		}
	default:
		syncRecord.Status = dal.SourceSyncStatusFailed
		syncRecord.Error = "unsupported_source_kind: source kind is not supported"
	}

	syncRecord.FinishedAtUnix = time.Now().Unix()
	updateCtx, updateCancel := configuredSourceRepositorySyncUpdateContext(ctx)
	defer updateCancel()

	updated, err := repos.Sources().UpdateRepositorySync(updateCtx, syncRecord)
	if err != nil {
		return fmt.Errorf("update configured source repository sync %q: %w", rec.RepositoryID, err)
	}

	if syncRecord.Status == dal.SourceSyncStatusFailed {
		return fmt.Errorf("sync configured source repository %q: %s", rec.RepositoryID, syncRecord.Error)
	}

	logConfiguredSourceRepository(logger, "synced", updated, "")
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

	if namespacePath == "" {
		namespacePath = "-"
	}

	logger.Info("Configured source repository %s: repository_id=%s namespace=%s checkout_mode=%s enabled=%t",
		action,
		rec.RepositoryID,
		namespacePath,
		rec.CheckoutMode,
		rec.Enabled,
	)
}

func configuredSourceRepositorySyncRef(rec dal.SourceRepositoryRecord) string {
	ref := strings.TrimSpace(rec.DefaultRef)
	if ref == "" {
		return "HEAD"
	}

	return ref
}

func configuredSourceRepositorySyncCheckoutStatus(ctx context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
	if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
		return sourcepkg.SyncManagedGitCheckout(ctx, sourcepkg.ManagedGitCheckoutRequest{
			CheckoutPath: rec.CheckoutPath,
			RemoteURL:    rec.CanonicalURL,
			DefaultRef:   syncRef,
		})
	}

	return sourcepkg.NewGitCheckout(rec.CheckoutPath).Status(ctx, syncRef)
}

func configuredSourceRepositoryStatusSyncError(status sourcepkg.GitCheckoutStatus) string {
	if status.ErrorCode == "" {
		return ""
	}

	if status.ErrorMessage == "" {
		return status.ErrorCode
	}

	return status.ErrorCode + ": " + status.ErrorMessage
}

func configuredSourceSyncStaleBeforeUnix(nowUnix int64) int64 {
	timeout := config.SourceSyncRunningTimeout()
	if timeout <= 0 {
		return 0
	}

	return time.Unix(nowUnix, 0).Add(-timeout).Unix()
}

func configuredSourceRepositorySyncUpdateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}

	return context.WithTimeout(context.Background(), 10*time.Second)
}
