package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/secrets"
	sourcepkg "vectis/internal/source"
	"vectis/internal/utils"
)

type sourceRepositorySyncStatusFunc func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus
type sourceRepositoryCredentialResolver func(context.Context, dal.SourceRepositoryRecord) (sourcepkg.GitCredentials, error)

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

func startConfiguredSourceRepositoryPeriodicSync(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger) {
	startConfiguredSourceRepositoryPeriodicSyncWithStatus(ctx, repos, logger, configuredSourceRepositorySyncCheckoutStatus)
}

func startConfiguredSourceRepositoryPeriodicSyncWithStatus(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger, statusFn sourceRepositorySyncStatusFunc) {
	interval := config.SourceSyncConfiguredRepositoriesInterval()
	if interval <= 0 {
		return
	}

	if repos == nil {
		if logger != nil {
			logger.Warn("Configured source repository periodic sync disabled: repositories are not configured")
		}
		return
	}

	if logger != nil {
		logger.Info("Configured source repository periodic sync enabled: interval=%s max_concurrency=%d failure_backoff=%s",
			interval,
			config.SourceSyncConfiguredRepositoriesMaxConcurrency(),
			config.SourceSyncConfiguredRepositoriesFailureBackoff(),
		)
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				if logger != nil {
					logger.Info("Configured source repository periodic sync stopped")
				}

				return
			case <-ticker.C:
				if err := syncConfiguredSourceRepositoriesPeriodicCycle(ctx, repos, logger, statusFn); err != nil && logger != nil {
					logger.Warn("Configured source repository periodic sync cycle completed with errors: %v", err)
				}
			}
		}
	}()
}

func reconcileConfiguredSourceSchedules(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger) error {
	decls, err := config.SourceScheduleDeclarations()
	if err != nil {
		return err
	}

	if len(decls) == 0 {
		return nil
	}

	now := time.Now().UTC()
	for _, decl := range decls {
		rec, err := configuredSourceScheduleRecord(ctx, repos, decl, now)
		if err != nil {
			return err
		}

		existing, err := repos.Schedules().GetCronScheduleByScheduleID(ctx, rec.ScheduleID)
		if err != nil {
			if !dal.IsNotFound(err) {
				return fmt.Errorf("get configured source schedule %q: %w", rec.ScheduleID, err)
			}

			created, err := repos.Schedules().CreateCronSchedule(ctx, rec)
			if err != nil {
				return fmt.Errorf("create configured source schedule %q: %w", rec.ScheduleID, err)
			}

			logConfiguredSourceSchedule(logger, "created", created)
			continue
		}

		if configuredSourceScheduleEqual(existing, rec) {
			logConfiguredSourceSchedule(logger, "unchanged", existing)
			continue
		}

		if existing.CronSpec == rec.CronSpec && (existing.Enabled || !rec.Enabled) {
			rec.NextRunAt = time.Time{}
		}

		updated, err := repos.Schedules().UpdateCronSchedule(ctx, rec)
		if err != nil {
			return fmt.Errorf("update configured source schedule %q: %w", rec.ScheduleID, err)
		}

		logConfiguredSourceSchedule(logger, "updated", updated)
	}

	return nil
}

func syncConfiguredSourceRepositoriesPeriodicCycle(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger, statusFn sourceRepositorySyncStatusFunc) error {
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

	eligible := make([]dal.SourceRepositoryRecord, 0, len(decls))
	nowUnix := time.Now().Unix()
	backoff := config.SourceSyncConfiguredRepositoriesFailureBackoff()
	for _, decl := range decls {
		rec, err := repos.Sources().GetRepository(ctx, decl.RepositoryID)
		if err != nil {
			return fmt.Errorf("get configured source repository %q for periodic sync: %w", decl.RepositoryID, err)
		}

		if !rec.Enabled {
			logConfiguredSourceRepository(logger, "periodic sync skipped disabled", rec, "")
			continue
		}

		if configuredSourceRepositorySyncBackoffActive(rec, nowUnix, backoff) {
			logConfiguredSourceRepository(logger, "periodic sync skipped backoff", rec, "")
			continue
		}

		eligible = append(eligible, rec)
	}

	return syncConfiguredSourceRepositoryRecords(ctx, repos, logger, eligible, config.SourceSyncConfiguredRepositoriesMaxConcurrency(), statusFn)
}

func syncConfiguredSourceRepositoryRecords(ctx context.Context, repos *dal.SQLRepositories, logger interfaces.Logger, records []dal.SourceRepositoryRecord, maxConcurrency int, statusFn sourceRepositorySyncStatusFunc) error {
	if len(records) == 0 {
		return nil
	}

	if statusFn == nil {
		statusFn = configuredSourceRepositorySyncCheckoutStatus
	}

	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	sem := make(chan struct{}, maxConcurrency)
	errCh := make(chan error, len(records))
	started := 0

	for _, rec := range records {
		select {
		case <-ctx.Done():
			if started == 0 {
				return ctx.Err()
			}

			return collectConfiguredSourceRepositorySyncErrors(errCh, started, ctx.Err())
		case sem <- struct{}{}:
		}

		started++
		go func(rec dal.SourceRepositoryRecord) {
			defer func() { <-sem }()

			syncCtx := ctx
			syncCancel := func() {}
			if timeout := config.SourceSyncRunningTimeout(); timeout > 0 {
				syncCtx, syncCancel = context.WithTimeout(ctx, timeout)
			}
			defer syncCancel()

			err := syncConfiguredSourceRepository(syncCtx, repos, logger, rec, statusFn)
			if err != nil && logger != nil {
				logger.Warn("Configured source repository periodic sync failed: %v", err)
			}

			errCh <- err
		}(rec)
	}

	return collectConfiguredSourceRepositorySyncErrors(errCh, started, nil)
}

func collectConfiguredSourceRepositorySyncErrors(errCh <-chan error, count int, seed error) error {
	errs := make([]string, 0)
	if seed != nil {
		errs = append(errs, seed.Error())
	}

	for i := 0; i < count; i++ {
		if err := <-errCh; err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("%d configured source repository syncs failed: %s", len(errs), strings.Join(errs, "; "))
}

func configuredSourceRepositorySyncBackoffActive(rec dal.SourceRepositoryRecord, nowUnix int64, backoff time.Duration) bool {
	if backoff <= 0 || strings.TrimSpace(rec.SyncStatus) != dal.SourceSyncStatusFailed || rec.LastSyncFinishedAtUnix <= 0 {
		return false
	}

	return time.Unix(rec.LastSyncFinishedAtUnix, 0).Add(backoff).After(time.Unix(nowUnix, 0))
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

func configuredSourceScheduleRecord(ctx context.Context, repos *dal.SQLRepositories, decl config.SourceScheduleDeclaration, now time.Time) (dal.CronScheduleRecord, error) {
	enabled := true
	if decl.Enabled != nil {
		enabled = *decl.Enabled
	}

	repo, err := repos.Sources().GetRepository(ctx, decl.RepositoryID)
	if err != nil {
		return dal.CronScheduleRecord{}, fmt.Errorf("configured source schedule %q repository %q: %w", decl.ScheduleID, decl.RepositoryID, err)
	}

	if enabled && !repo.Enabled {
		return dal.CronScheduleRecord{}, fmt.Errorf("configured source schedule %q references disabled repository %q", decl.ScheduleID, decl.RepositoryID)
	}

	if strings.TrimSpace(decl.Path) == "" {
		if _, err := sourcepkg.DefinitionPathForJobID(decl.JobID); err != nil {
			return dal.CronScheduleRecord{}, fmt.Errorf("configured source schedule %q job_id: %w", decl.ScheduleID, err)
		}
	}

	nextRunAt, err := configuredSourceScheduleNextRun(decl.CronSpec, now)
	if err != nil {
		return dal.CronScheduleRecord{}, fmt.Errorf("configured source schedule %q cron_spec: %w", decl.ScheduleID, err)
	}

	return dal.CronScheduleRecord{
		ScheduleID:         strings.TrimSpace(decl.ScheduleID),
		JobID:              strings.TrimSpace(decl.JobID),
		CronSpec:           strings.TrimSpace(decl.CronSpec),
		NextRunAt:          nextRunAt,
		SourceRepositoryID: strings.TrimSpace(decl.RepositoryID),
		SourceRef:          strings.TrimSpace(decl.Ref),
		SourcePath:         strings.TrimSpace(decl.Path),
		Enabled:            enabled,
	}, nil
}

func configuredSourceScheduleNextRun(cronSpec string, from time.Time) (time.Time, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(strings.TrimSpace(cronSpec))
	if err != nil {
		return time.Time{}, err
	}

	return schedule.Next(from.UTC()), nil
}

func configuredSourceScheduleEqual(existing, desired dal.CronScheduleRecord) bool {
	return existing.JobID == desired.JobID &&
		existing.CronSpec == desired.CronSpec &&
		existing.SourceRepositoryID == desired.SourceRepositoryID &&
		existing.SourceRef == desired.SourceRef &&
		existing.SourcePath == desired.SourcePath &&
		existing.Enabled == desired.Enabled
}

func logConfiguredSourceSchedule(logger interfaces.Logger, action string, rec dal.CronScheduleRecord) {
	if logger == nil {
		return
	}

	logger.Info("Configured source schedule %s: schedule_id=%s repository_id=%s job_id=%s cron_spec=%q enabled=%t",
		action,
		rec.ScheduleID,
		rec.SourceRepositoryID,
		rec.JobID,
		rec.CronSpec,
		rec.Enabled,
	)
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
	return configuredSourceRepositorySyncCheckoutStatusWithCredentialResolver(nil)(ctx, rec, syncRef)
}

func configuredSourceRepositorySyncCheckoutStatusWithCredentialResolver(resolver sourceRepositoryCredentialResolver) sourceRepositorySyncStatusFunc {
	return func(ctx context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
		return configuredSourceRepositorySyncCheckoutStatusResolved(ctx, rec, syncRef, resolver)
	}
}

func configuredSourceRepositorySyncCheckoutStatusResolved(ctx context.Context, rec dal.SourceRepositoryRecord, syncRef string, resolver sourceRepositoryCredentialResolver) sourcepkg.GitCheckoutStatus {
	if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
		credentials, err := configuredSourceRepositoryGitCredentials(ctx, rec, resolver)
		if err != nil {
			return sourcepkg.GitCheckoutStatus{
				CheckoutPath: rec.CheckoutPath,
				DefaultRef:   syncRef,
				ErrorCode:    "git_credentials_unavailable",
				ErrorMessage: err.Error(),
			}
		}

		return sourcepkg.SyncManagedGitCheckout(ctx, sourcepkg.ManagedGitCheckoutRequest{
			CheckoutPath: rec.CheckoutPath,
			RemoteURL:    rec.CanonicalURL,
			DefaultRef:   syncRef,
			Credentials:  credentials,
		})
	}

	return sourcepkg.NewGitCheckout(rec.CheckoutPath).Status(ctx, syncRef)
}

func configuredSourceRepositoryGitCredentials(ctx context.Context, rec dal.SourceRepositoryRecord, resolver sourceRepositoryCredentialResolver) (sourcepkg.GitCredentials, error) {
	if strings.TrimSpace(rec.CredentialRef) == "" {
		return sourcepkg.GitCredentials{}, nil
	}

	if resolver == nil {
		return sourcepkg.GitCredentials{}, fmt.Errorf("credential_ref is configured but source credential resolver is not configured")
	}

	return resolver(ctx, rec)
}

func newConfiguredSourceRepositoryCredentialResolver(logger interfaces.Logger) (sourceRepositoryCredentialResolver, error) {
	root := strings.TrimSpace(config.SecretsEncryptedFSRoot())
	keyFile := strings.TrimSpace(config.SecretsEncryptedFSKeyFile())
	if root == "" && keyFile == "" {
		return nil, nil
	}

	if root == "" || keyFile == "" {
		return nil, fmt.Errorf("source repository credentials require both secrets.encryptedfs.root and secrets.encryptedfs.key_file")
	}

	provider, err := secrets.NewEncryptedFSProvider(root, secrets.WithEncryptedFSKeyFile(keyFile))
	if err != nil {
		return nil, fmt.Errorf("source repository credential provider: %w", err)
	}

	if logger != nil {
		logger.Info("Configured encryptedfs source repository credential resolver")
	}

	return func(ctx context.Context, rec dal.SourceRepositoryRecord) (sourcepkg.GitCredentials, error) {
		ref := strings.TrimSpace(rec.CredentialRef)
		if ref == "" {
			return sourcepkg.GitCredentials{}, nil
		}

		bundle, err := provider.Resolve(ctx, secrets.ResolveRequest{
			Scope: secrets.ExecutionScope{
				JobID: "source-repository:" + strings.TrimSpace(rec.RepositoryID),
			},
			Secrets: []secrets.Reference{{
				ID:  "git-credential",
				Ref: ref,
			}},
		})

		if err != nil {
			return sourcepkg.GitCredentials{}, fmt.Errorf("resolve source repository credential %q: %w", rec.RepositoryID, err)
		}

		if len(bundle.Files) != 1 {
			return sourcepkg.GitCredentials{}, fmt.Errorf("resolve source repository credential %q: expected 1 secret, got %d", rec.RepositoryID, len(bundle.Files))
		}

		credentials, err := sourcepkg.ParseGitCredentials(bundle.Files[0].Data)
		if err != nil {
			return sourcepkg.GitCredentials{}, fmt.Errorf("parse source repository credential %q: %w", rec.RepositoryID, err)
		}

		return credentials, nil
	}, nil
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
