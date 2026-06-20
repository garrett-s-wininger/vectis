package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/artifact"
	"vectis/internal/backoff"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	jobexec "vectis/internal/job"
)

type workerArtifactPublisher struct {
	logger    interfaces.Logger
	manifests dal.ArtifactsRepository
	catalog   cell.CatalogEventPublisher
	metrics   backoff.RetryMetrics

	runID         string
	taskID        string
	taskKey       string
	taskAttemptID string
	executionID   string
	cellID        string
	job           *api.Job
	runs          dal.RunsRepository
	maxBytes      int64
	maxRunBytes   int64
	maxRunCount   int64

	mu            sync.Mutex
	publisher     *artifact.RoutedPublisher
	ensureTask    sync.Once
	ensureTaskErr error
}

func (w *worker) newArtifactPublisher(job *api.Job, env *cell.ExecutionEnvelope) *workerArtifactPublisher {
	if w == nil || w.artifactManifests == nil || env == nil {
		return nil
	}

	return &workerArtifactPublisher{
		logger:        w.logger,
		manifests:     w.artifactManifests,
		catalog:       w.catalog,
		metrics:       w.retryMetrics,
		runID:         env.RunID,
		taskID:        env.TaskID,
		taskKey:       env.TaskKey,
		taskAttemptID: env.TaskAttemptID,
		executionID:   env.ExecutionID,
		cellID:        env.CellID,
		job:           job,
		runs:          w.store,
		maxBytes:      w.artifactMaxBytes,
		maxRunBytes:   w.artifactMaxRunBytes,
		maxRunCount:   w.artifactMaxCount,
	}
}

func (p *workerArtifactPublisher) PublishArtifact(ctx context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	if p == nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("artifact publisher is not configured")
	}

	if err := p.ensureDurableTaskPath(ctx); err != nil {
		return action.ArtifactPublishResult{}, err
	}

	publisher, err := p.ensurePublisher(ctx)
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}

	maxBytes := req.MaxBytes
	if p.maxBytes > 0 && (maxBytes <= 0 || maxBytes > p.maxBytes) {
		maxBytes = p.maxBytes
	}

	published, err := publisher.Publish(ctx, artifact.PublishRequest{
		RunID:         p.runID,
		TaskID:        p.taskID,
		TaskAttemptID: p.taskAttemptID,
		ExecutionID:   p.executionID,
		CellID:        p.cellID,
		Name:          req.Name,
		Path:          req.Path,
		ContentType:   req.ContentType,
		MetadataJSON:  req.MetadataJSON,
		Reader:        req.Reader,
		ExpectedSize:  req.ExpectedSize,
		RequireSize:   req.RequireSize,
		MaxBytes:      maxBytes,
	})
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}

	if err := p.recordArtifactCatalogEvent(ctx, artifactCreateFromRecord(published.Manifest)); err != nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("record artifact catalog event: %w", err)
	}

	return action.ArtifactPublishResult{
		Name:            published.Manifest.Name,
		Path:            published.Manifest.Path,
		ContentType:     published.Manifest.ContentType,
		BlobKey:         published.Manifest.BlobKey,
		BlobAlgorithm:   published.Manifest.BlobAlgorithm,
		BlobDigest:      published.Manifest.BlobDigest,
		SizeBytes:       published.Manifest.SizeBytes,
		ArtifactShardID: published.Manifest.ArtifactShardID,
	}, nil
}

func (p *workerArtifactPublisher) ensureDurableTaskPath(ctx context.Context) error {
	p.ensureTask.Do(func() {
		p.ensureTaskErr = p.materializeDurableTaskPath(ctx)
	})

	return p.ensureTaskErr
}

func (p *workerArtifactPublisher) materializeDurableTaskPath(ctx context.Context) error {
	if p == nil {
		return nil
	}

	taskKey := strings.TrimSpace(p.taskKey)
	if p.runs == nil || p.job == nil || taskKey == "" || taskKey == dal.RootTaskKey {
		return nil
	}

	plan, err := jobexec.PlanTaskExecutions(p.job)
	if err != nil {
		return fmt.Errorf("plan artifact task path: %w", err)
	}

	path, err := taskPlanPathTo(plan, taskKey)
	if err != nil {
		return fmt.Errorf("plan artifact task path: %w", err)
	}

	if len(path) == 0 {
		return nil
	}

	if _, err := jobexec.EnsurePlannedTaskExecutions(ctx, p.runs, p.runID, path, p.cellID); err != nil {
		return fmt.Errorf("materialize artifact task path: %w", err)
	}

	return nil
}

func (p *workerArtifactPublisher) recordArtifactCatalogEvent(ctx context.Context, create dal.ArtifactCreate) error {
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  finalizeMaxAttempts,
		BaseDelay: finalizeBackoffBase,
		Metrics:   p.metrics,
		Component: "worker_artifact_catalog_event",
	})

	return retryer.Do(ctx, func() error {
		return p.catalog.RecordArtifact(ctx, create)
	}, func(attempt int, nextDelay time.Duration, err error) {
		if p.logger != nil {
			p.logger.Warn("Record artifact catalog event %s/%s failed (attempt %d/%d): %v; retrying in %v",
				create.RunID, create.Name, attempt, finalizeMaxAttempts, err, nextDelay)
		}
	})
}

func artifactCreateFromRecord(rec dal.ArtifactRecord) dal.ArtifactCreate {
	return dal.ArtifactCreate{
		RunID:           rec.RunID,
		TaskID:          stringPtrValue(rec.TaskID),
		TaskAttemptID:   stringPtrValue(rec.TaskAttemptID),
		ExecutionID:     stringPtrValue(rec.ExecutionID),
		CellID:          rec.CellID,
		Name:            rec.Name,
		Path:            rec.Path,
		ContentType:     rec.ContentType,
		BlobKey:         rec.BlobKey,
		BlobAlgorithm:   rec.BlobAlgorithm,
		BlobDigest:      rec.BlobDigest,
		SizeBytes:       rec.SizeBytes,
		ArtifactShardID: rec.ArtifactShardID,
		MetadataJSON:    rec.MetadataJSON,
	}
}

func (p *workerArtifactPublisher) ensurePublisher(ctx context.Context) (*artifact.RoutedPublisher, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.publisher != nil {
		return p.publisher, nil
	}

	publisher, err := artifact.NewPublisherForRun(ctx, p.runID, artifact.RoutedPublisherOptions{
		Manifests:       p.manifests,
		Logger:          p.logger,
		PinnedAddress:   config.PinnedArtifactAddress(),
		RegistryAddress: config.WorkerRegistryDialAddress(),
		CellID:          p.cellID,
		RetryMetrics:    p.metrics,
		MaxRunBytes:     p.maxRunBytes,
		MaxRunArtifacts: p.maxRunCount,
	})

	if err != nil {
		return nil, err
	}

	p.publisher = publisher
	return p.publisher, nil
}

func (p *workerArtifactPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.publisher == nil {
		return nil
	}

	err := p.publisher.Close()
	p.publisher = nil
	return err
}

var _ action.ArtifactPublisher = (*workerArtifactPublisher)(nil)
