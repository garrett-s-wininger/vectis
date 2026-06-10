package main

import (
	"context"
	"fmt"
	"sync"

	"vectis/internal/action"
	"vectis/internal/artifact"
	"vectis/internal/backoff"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
)

type workerArtifactPublisher struct {
	logger    interfaces.Logger
	manifests dal.ArtifactsRepository
	metrics   backoff.RetryMetrics

	runID         string
	taskID        string
	taskAttemptID string
	executionID   string
	cellID        string
	maxBytes      int64

	mu        sync.Mutex
	publisher *artifact.RoutedPublisher
}

func (w *worker) newArtifactPublisher(env *cell.ExecutionEnvelope) *workerArtifactPublisher {
	if w == nil || w.artifactManifests == nil || env == nil {
		return nil
	}

	return &workerArtifactPublisher{
		logger:        w.logger,
		manifests:     w.artifactManifests,
		metrics:       w.retryMetrics,
		runID:         env.RunID,
		taskID:        env.TaskID,
		taskAttemptID: env.TaskAttemptID,
		executionID:   env.ExecutionID,
		cellID:        env.CellID,
		maxBytes:      w.artifactMaxBytes,
	}
}

func (p *workerArtifactPublisher) PublishArtifact(ctx context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	if p == nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("artifact publisher is not configured")
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
		MaxBytes:      maxBytes,
	})
	if err != nil {
		return action.ArtifactPublishResult{}, err
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
