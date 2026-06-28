package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
	"vectis/internal/resolver"
)

type ReaderOptions struct {
	Logger          interfaces.Logger
	PinnedAddress   string
	PinnedShardID   string
	RegistryAddress string
	Registry        *registry.Registry
	RetryMetrics    backoff.RetryMetrics
}

type RoutedReader struct {
	client  api.ArtifactServiceClient
	connID  string
	cleanup func()
}

func NewReaderForArtifact(ctx context.Context, rec dal.ArtifactRecord, opts ReaderOptions) (*RoutedReader, error) {
	endpoint, err := resolveArtifactReadEndpoint(ctx, rec.ArtifactShardID, opts)
	if err != nil {
		return nil, err
	}

	logger := artifactRouteLogger(opts.Logger)
	conn, cleanup, err := resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_ARTIFACT, endpoint.Address, logger, nil, opts.RetryMetrics)
	if err != nil {
		return nil, err
	}

	return &RoutedReader{
		client:  api.NewArtifactServiceClient(conn),
		connID:  endpoint.ID,
		cleanup: cleanup,
	}, nil
}

func (r *RoutedReader) Close() error {
	if r == nil || r.cleanup == nil {
		return nil
	}

	r.cleanup()
	r.cleanup = nil
	return nil
}

func (r *RoutedReader) StatBlob(ctx context.Context, key string) (BlobDescriptor, error) {
	if r == nil || r.client == nil {
		return BlobDescriptor{}, fmt.Errorf("artifact reader is not configured")
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact blob key is required")
	}

	resp, err := r.client.StatBlob(ctx, &api.GetBlobRequest{Key: &key})
	if err != nil {
		return BlobDescriptor{}, err
	}

	return blobDescriptorFromAPI(resp)
}

func (r *RoutedReader) WriteBlob(ctx context.Context, key string, w io.Writer) (BlobDescriptor, error) {
	if r == nil || r.client == nil {
		return BlobDescriptor{}, fmt.Errorf("artifact reader is not configured")
	}

	key = strings.TrimSpace(key)
	if key == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact blob key is required")
	}

	stream, err := r.client.ReadBlob(ctx, &api.GetBlobRequest{Key: &key})
	if err != nil {
		return BlobDescriptor{}, err
	}

	var desc BlobDescriptor
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return desc, err
		}

		if resp.GetBlob() != nil {
			parsed, err := blobDescriptorFromAPI(resp.GetBlob())
			if err != nil {
				return desc, err
			}

			desc = parsed
		}

		if data := resp.GetData(); len(data) > 0 {
			if _, err := w.Write(data); err != nil {
				return desc, err
			}
		}
	}

	if desc.Key == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact read returned no blob descriptor")
	}

	return desc, nil
}

func resolveArtifactReadEndpoint(ctx context.Context, shardID string, opts ReaderOptions) (ArtifactEndpoint, error) {
	shardID = strings.TrimSpace(shardID)
	if shardID == "" {
		return ArtifactEndpoint{}, fmt.Errorf("artifact shard id is required")
	}

	if pinnedAddress := strings.TrimSpace(opts.PinnedAddress); pinnedAddress != "" {
		id := strings.TrimSpace(opts.PinnedShardID)
		if id == "" {
			id = pinnedArtifactShardID
		}

		return ArtifactEndpoint{
			ID:       id,
			Address:  pinnedAddress,
			Writable: true,
		}, nil
	}

	reg := opts.Registry
	if reg == nil {
		var err error
		reg, err = resolver.NewRegistryClient(ctx, opts.RegistryAddress, artifactRouteLogger(opts.Logger), nil, opts.RetryMetrics)
		if err != nil {
			return ArtifactEndpoint{}, fmt.Errorf("artifact registry client: %w", err)
		}
		defer reg.Close()
	}

	address, err := reg.InstanceAddress(ctx, api.Component_COMPONENT_ARTIFACT, shardID)
	if err != nil {
		return ArtifactEndpoint{}, err
	}

	return ArtifactEndpoint{
		ID:      shardID,
		Address: address,
	}, nil
}

func DefaultReaderOptions(logger interfaces.Logger, retryMetrics backoff.RetryMetrics) ReaderOptions {
	return ReaderOptions{
		Logger:          logger,
		PinnedAddress:   config.PinnedArtifactAddress(),
		RegistryAddress: config.APIRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
	}
}
