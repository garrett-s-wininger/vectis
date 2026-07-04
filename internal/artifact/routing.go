package artifact

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
	"vectis/internal/resolver"
)

const pinnedArtifactShardID = "pinned"

type RoutedPublisherOptions struct {
	Manifests          dal.ArtifactsRepository
	Logger             interfaces.Logger
	PinnedAddress      string
	PinnedShardID      string
	RegistryAddress    string
	Registry           *registry.Registry
	CellID             string
	RetryMetrics       backoff.RetryMetrics
	UploadChunkBytes   int
	DefaultMaxBytes    int64
	MaxRunBytes        int64
	MaxRunArtifacts    int64
	DefaultContentType string
}

type ArtifactEndpoint struct {
	ID       string
	Address  string
	Writable bool
}

type RoutedPublisher struct {
	*Publisher
	Endpoint ArtifactEndpoint
	cleanup  func()
}

func NewPublisherForRun(ctx context.Context, runID string, opts RoutedPublisherOptions) (*RoutedPublisher, error) {
	endpoint, err := resolveArtifactPublishEndpoint(ctx, runID, opts)
	if err != nil {
		return nil, err
	}

	logger := artifactRouteLogger(opts.Logger)
	conn, cleanup, err := resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_ARTIFACT, endpoint.Address, logger, nil, opts.RetryMetrics)
	if err != nil {
		return nil, err
	}

	publisher, err := NewPublisher(PublisherOptions{
		Client:             api.NewArtifactServiceClient(conn),
		Manifests:          opts.Manifests,
		ArtifactShardID:    endpoint.ID,
		UploadChunkBytes:   opts.UploadChunkBytes,
		DefaultMaxBytes:    opts.DefaultMaxBytes,
		MaxRunBytes:        opts.MaxRunBytes,
		MaxRunArtifacts:    opts.MaxRunArtifacts,
		DefaultContentType: opts.DefaultContentType,
	})

	if err != nil {
		cleanup()
		return nil, err
	}

	return &RoutedPublisher{
		Publisher: publisher,
		Endpoint:  endpoint,
		cleanup:   cleanup,
	}, nil
}

func (p *RoutedPublisher) Close() error {
	if p == nil || p.cleanup == nil {
		return nil
	}

	p.cleanup()
	p.cleanup = nil
	return nil
}

func resolveArtifactPublishEndpoint(ctx context.Context, runID string, opts RoutedPublisherOptions) (ArtifactEndpoint, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return ArtifactEndpoint{}, fmt.Errorf("run id is required")
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

		defer func(closer interface{ Close() error }) { _ = closer.Close() }(reg)
	}

	endpoints, err := listArtifactPublishEndpoints(ctx, reg, opts.CellID)
	if err != nil {
		return ArtifactEndpoint{}, err
	}

	writable := make([]ArtifactEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint.Writable {
			writable = append(writable, endpoint)
		}
	}

	if len(writable) == 0 {
		return ArtifactEndpoint{}, fmt.Errorf("no writable artifact endpoints available")
	}

	return chooseArtifactEndpoint(runID, writable), nil
}

func listArtifactPublishEndpoints(ctx context.Context, reg *registry.Registry, cellID string) ([]ArtifactEndpoint, error) {
	if reg == nil {
		return nil, fmt.Errorf("registry client is required")
	}

	entries, err := reg.ListRegistrations(ctx, api.Component_COMPONENT_ARTIFACT, registry.DefaultServiceMetadataForCell(cellID))
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 || !artifactEntriesHaveWritable(entries) {
		allEntries, err := reg.ListRegistrations(ctx, api.Component_COMPONENT_ARTIFACT, nil)
		if err != nil {
			return nil, err
		}

		if len(entries) == 0 || artifactEntriesHaveWritable(allEntries) {
			entries = allEntries
		}
	}

	seen := make(map[string]ArtifactEndpoint, len(entries))
	for _, entry := range entries {
		address := strings.TrimSpace(entry.GetAddress())
		if address == "" {
			continue
		}

		id := strings.TrimSpace(entry.GetInstanceId())
		if id == "" {
			id = address
		}

		metadata := entry.GetMetadata()
		writable := metadata[registry.MetadataArtifactWriteState] != registry.ArtifactWriteStateReadOnly
		seen[id] = ArtifactEndpoint{ID: id, Address: address, Writable: writable}
	}

	out := make([]ArtifactEndpoint, 0, len(seen))
	for _, endpoint := range seen {
		out = append(out, endpoint)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})

	if len(out) == 0 {
		return nil, fmt.Errorf("no artifact registrations available")
	}

	return out, nil
}

func artifactEntriesHaveWritable(entries []*api.RegistryEntry) bool {
	for _, entry := range entries {
		if entry == nil {
			continue
		}

		if entry.GetMetadata()[registry.MetadataArtifactWriteState] != registry.ArtifactWriteStateReadOnly {
			return true
		}
	}

	return false
}

func chooseArtifactEndpoint(runID string, endpoints []ArtifactEndpoint) ArtifactEndpoint {
	var best ArtifactEndpoint
	var bestScore uint64
	for i, endpoint := range endpoints {
		score := artifactRendezvousScore(runID, endpoint.ID)
		if i == 0 || score > bestScore {
			best = endpoint
			bestScore = score
		}
	}

	return best
}

func artifactRendezvousScore(runID, endpointID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(runID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(endpointID))

	return h.Sum64()
}

func artifactRouteLogger(logger interfaces.Logger) interfaces.Logger {
	if logger != nil {
		return logger
	}

	return interfaces.NewLogger("artifact-client")
}
