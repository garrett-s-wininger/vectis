package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
)

const defaultUploadBlobChunkBytes = 128 * 1024

var ErrRunArtifactQuotaExceeded = errors.New("artifact run quota exceeded")

type PublisherOptions struct {
	Client             api.ArtifactServiceClient
	Manifests          dal.ArtifactsRepository
	ArtifactShardID    string
	UploadChunkBytes   int
	DefaultMaxBytes    int64
	MaxRunBytes        int64
	MaxRunArtifacts    int64
	DefaultContentType string
}

type Publisher struct {
	client             api.ArtifactServiceClient
	manifests          dal.ArtifactsRepository
	artifactShardID    string
	uploadChunkBytes   int
	defaultMaxBytes    int64
	maxRunBytes        int64
	maxRunArtifacts    int64
	defaultContentType string
}

type PublishRequest struct {
	RunID          string
	TaskID         string
	TaskAttemptID  string
	ExecutionID    string
	CellID         string
	Name           string
	Path           string
	ContentType    string
	MetadataJSON   *string
	Reader         io.Reader
	ExpectedSHA256 string
	ExpectedSize   int64
	RequireSize    bool
	MaxBytes       int64
}

type PublishedArtifact struct {
	Manifest dal.ArtifactRecord
	Blob     BlobDescriptor
}

func NewPublisher(opts PublisherOptions) (*Publisher, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("artifact client is required")
	}

	if opts.Manifests == nil {
		return nil, fmt.Errorf("artifact manifest repository is required")
	}

	opts.ArtifactShardID = strings.TrimSpace(opts.ArtifactShardID)
	if opts.ArtifactShardID == "" {
		return nil, fmt.Errorf("artifact shard id is required")
	}

	chunkBytes := opts.UploadChunkBytes
	if chunkBytes <= 0 {
		chunkBytes = defaultUploadBlobChunkBytes
	}

	return &Publisher{
		client:             opts.Client,
		manifests:          opts.Manifests,
		artifactShardID:    opts.ArtifactShardID,
		uploadChunkBytes:   chunkBytes,
		defaultMaxBytes:    opts.DefaultMaxBytes,
		maxRunBytes:        nonNegativeInt64(opts.MaxRunBytes),
		maxRunArtifacts:    nonNegativeInt64(opts.MaxRunArtifacts),
		defaultContentType: strings.TrimSpace(opts.DefaultContentType),
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, req PublishRequest) (PublishedArtifact, error) {
	if err := validatePublishRequest(req); err != nil {
		return PublishedArtifact{}, err
	}

	knownSize := req.RequireSize || req.ExpectedSize > 0
	if err := p.enforceRunQuota(ctx, req, req.ExpectedSize, knownSize); err != nil {
		return PublishedArtifact{}, err
	}

	desc, err := p.uploadBlob(ctx, req)
	if err != nil {
		return PublishedArtifact{}, err
	}

	if !knownSize {
		if err := p.enforceRunQuota(ctx, req, desc.Size, true); err != nil {
			return PublishedArtifact{}, err
		}
	}

	contentType := strings.TrimSpace(req.ContentType)
	if contentType == "" {
		contentType = p.defaultContentType
	}

	rec, err := p.manifests.Record(ctx, dal.ArtifactCreate{
		RunID:           req.RunID,
		TaskID:          req.TaskID,
		TaskAttemptID:   req.TaskAttemptID,
		ExecutionID:     req.ExecutionID,
		CellID:          req.CellID,
		Name:            req.Name,
		Path:            req.Path,
		ContentType:     contentType,
		BlobKey:         desc.Key,
		BlobAlgorithm:   desc.Algorithm,
		BlobDigest:      desc.Digest,
		SizeBytes:       desc.Size,
		ArtifactShardID: p.artifactShardID,
		MetadataJSON:    req.MetadataJSON,
	})

	if err != nil {
		return PublishedArtifact{}, err
	}

	return PublishedArtifact{Manifest: rec, Blob: desc}, nil
}

func (p *Publisher) enforceRunQuota(ctx context.Context, req PublishRequest, projectedSize int64, haveSize bool) error {
	if p.maxRunArtifacts <= 0 && (p.maxRunBytes <= 0 || !haveSize) {
		return nil
	}

	usage, err := p.manifests.GetRunUsageExcludingName(ctx, req.RunID, req.Name)
	if err != nil {
		return err
	}

	projectedCount := usage.Count + 1
	if p.maxRunArtifacts > 0 && projectedCount > p.maxRunArtifacts {
		return fmt.Errorf("%w: run %s would have %d artifacts, max %d", ErrRunArtifactQuotaExceeded, req.RunID, projectedCount, p.maxRunArtifacts)
	}

	if p.maxRunBytes > 0 && haveSize {
		projectedBytes := usage.SizeBytes + projectedSize
		if projectedBytes > p.maxRunBytes {
			return fmt.Errorf("%w: run %s would have %d artifact bytes, max %d", ErrRunArtifactQuotaExceeded, req.RunID, projectedBytes, p.maxRunBytes)
		}
	}

	return nil
}

func (p *Publisher) uploadBlob(ctx context.Context, req PublishRequest) (BlobDescriptor, error) {
	stream, err := p.client.UploadBlob(ctx)
	if err != nil {
		return BlobDescriptor{}, err
	}

	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = p.defaultMaxBytes
	}

	if err := stream.Send(uploadOptionsRequest(req, maxBytes)); err != nil {
		return BlobDescriptor{}, err
	}

	buf := make([]byte, p.uploadChunkBytes)
	emptyReads := 0
	for {
		if err := ctx.Err(); err != nil {
			return BlobDescriptor{}, err
		}

		n, readErr := req.Reader.Read(buf)
		if n > 0 {
			emptyReads = 0
			if err := stream.Send(&api.UploadBlobRequest{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return BlobDescriptor{}, err
			}
		}

		if readErr == nil {
			if n == 0 {
				emptyReads++
				if emptyReads >= 100 {
					return BlobDescriptor{}, io.ErrNoProgress
				}
			}

			continue
		}

		if readErr == io.EOF {
			break
		}

		return BlobDescriptor{}, fmt.Errorf("read artifact content: %w", readErr)
	}

	desc, err := stream.CloseAndRecv()
	if err != nil {
		return BlobDescriptor{}, err
	}

	return blobDescriptorFromAPI(desc)
}

func validatePublishRequest(req PublishRequest) error {
	if req.Reader == nil {
		return fmt.Errorf("artifact reader is required")
	}

	if strings.TrimSpace(req.RunID) == "" {
		return fmt.Errorf("%w: run_id is required", dal.ErrConflict)
	}

	if strings.TrimSpace(req.Name) == "" {
		return fmt.Errorf("%w: artifact name is required", dal.ErrConflict)
	}

	if req.ExpectedSize < 0 {
		return fmt.Errorf("%w: artifact expected_size must be >= 0", dal.ErrConflict)
	}

	if req.MetadataJSON != nil {
		metadata := strings.TrimSpace(*req.MetadataJSON)
		if metadata != "" && !json.Valid([]byte(metadata)) {
			return fmt.Errorf("%w: artifact metadata_json must be valid JSON", dal.ErrConflict)
		}
	}

	return nil
}

func uploadOptionsRequest(req PublishRequest, maxBytes int64) *api.UploadBlobRequest {
	out := &api.UploadBlobRequest{}

	if req.ExpectedSHA256 != "" {
		out.ExpectedSha256 = strPointer(req.ExpectedSHA256)
	}

	if req.ExpectedSize != 0 || req.RequireSize {
		out.ExpectedSize = int64Pointer(req.ExpectedSize)
	}

	if req.RequireSize {
		out.RequireSize = boolPointer(req.RequireSize)
	}

	if maxBytes > 0 {
		out.MaxBytes = int64Pointer(maxBytes)
	}

	return out
}

func blobDescriptorFromAPI(desc *api.BlobDescriptor) (BlobDescriptor, error) {
	if desc == nil {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned nil blob descriptor")
	}

	out := BlobDescriptor{
		Key:       strings.TrimSpace(desc.GetKey()),
		Algorithm: strings.TrimSpace(desc.GetAlgorithm()),
		Digest:    strings.TrimSpace(desc.GetDigest()),
		Size:      desc.GetSize(),
	}

	if out.Key == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned empty blob key")
	}

	if out.Algorithm == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned empty blob algorithm")
	}

	if out.Digest == "" {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned empty blob digest")
	}

	if out.Size < 0 {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned negative blob size")
	}

	if err := ValidateBlobDescriptor(out); err != nil {
		return BlobDescriptor{}, fmt.Errorf("artifact upload returned invalid blob descriptor: %w", err)
	}

	return out, nil
}

func strPointer(v string) *string {
	return &v
}

func int64Pointer(v int64) *int64 {
	return &v
}

func boolPointer(v bool) *bool {
	return &v
}

func nonNegativeInt64(v int64) int64 {
	if v < 0 {
		return 0
	}

	return v
}
