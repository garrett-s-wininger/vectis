package workercore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const artifactCallbackChunkSize = 64 * 1024

type Session struct {
	session *api.WorkerCoreTaskSession
}

func NewSession(session *api.WorkerCoreTaskSession) (Session, error) {
	if session == nil {
		return Session{}, fmt.Errorf("task session is required")
	}

	sessionID := strings.TrimSpace(session.GetSessionId())
	if sessionID == "" {
		return Session{}, fmt.Errorf("task session id is required")
	}

	if (session.GetLogsEnabled() || session.GetArtifactsEnabled()) && strings.TrimSpace(session.GetShellEndpoint()) == "" {
		return Session{}, fmt.Errorf("shell endpoint is required for callback-enabled task sessions")
	}

	return Session{session: session}, nil
}

func (s Session) ID() string {
	return s.session.GetSessionId()
}

func (s Session) ShellEndpoint() string {
	return s.session.GetShellEndpoint()
}

func (s Session) LogsEnabled() bool {
	return s.session.GetLogsEnabled()
}

func (s Session) ArtifactsEnabled() bool {
	return s.session.GetArtifactsEnabled()
}

func (s Session) WorkloadIdentity() *api.WorkerCoreWorkloadIdentity {
	return s.session.GetWorkloadIdentity()
}

func (s Session) ActionLocks() []*api.WorkerCoreActionLock {
	return append([]*api.WorkerCoreActionLock(nil), s.session.GetActionLocks()...)
}

func (s Session) OpenLogStream(ctx context.Context) (*LogStream, error) {
	if !s.LogsEnabled() {
		return nil, fmt.Errorf("task session %q does not enable log callbacks", s.ID())
	}

	conn, err := DialUnixShell(ctx, s.ShellEndpoint())
	if err != nil {
		return nil, err
	}

	stream, err := api.NewWorkerCoreShellServiceClient(conn).StreamLogs(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open worker core shell log stream: %w", err)
	}

	return &LogStream{
		sessionID: s.ID(),
		conn:      conn,
		stream:    stream,
	}, nil
}

func (s Session) SendLog(ctx context.Context, chunk *api.LogChunk) error {
	stream, err := s.OpenLogStream(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(chunk); err != nil {
		_ = stream.Close()
		return err
	}

	return stream.Close()
}

type LogStream struct {
	sessionID string
	conn      io.Closer
	stream    api.WorkerCoreShellService_StreamLogsClient
	mu        sync.Mutex
	closed    bool
}

func (s *LogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return fmt.Errorf("log chunk is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("log stream is closed")
	}

	return s.stream.Send(&api.WorkerCoreLogChunk{
		SessionId: proto.String(s.sessionID),
		Chunk:     chunk,
	})
}

func (s *LogStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	_, streamErr := s.stream.CloseAndRecv()
	closeErr := s.conn.Close()
	if streamErr != nil {
		return streamErr
	}

	return closeErr
}

type ArtifactRequest struct {
	Name         string
	Path         string
	ContentType  string
	MetadataJSON string
	ExpectedSize int64
	RequireSize  bool
	MaxBytes     int64
	Reader       io.Reader
}

type Artifact struct {
	Name            string
	Path            string
	ContentType     string
	BlobKey         string
	BlobAlgorithm   string
	BlobDigest      string
	SizeBytes       int64
	ArtifactShardID string
}

func (s Session) PublishArtifact(ctx context.Context, req ArtifactRequest) (Artifact, error) {
	if !s.ArtifactsEnabled() {
		return Artifact{}, fmt.Errorf("task session %q does not enable artifact callbacks", s.ID())
	}

	conn, err := DialUnixShell(ctx, s.ShellEndpoint())
	if err != nil {
		return Artifact{}, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)

	stream, err := api.NewWorkerCoreShellServiceClient(conn).PublishArtifact(ctx)
	if err != nil {
		return Artifact{}, fmt.Errorf("open worker core shell artifact stream: %w", err)
	}

	if err := stream.Send(&api.WorkerCoreArtifactChunk{Metadata: artifactMetadataProto(s.ID(), req)}); err != nil {
		return Artifact{}, fmt.Errorf("send artifact metadata: %w", err)
	}

	reader := req.Reader
	if reader == nil {
		reader = bytes.NewReader(nil)
	}

	buf := make([]byte, artifactCallbackChunkSize)
	for {
		n, readErr := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&api.WorkerCoreArtifactChunk{Data: append([]byte(nil), buf[:n]...)}); err != nil {
				return Artifact{}, fmt.Errorf("send artifact data: %w", err)
			}
		}

		if readErr == io.EOF {
			break
		}

		if readErr != nil {
			return Artifact{}, fmt.Errorf("read artifact data: %w", readErr)
		}
	}

	result, err := stream.CloseAndRecv()
	if err != nil {
		return Artifact{}, fmt.Errorf("publish artifact via worker core shell: %w", err)
	}

	return artifactFromProto(result), nil
}

func artifactMetadataProto(sessionID string, req ArtifactRequest) *api.WorkerCoreArtifactMetadata {
	metadata := &api.WorkerCoreArtifactMetadata{
		SessionId:    proto.String(sessionID),
		Name:         proto.String(req.Name),
		Path:         proto.String(req.Path),
		ContentType:  proto.String(req.ContentType),
		ExpectedSize: proto.Int64(req.ExpectedSize),
		RequireSize:  proto.Bool(req.RequireSize),
		MaxBytes:     proto.Int64(req.MaxBytes),
	}

	if strings.TrimSpace(req.MetadataJSON) != "" {
		metadata.MetadataJson = proto.String(req.MetadataJSON)
	}

	return metadata
}

func artifactFromProto(in *api.WorkerCoreArtifact) Artifact {
	if in == nil {
		return Artifact{}
	}

	return Artifact{
		Name:            in.GetName(),
		Path:            in.GetPath(),
		ContentType:     in.GetContentType(),
		BlobKey:         in.GetBlobKey(),
		BlobAlgorithm:   in.GetBlobAlgorithm(),
		BlobDigest:      in.GetBlobDigest(),
		SizeBytes:       in.GetSizeBytes(),
		ArtifactShardID: in.GetArtifactShardId(),
	}
}
