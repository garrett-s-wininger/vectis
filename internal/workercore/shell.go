package workercore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ShellServer struct {
	api.UnimplementedWorkerCoreShellServiceServer

	mu       sync.RWMutex
	sessions map[string]TaskSession
}

func NewShellServer() *ShellServer {
	return &ShellServer{sessions: map[string]TaskSession{}}
}

func (s *ShellServer) RegisterSession(session TaskSession) (func(), error) {
	if s == nil {
		return nil, fmt.Errorf("worker core shell is not configured")
	}

	if session == nil {
		return nil, fmt.Errorf("worker core shell session is required")
	}

	sessionID := strings.TrimSpace(session.SessionID())
	if sessionID == "" {
		return nil, fmt.Errorf("worker core shell session id is required")
	}

	if session.LogClient() != nil && strings.TrimSpace(session.RunID()) == "" {
		return nil, fmt.Errorf("worker core shell log session run_id is required")
	}

	s.mu.Lock()
	if s.sessions == nil {
		s.sessions = map[string]TaskSession{}
	}
	s.sessions[sessionID] = session
	s.mu.Unlock()

	return func() {
		s.mu.Lock()
		delete(s.sessions, sessionID)
		s.mu.Unlock()
	}, nil
}

func (s *ShellServer) StreamLogs(stream api.WorkerCoreShellService_StreamLogsServer) error {
	var sessionID string
	var session TaskSession
	var logStream interface {
		Send(*api.LogChunk) error
		CloseSend() error
	}

	closeLogStream := func() error {
		if logStream == nil {
			return nil
		}

		stream := logStream
		logStream = nil
		if closer, ok := stream.(interface{ CloseAndRecv() error }); ok {
			return closer.CloseAndRecv()
		}

		return stream.CloseSend()
	}

	defer func() {
		_ = closeLogStream()
	}()

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if err := closeLogStream(); err != nil {
				return status.Errorf(codes.Unavailable, "close shell log stream: %v", err)
			}

			return stream.SendAndClose(&api.Empty{})
		}

		if err != nil {
			return err
		}

		if msg == nil || msg.GetChunk() == nil {
			return status.Error(codes.InvalidArgument, "worker core log chunk is required")
		}

		msgSessionID := strings.TrimSpace(msg.GetSessionId())
		if msgSessionID == "" {
			return status.Error(codes.InvalidArgument, "worker core log chunk session_id is required")
		}

		if sessionID == "" {
			sessionID = msgSessionID
			session, err = s.session(sessionID)
			if err != nil {
				return err
			}

			if session.LogClient() == nil {
				return status.Error(codes.FailedPrecondition, "worker core shell session has no log client")
			}

			logStream, err = session.LogClient().StreamLogs(stream.Context())
			if err != nil {
				return status.Errorf(codes.Unavailable, "open shell log stream: %v", err)
			}
		} else if msgSessionID != sessionID {
			return status.Error(codes.InvalidArgument, "worker core log stream changed session_id")
		}

		if err := validateShellLogChunk(session, msg.GetChunk()); err != nil {
			return err
		}

		if err := logStream.Send(msg.GetChunk()); err != nil {
			return status.Errorf(codes.Unavailable, "forward shell log chunk: %v", err)
		}
	}
}

func validateShellLogChunk(session TaskSession, chunk *api.LogChunk) error {
	if session == nil {
		return status.Error(codes.FailedPrecondition, "worker core shell session is required")
	}

	if chunk == nil {
		return status.Error(codes.InvalidArgument, "worker core log chunk is required")
	}

	wantRunID := strings.TrimSpace(session.RunID())
	if wantRunID == "" {
		return status.Error(codes.FailedPrecondition, "worker core shell session run_id is required")
	}

	gotRunID := strings.TrimSpace(chunk.GetRunId())
	if gotRunID == "" {
		return status.Error(codes.InvalidArgument, "worker core log chunk run_id is required")
	}

	if gotRunID != wantRunID {
		return status.Errorf(codes.InvalidArgument, "worker core log chunk run_id %q does not match session run_id %q", gotRunID, wantRunID)
	}

	return nil
}

func (s *ShellServer) PublishArtifact(stream api.WorkerCoreShellService_PublishArtifactServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "worker core artifact metadata is required")
		}

		return err
	}

	if first == nil || first.GetMetadata() == nil {
		return status.Error(codes.InvalidArgument, "worker core artifact metadata is required")
	}

	meta := first.GetMetadata()
	sessionID := strings.TrimSpace(meta.GetSessionId())
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "worker core artifact session_id is required")
	}

	session, err := s.session(sessionID)
	if err != nil {
		return err
	}

	publisher := session.ArtifactPublisher()
	if publisher == nil {
		return status.Error(codes.FailedPrecondition, "worker core shell session has no artifact publisher")
	}

	reader, writer := io.Pipe()
	recvDone := make(chan error, 1)
	go func() {
		recvDone <- receiveArtifactData(stream.Context(), stream, first.GetData(), writer)
	}()

	metadataJSON := optionalString(meta.MetadataJson)
	result, publishErr := publisher.PublishArtifact(stream.Context(), action.ArtifactPublishRequest{
		Name:         meta.GetName(),
		Path:         meta.GetPath(),
		ContentType:  meta.GetContentType(),
		MetadataJSON: metadataJSON,
		Reader:       reader,
		ExpectedSize: meta.GetExpectedSize(),
		RequireSize:  meta.GetRequireSize(),
		MaxBytes:     meta.GetMaxBytes(),
	})

	if publishErr != nil {
		_ = reader.CloseWithError(publishErr)
		<-recvDone
		return status.Errorf(codes.Internal, "publish shell artifact: %v", publishErr)
	}

	if recvErr := <-recvDone; recvErr != nil {
		return recvErr
	}

	return stream.SendAndClose(ArtifactResultProto(result))
}

func (s *ShellServer) session(sessionID string) (TaskSession, error) {
	if s == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core shell is not configured")
	}

	s.mu.RLock()
	session := s.sessions[sessionID]
	s.mu.RUnlock()

	if session == nil {
		return nil, status.Errorf(codes.PermissionDenied, "worker core shell session %q is not registered", sessionID)
	}

	return session, nil
}

func receiveArtifactData(ctx context.Context, stream api.WorkerCoreShellService_PublishArtifactServer, first []byte, writer *io.PipeWriter) error {
	closeWith := func(err error) error {
		if err != nil {
			_ = writer.CloseWithError(err)
			return err
		}

		return writer.Close()
	}

	if len(first) > 0 {
		if _, err := writer.Write(first); err != nil {
			return closeWith(err)
		}
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return closeWith(nil)
		}

		if err != nil {
			return closeWith(err)
		}

		if msg == nil {
			continue
		}

		if msg.GetMetadata() != nil {
			return closeWith(status.Error(codes.InvalidArgument, "worker core artifact metadata may only be sent once"))
		}

		if data := msg.GetData(); len(data) > 0 {
			if _, err := writer.Write(data); err != nil {
				return closeWith(err)
			}
		}

		select {
		case <-ctx.Done():
			return closeWith(ctx.Err())
		default:
		}
	}
}

func optionalString(value *string) *string {
	if value == nil {
		return nil
	}

	out := *value
	return &out
}
