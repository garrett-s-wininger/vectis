package workercore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/interfaces"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ServiceOptions struct {
	Description    CoreDescription
	Logger         interfaces.Logger
	ActionResolver actionregistry.Resolver
}

type Service struct {
	api.UnimplementedWorkerCoreServiceServer

	core           Core
	description    CoreDescription
	logger         interfaces.Logger
	actionResolver actionregistry.Resolver
}

func NewService(core Core, opts ServiceOptions) *Service {
	return &Service{
		core:           core,
		description:    opts.Description,
		logger:         opts.Logger,
		actionResolver: opts.ActionResolver,
	}
}

func (s *Service) DescribeCore(context.Context, *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error) {
	if s == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core service is not configured")
	}

	return CoreDescriptionProto(s.description), nil
}

func (s *Service) ExecuteTask(ctx context.Context, req *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
	if s == nil || s.core == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core is not configured")
	}

	execReq, cleanup, err := s.executeTaskRequest(ctx, req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer cleanup()

	if err := s.core.ExecuteTask(ctx, execReq); err != nil {
		outcome := api.RunOutcome_RUN_OUTCOME_FAILURE
		reasonCode := workersdk.ReasonExecutionFailed
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			outcome = api.RunOutcome_RUN_OUTCOME_UNKNOWN
			reasonCode = workersdk.ReasonCancelled
		}

		return &api.ExecuteWorkerCoreTaskResponse{
			Outcome:    outcome.Enum(),
			Message:    proto.String(err.Error()),
			ReasonCode: proto.String(reasonCode),
		}, nil
	}

	return &api.ExecuteWorkerCoreTaskResponse{Outcome: api.RunOutcome_RUN_OUTCOME_SUCCESS.Enum()}, nil
}

func (s *Service) CancelTask(ctx context.Context, req *api.CancelWorkerCoreTaskRequest) (*api.Empty, error) {
	if s == nil || s.core == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core is not configured")
	}

	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, status.Error(codes.InvalidArgument, "task session id is required")
	}

	if cancellable, ok := s.core.(CancellableCore); ok {
		if err := cancellable.CancelTask(ctx, CancelTaskRequest{
			SessionID: sessionID,
			RunID:     req.GetRunId(),
			TaskKey:   req.GetTaskKey(),
			Reason:    req.GetReason(),
		}); err != nil {
			return nil, status.Errorf(codes.Internal, "cancel worker core task: %v", err)
		}
	}

	return &api.Empty{}, nil
}

func (s *Service) executeTaskRequest(ctx context.Context, req *api.ExecuteWorkerCoreTaskRequest) (ExecuteTaskRequest, func(), error) {
	if req == nil {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("execute task request is required")
	}

	if req.GetJob() == nil {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("job is required")
	}

	if strings.TrimSpace(req.GetTaskKey()) == "" {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("task key is required")
	}

	session := req.GetSession()
	if session == nil {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("task session is required")
	}

	sessionID := strings.TrimSpace(session.GetSessionId())
	if sessionID == "" {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("task session id is required")
	}

	shellEndpoint := strings.TrimSpace(session.GetShellEndpoint())
	if (session.GetLogsEnabled() || session.GetArtifactsEnabled()) && shellEndpoint == "" {
		return ExecuteTaskRequest{}, func() {}, fmt.Errorf("shell endpoint is required for callback-enabled task sessions")
	}

	workloadIdentity := workloadIdentityFromProto(session.GetWorkloadIdentity())
	if err := ValidateTaskSessionIdentity(req.GetJob(), sessionID, workloadIdentity); err != nil {
		return ExecuteTaskRequest{}, func() {}, err
	}

	var cleanupFuncs []func()
	var logClient interfaces.LogClient
	if session.GetLogsEnabled() {
		logClient = newCallbackLogClient(sessionID, shellEndpoint)
		cleanupFuncs = append(cleanupFuncs, func() { _ = logClient.Close() })
	}

	actionLocks := actionLocksFromProto(session.GetActionLocks())
	actionResolver := s.actionResolver
	if actionResolver == nil && len(actionLocks) > 0 {
		actionResolver = frozenDescriptorResolverFromLocks(actionLocks)
	}

	taskSession := NewTaskSession(TaskSessionOptions{
		SessionID:         sessionID,
		ShellEndpoint:     shellEndpoint,
		Logger:            s.logger,
		LogClient:         logClient,
		ArtifactPublisher: artifactPublisherForSession(session),
		WorkloadIdentity:  workloadIdentity,
		ActionLocks:       actionLocks,
		ActionResolver:    actionResolver,
		SecretFiles:       secretFilesFromProto(session.GetSecretFiles()),
	})

	return ExecuteTaskRequest{
			Job:     req.GetJob(),
			TaskKey: req.GetTaskKey(),
			Session: taskSession,
		}, func() {
			for i := len(cleanupFuncs) - 1; i >= 0; i-- {
				cleanupFuncs[i]()
			}
		}, nil
}

func artifactPublisherForSession(session *api.WorkerCoreTaskSession) action.ArtifactPublisher {
	if session == nil || !session.GetArtifactsEnabled() {
		return nil
	}

	return newCallbackArtifactPublisher(session.GetSessionId(), session.GetShellEndpoint())
}

type frozenDescriptorResolver map[string]actionregistry.Descriptor

func frozenDescriptorResolverFromLocks(locks []actionregistry.ActionLock) frozenDescriptorResolver {
	out := frozenDescriptorResolver{}
	for _, lock := range locks {
		uses := strings.TrimSpace(lock.Uses)
		if uses == "" {
			continue
		}

		if _, exists := out[uses]; !exists {
			out[uses] = lock.Descriptor
		}
	}

	return out
}

func (r frozenDescriptorResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r[strings.TrimSpace(uses)]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("no frozen action descriptor for %q", uses)
	}

	return descriptor, nil
}
