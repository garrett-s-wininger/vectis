package workercore

import (
	"context"
	"errors"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/workloadidentity"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestRemoteCoreExecuteTaskSendsShellSessionContract(t *testing.T) {
	jobID := "job-remote-core"
	runID := "run-remote-core"
	nodeID := "root"
	uses := "builtins/shell"
	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
		},
	}

	var captured *api.ExecuteWorkerCoreTaskRequest
	core := NewRemoteCore(fakeWorkerCoreClient{
		execute: func(_ context.Context, req *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
			captured = req
			return &api.ExecuteWorkerCoreTaskResponse{Outcome: api.RunOutcome_RUN_OUTCOME_SUCCESS.Enum()}, nil
		},
	})

	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job:     job,
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID:     "session-1",
			ShellEndpoint: "unix:///tmp/vectis-worker-core-shell.sock",
			LogClient:     mocks.NewMockLogClient(),
			Logger:        mocks.NewMockLogger(),
			ArtifactPublisher: fakeArtifactPublisher{
				result: action.ArtifactPublishResult{Name: "artifact"},
			},
			WorkloadIdentity: &workloadidentity.Identity{
				SPIFFEID:      "spiffe://vectis.local/cell/local/job/job-remote-core/run/run-remote-core/execution/session-1",
				TrustDomain:   "vectis.local",
				NamespacePath: "/team",
				CellID:        "local",
				JobID:         jobID,
				RunID:         runID,
				ExecutionID:   "session-1",
				X509SVID:      &workloadidentity.X509SVID{SPIFFEID: "spiffe://vectis.local/workload"},
			},
			ActionLocks: []actionregistry.ActionLock{
				{
					NodeID:   "node-1",
					NodePath: "root",
					Uses:     "actions/example",
					Descriptor: actionregistry.Descriptor{
						CanonicalName: "actions/example",
						DisplayName:   "Example",
						Version:       "v1",
						Digest:        "sha256:one",
						Source:        actionregistry.SourceBuiltin,
						Runtime:       actionregistry.RuntimeProcess,
						RuntimeConfig: map[string]string{"entrypoint": "run"},
						InputSchema: actionregistry.InputSchema{
							AllowUnknown: true,
							Fields: []actionregistry.InputField{
								{Name: "url", Type: action.FieldURL, Required: true},
							},
						},
						PortSchema: []actionregistry.PortSpec{
							{Name: "main", Min: 1, Max: action.PortUnlimited, Primary: true},
						},
						LocalOnly:    true,
						Capabilities: []actionregistry.Capability{actionregistry.CapabilityNetwork},
						Status:       actionregistry.DescriptorStatusActive,
						StatusReason: "ok",
					},
				},
			},
		}),
	})

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if captured == nil {
		t.Fatal("remote core did not receive request")
	}

	if captured.GetJob() != job {
		t.Fatal("request job was not forwarded")
	}

	if captured.GetTaskKey() != dal.RootTaskKey {
		t.Fatalf("task key = %q, want %q", captured.GetTaskKey(), dal.RootTaskKey)
	}

	session := captured.GetSession()
	if session.GetSessionId() != "session-1" {
		t.Fatalf("session id = %q", session.GetSessionId())
	}

	if session.GetShellEndpoint() != "unix:///tmp/vectis-worker-core-shell.sock" {
		t.Fatalf("shell endpoint = %q", session.GetShellEndpoint())
	}

	if !session.GetLogsEnabled() {
		t.Fatal("logs_enabled = false, want true")
	}

	if !session.GetArtifactsEnabled() {
		t.Fatal("artifacts_enabled = false, want true")
	}

	identity := session.GetWorkloadIdentity()
	if identity.GetSpiffeId() == "" || identity.GetX509SvidSpiffeId() != "spiffe://vectis.local/workload" {
		t.Fatalf("workload identity not forwarded: %#v", identity)
	}

	locks := session.GetActionLocks()
	if len(locks) != 1 {
		t.Fatalf("action locks = %d, want 1", len(locks))
	}

	lock := locks[0]
	if lock.GetNodePath() != "root" || lock.GetUses() != "actions/example" {
		t.Fatalf("action lock = %#v", lock)
	}

	desc := lock.GetDescriptor_()
	if desc.GetCanonicalName() != "actions/example" || desc.GetRuntime() != string(actionregistry.RuntimeProcess) {
		t.Fatalf("descriptor = %#v", desc)
	}

	if desc.GetRuntimeConfig()["entrypoint"] != "run" {
		t.Fatalf("runtime config = %#v", desc.GetRuntimeConfig())
	}

	if !desc.GetInputSchema().GetAllowUnknown() || desc.GetInputSchema().GetFields()[0].GetType() != string(action.FieldURL) {
		t.Fatalf("input schema = %#v", desc.GetInputSchema())
	}

	if desc.GetPortSchema()[0].GetMax() != action.PortUnlimited {
		t.Fatalf("port schema max = %d, want %d", desc.GetPortSchema()[0].GetMax(), action.PortUnlimited)
	}

	if len(desc.GetCapabilities()) != 1 || desc.GetCapabilities()[0] != string(actionregistry.CapabilityNetwork) {
		t.Fatalf("capabilities = %#v", desc.GetCapabilities())
	}
}

func TestRemoteCoreExecuteTaskRequiresCallbackEndpoint(t *testing.T) {
	core := NewRemoteCore(fakeWorkerCoreClient{})
	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job:     &api.Job{},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID: "session-1",
			LogClient: mocks.NewMockLogClient(),
			Logger:    mocks.NewMockLogger(),
		}),
	})

	if err == nil || !strings.Contains(err.Error(), "requires a shell endpoint") {
		t.Fatalf("ExecuteTask error = %v, want shell endpoint validation", err)
	}
}

func TestRemoteCoreExecuteTaskFailureOutcome(t *testing.T) {
	core := NewRemoteCore(fakeWorkerCoreClient{
		execute: func(context.Context, *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
			return &api.ExecuteWorkerCoreTaskResponse{
				Outcome:    api.RunOutcome_RUN_OUTCOME_FAILURE.Enum(),
				Message:    proto.String("jenkins failed"),
				ReasonCode: proto.String("jenkins.stage_failed"),
			}, nil
		},
	})

	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job:     &api.Job{},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID: "session-1",
			Logger:    mocks.NewMockLogger(),
		}),
	})

	if err == nil || !strings.Contains(err.Error(), "jenkins failed") {
		t.Fatalf("ExecuteTask error = %v, want failure message", err)
	}

	var resultErr *TaskResultError
	if !errors.As(err, &resultErr) {
		t.Fatalf("ExecuteTask error type = %T, want *TaskResultError", err)
	}

	if resultErr.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", resultErr.Outcome)
	}

	if resultErr.ReasonCode != "jenkins.stage_failed" {
		t.Fatalf("reason code = %q, want provider reason", resultErr.ReasonCode)
	}
}

func TestRemoteCoreExecuteTaskDefaultReasonCode(t *testing.T) {
	core := NewRemoteCore(fakeWorkerCoreClient{
		execute: func(context.Context, *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
			return &api.ExecuteWorkerCoreTaskResponse{
				Outcome: api.RunOutcome_RUN_OUTCOME_UNKNOWN.Enum(),
			}, nil
		},
	})

	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job:     &api.Job{},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID: "session-1",
			Logger:    mocks.NewMockLogger(),
		}),
	})

	var resultErr *TaskResultError
	if !errors.As(err, &resultErr) {
		t.Fatalf("ExecuteTask error type = %T, want *TaskResultError", err)
	}

	if resultErr.ReasonCode != workersdk.ReasonUnknown {
		t.Fatalf("reason code = %q, want %q", resultErr.ReasonCode, workersdk.ReasonUnknown)
	}
}

func TestRemoteCoreDescribe(t *testing.T) {
	core := NewRemoteCore(fakeWorkerCoreClient{
		describe: func(context.Context, *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error) {
			return CoreDescriptionProto(CoreDescription{
				Capabilities:       []CoreCapability{{Name: "external-runner", Version: "v1", Metadata: map[string]string{"provider": "jenkins"}}},
				SupportedIsolation: []string{action.IsolationHost},
				Metadata:           map[string]string{"name": "test-core"},
			}), nil
		},
	})

	desc, err := core.Describe(context.Background())
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}

	if desc.ProtocolVersion != ProtocolVersion {
		t.Fatalf("protocol version = %q, want %q", desc.ProtocolVersion, ProtocolVersion)
	}

	if len(desc.Capabilities) != 1 || desc.Capabilities[0].Metadata["provider"] != "jenkins" {
		t.Fatalf("capabilities = %#v", desc.Capabilities)
	}
}

func TestValidateCoreDescription(t *testing.T) {
	valid := CoreDescription{
		ProtocolVersion: ProtocolVersion,
		Capabilities: []CoreCapability{
			{Name: workersdk.CapabilityExecute, Version: "v1"},
			{Name: workersdk.CapabilityCancelTask, Version: "v1"},
			{Name: workersdk.CapabilityShellLogCallback, Version: "v1"},
			{Name: workersdk.CapabilityShellArtifactPush, Version: "v1"},
		},
	}

	if err := ValidateCoreDescription(valid, RequiredWorkerCoreCapabilities()); err != nil {
		t.Fatalf("ValidateCoreDescription valid: %v", err)
	}

	badProtocol := valid
	badProtocol.ProtocolVersion = "workercore.v0"
	if err := ValidateCoreDescription(badProtocol, RequiredWorkerCoreCapabilities()); err == nil || !strings.Contains(err.Error(), "protocol version") {
		t.Fatalf("ValidateCoreDescription protocol error = %v, want protocol version error", err)
	}

	missingCapability := valid
	missingCapability.Capabilities = missingCapability.Capabilities[:2]
	err := ValidateCoreDescription(missingCapability, RequiredWorkerCoreCapabilities())
	if err == nil || !strings.Contains(err.Error(), workersdk.CapabilityShellLogCallback) || !strings.Contains(err.Error(), workersdk.CapabilityShellArtifactPush) {
		t.Fatalf("ValidateCoreDescription missing capability error = %v", err)
	}
}

func TestRemoteCoreExecuteTaskWrapsClientError(t *testing.T) {
	core := NewRemoteCore(fakeWorkerCoreClient{
		execute: func(context.Context, *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
			return nil, errors.New("dial unix socket")
		},
	})

	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job:     &api.Job{},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			SessionID: "session-1",
			Logger:    mocks.NewMockLogger(),
		}),
	})

	if err == nil || !strings.Contains(err.Error(), "dial unix socket") {
		t.Fatalf("ExecuteTask error = %v, want wrapped client error", err)
	}
}

func TestRemoteCoreCancelTask(t *testing.T) {
	var captured *api.CancelWorkerCoreTaskRequest
	core := NewRemoteCore(fakeWorkerCoreClient{
		cancel: func(_ context.Context, req *api.CancelWorkerCoreTaskRequest) (*api.Empty, error) {
			captured = req
			return &api.Empty{}, nil
		},
	})

	err := core.CancelTask(context.Background(), CancelTaskRequest{
		SessionID: "execution-1",
		RunID:     "run-1",
		TaskKey:   "root",
		Reason:    "remote request",
	})

	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	if captured.GetSessionId() != "execution-1" || captured.GetRunId() != "run-1" || captured.GetReason() != "remote request" {
		t.Fatalf("cancel request = %#v", captured)
	}
}

func TestArtifactProtocolConversions(t *testing.T) {
	metadataJSON := `{"source":"test"}`
	metadata := ArtifactMetadataProto("session-1", action.ArtifactPublishRequest{
		Name:         "build-log",
		Path:         "logs/build.txt",
		ContentType:  "text/plain",
		MetadataJSON: &metadataJSON,
		ExpectedSize: 42,
		RequireSize:  true,
		MaxBytes:     1024,
	})

	if metadata.GetSessionId() != "session-1" || metadata.GetMetadataJson() != metadataJSON {
		t.Fatalf("artifact metadata = %#v", metadata)
	}

	result := action.ArtifactPublishResult{
		Name:            "build-log",
		Path:            "logs/build.txt",
		ContentType:     "text/plain",
		BlobKey:         "blob-1",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "abc",
		SizeBytes:       42,
		ArtifactShardID: "artifact-a",
	}

	roundTrip := ArtifactResultFromProto(ArtifactResultProto(result))
	if roundTrip != result {
		t.Fatalf("artifact result round trip = %#v, want %#v", roundTrip, result)
	}
}

type fakeWorkerCoreClient struct {
	describe func(context.Context, *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error)
	execute  func(context.Context, *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error)
	cancel   func(context.Context, *api.CancelWorkerCoreTaskRequest) (*api.Empty, error)
}

func (f fakeWorkerCoreClient) DescribeCore(ctx context.Context, req *api.DescribeWorkerCoreRequest, _ ...grpc.CallOption) (*api.DescribeWorkerCoreResponse, error) {
	if f.describe != nil {
		return f.describe(ctx, req)
	}

	return &api.DescribeWorkerCoreResponse{}, nil
}

func (f fakeWorkerCoreClient) ExecuteTask(ctx context.Context, req *api.ExecuteWorkerCoreTaskRequest, _ ...grpc.CallOption) (*api.ExecuteWorkerCoreTaskResponse, error) {
	if f.execute != nil {
		return f.execute(ctx, req)
	}

	return &api.ExecuteWorkerCoreTaskResponse{}, nil
}

func (f fakeWorkerCoreClient) CancelTask(ctx context.Context, req *api.CancelWorkerCoreTaskRequest, _ ...grpc.CallOption) (*api.Empty, error) {
	if f.cancel != nil {
		return f.cancel(ctx, req)
	}

	return &api.Empty{}, nil
}

type fakeArtifactPublisher struct {
	result action.ArtifactPublishResult
	err    error
}

func (p fakeArtifactPublisher) PublishArtifact(context.Context, action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	return p.result, p.err
}
