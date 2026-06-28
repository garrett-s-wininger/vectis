package workercore

import (
	"context"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/secrets"
	"vectis/internal/testutil/socktest"
	"vectis/internal/workloadidentity"

	"google.golang.org/grpc"
)

func BenchmarkWorkerCore_ExecuteTaskRequestProto(b *testing.B) {
	for _, tc := range []struct {
		name    string
		session TaskSession
	}{
		{name: "minimal", session: benchmarkWorkerCoreSession(false)},
		{name: "rich", session: benchmarkWorkerCoreSession(true)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			req := ExecuteTaskRequest{
				Job:     benchmarkWorkerCoreJob(),
				TaskKey: dal.RootTaskKey,
				Session: tc.session,
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				protoReq, err := ExecuteTaskRequestProto(req)
				if err != nil {
					b.Fatalf("ExecuteTaskRequestProto: %v", err)
				}
				if protoReq.GetTaskKey() != dal.RootTaskKey {
					b.Fatalf("task key = %q", protoReq.GetTaskKey())
				}
			}
		})
	}
}

func BenchmarkWorkerCore_ServiceExecuteTask_InProcess(b *testing.B) {
	ctx := context.Background()
	service := NewService(benchmarkNoopCore{}, ServiceOptions{})
	protoReq, err := ExecuteTaskRequestProto(ExecuteTaskRequest{
		Job:     benchmarkWorkerCoreJob(),
		TaskKey: dal.RootTaskKey,
		Session: benchmarkWorkerCoreSession(false),
	})
	if err != nil {
		b.Fatalf("ExecuteTaskRequestProto: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := service.ExecuteTask(ctx, protoReq)
		if err != nil {
			b.Fatalf("ExecuteTask: %v", err)
		}
		if resp.GetOutcome() != api.RunOutcome_RUN_OUTCOME_SUCCESS {
			b.Fatalf("outcome = %s", resp.GetOutcome())
		}
	}
}

func BenchmarkWorkerCore_RemoteExecuteTask_UnixSocket(b *testing.B) {
	socketPath := socktest.ShortPath(b, "worker-core-bench.sock")
	server, listener, err := NewUnixCoreServer(socketPath, NewService(benchmarkNoopCore{}, ServiceOptions{}))
	if err != nil {
		b.Fatalf("NewUnixCoreServer: %v", err)
	}
	defer server.Stop()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			b.Errorf("worker core server: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, cleanup, err := DialUnixCore(ctx, socketPath)
	if err != nil {
		b.Fatalf("DialUnixCore: %v", err)
	}
	defer cleanup()

	req := ExecuteTaskRequest{
		Job:     benchmarkWorkerCoreJob(),
		TaskKey: dal.RootTaskKey,
		Session: benchmarkWorkerCoreSession(false),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := remote.ExecuteTask(ctx, req); err != nil {
			b.Fatalf("ExecuteTask: %v", err)
		}
	}
}

type benchmarkNoopCore struct{}

func (benchmarkNoopCore) ExecuteTask(context.Context, ExecuteTaskRequest) error {
	return nil
}

func benchmarkWorkerCoreJob() *api.Job {
	jobID := "bench-worker-core"
	runID := "bench-worker-core-run"
	nodeID := dal.RootTaskKey
	uses := "builtins/result"

	return &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &nodeID,
			Uses: &uses,
			With: map[string]string{"success": "true"},
		},
	}
}

func benchmarkWorkerCoreSession(rich bool) TaskSession {
	opts := TaskSessionOptions{
		SessionID: "bench-session",
	}

	if rich {
		opts.WorkloadIdentity = &workloadidentity.Identity{
			SPIFFEID:      "spiffe://vectis.local/cell/local/job/bench-worker-core/run/bench-worker-core-run/execution/bench-session",
			TrustDomain:   "vectis.local",
			NamespacePath: "/bench",
			CellID:        "local",
			JobID:         "bench-worker-core",
			RunID:         "bench-worker-core-run",
			ExecutionID:   "bench-session",
		}
		opts.ActionLocks = []actionregistry.ActionLock{{
			NodeID:   "node-root",
			NodePath: dal.RootTaskKey,
			Uses:     "actions/example",
			Descriptor: actionregistry.Descriptor{
				CanonicalName: "actions/example",
				DisplayName:   "Example",
				Version:       "v1",
				Digest:        "sha256:bench",
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
				Capabilities: []actionregistry.Capability{actionregistry.CapabilityNetwork},
				Status:       actionregistry.DescriptorStatusActive,
			},
		}}
		opts.SecretFiles = []secrets.FileMaterial{{
			ID:   "secret-1",
			Path: "secret.txt",
			Data: []byte("secret-value"),
			Mode: secrets.DefaultFileMode,
		}}
	}

	return NewTaskSession(opts)
}

var _ Core = benchmarkNoopCore{}
