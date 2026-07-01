package workercore

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/source"
)

func TestExecutorCoreExecutesTaskThroughJobExecutor(t *testing.T) {
	processExecutor := mocks.NewMockExecExecutor()
	process := mocks.NewMockProcess()
	process.SetStdout("hello from core\n")
	processExecutor.SetProcess(process)

	executor := job.NewExecutor(job.WithProcessExecutor(processExecutor))
	logDone := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = logDone
	defer func() { executor.TestLogStreamHook = nil }()

	jobID := "job-worker-core"
	runID := "run-worker-core"
	nodeID := "root"
	uses := "builtins/script"
	core := NewExecutorCore(executor)
	err := core.ExecuteTask(context.Background(), ExecuteTaskRequest{
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Id:   &nodeID,
				Uses: &uses,
				With: map[string]string{"script": "echo hello"},
			},
		},
		TaskKey: dal.RootTaskKey,
		Session: NewTaskSession(TaskSessionOptions{
			LogClient: mocks.NewMockLogClient(),
			Logger:    mocks.NewMockLogger(),
		}),
	})

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	select {
	case waiter := <-logDone:
		if err := waiter.WaitForDone(2 * time.Second); err != nil {
			t.Fatalf("wait for log flush: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for log stream hook")
	}

	paths := processExecutor.GetPaths()
	if len(paths) != 1 || paths[0] != "sh" {
		t.Fatalf("process paths = %v, want [sh]", paths)
	}

	args := processExecutor.GetArgs()
	if len(args) != 1 || len(args[0]) != 1 || filepath.Ext(args[0][0]) != ".sh" {
		t.Fatalf("process args = %v, want one generated .sh script arg", args)
	}

	if !process.WaitCalled() {
		t.Fatal("expected process Wait to be called")
	}
}

func TestExecutorCoreValidatesShellBoundaryInputs(t *testing.T) {
	tests := []struct {
		name string
		req  ExecuteTaskRequest
		want string
	}{
		{
			name: "missing job",
			req: ExecuteTaskRequest{
				TaskKey: dal.RootTaskKey,
				Session: NewTaskSession(TaskSessionOptions{
					LogClient: mocks.NewMockLogClient(),
					Logger:    mocks.NewMockLogger(),
				}),
			},
			want: "requires a job",
		},
		{
			name: "missing task key",
			req: ExecuteTaskRequest{
				Job: &api.Job{},
				Session: NewTaskSession(TaskSessionOptions{
					LogClient: mocks.NewMockLogClient(),
					Logger:    mocks.NewMockLogger(),
				}),
			},
			want: "requires a task key",
		},
		{
			name: "missing task session",
			req: ExecuteTaskRequest{
				Job:     &api.Job{},
				TaskKey: dal.RootTaskKey,
			},
			want: "requires a task session",
		},
		{
			name: "missing log client",
			req: ExecuteTaskRequest{
				Job:     &api.Job{},
				TaskKey: dal.RootTaskKey,
				Session: NewTaskSession(TaskSessionOptions{
					Logger: mocks.NewMockLogger(),
				}),
			},
			want: "requires a log client",
		},
		{
			name: "missing logger",
			req: ExecuteTaskRequest{
				Job:     &api.Job{},
				TaskKey: dal.RootTaskKey,
				Session: NewTaskSession(TaskSessionOptions{
					LogClient: mocks.NewMockLogClient(),
				}),
			},
			want: "requires a logger",
		},
	}

	core := NewExecutorCore(job.NewExecutor())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := core.ExecuteTask(context.Background(), tt.req)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ExecuteTask error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestExecutorCoreBuildsSessionCheckoutCache(t *testing.T) {
	core := NewExecutorCore(nil, WithExecutorCheckoutCacheRoot(t.TempDir()))
	cache, err := core.checkoutCacheForSession(NewTaskSession(TaskSessionOptions{
		CheckoutCacheRemoteURLs: []string{"https://mirror.invalid/vectis.git"},
	}))

	if err != nil {
		t.Fatalf("checkoutCacheForSession: %v", err)
	}

	if cache == nil {
		t.Fatal("checkoutCacheForSession returned nil cache")
	}

	withoutURLs, err := core.checkoutCacheForSession(NewTaskSession(TaskSessionOptions{}))
	if err != nil {
		t.Fatalf("checkoutCacheForSession without URLs: %v", err)
	}

	if withoutURLs != nil {
		t.Fatal("checkoutCacheForSession without URLs returned a cache")
	}
}

func TestExecutorCoreWarmCheckoutCacheReportsFailures(t *testing.T) {
	core := NewExecutorCore(nil, WithExecutorCheckoutCacheRoot(t.TempDir()))

	result, err := core.WarmCheckoutCache(context.Background(), WarmCheckoutCacheRequest{
		RemoteURLs: []string{"\n", "://bad", "://bad"},
	})

	if err != nil {
		t.Fatalf("WarmCheckoutCache: %v", err)
	}

	if result.Warmed != 0 {
		t.Fatalf("warmed = %d, want 0", result.Warmed)
	}

	if len(result.Failures) != 1 || result.Failures[0].Message == "" {
		t.Fatalf("failures = %+v, want one remote failure", result.Failures)
	}
}

func TestTaskSessionClonesActionLocks(t *testing.T) {
	locks := []actionregistry.ActionLock{
		{
			NodePath: "root",
			Uses:     "actions/example",
			Descriptor: actionregistry.Descriptor{
				CanonicalName: "actions/example",
				Version:       "v1",
				Digest:        "sha256:one",
				RuntimeConfig: map[string]string{"entrypoint": "first"},
			},
		},
	}

	session := NewTaskSession(TaskSessionOptions{ActionLocks: locks})
	locks[0].Descriptor.RuntimeConfig["entrypoint"] = "mutated"

	got := session.ActionLocks()
	got[0].Descriptor.RuntimeConfig["entrypoint"] = "changed again"

	again := session.ActionLocks()
	if again[0].Descriptor.RuntimeConfig["entrypoint"] != "first" {
		t.Fatalf("session action locks were not cloned defensively: %#v", again[0].Descriptor.RuntimeConfig)
	}
}

func TestTaskSessionClonesCheckoutCacheRemoteURLs(t *testing.T) {
	remoteURLs := []string{"https://mirror.invalid/one.git"}
	session := NewTaskSession(TaskSessionOptions{CheckoutCacheRemoteURLs: remoteURLs})
	remoteURLs[0] = "https://mirror.invalid/mutated.git"

	got := session.CheckoutCacheRemoteURLs()
	got[0] = "https://mirror.invalid/changed-again.git"

	again := session.CheckoutCacheRemoteURLs()
	if len(again) != 1 || again[0] != "https://mirror.invalid/one.git" {
		t.Fatalf("session checkout cache remote URLs were not cloned defensively: %+v", again)
	}
}

func TestTaskSessionClonesCheckoutCacheRemotes(t *testing.T) {
	remotes := []CheckoutCacheRemote{
		{
			RemoteURL:          "https://mirror.invalid/one.git",
			FallbackRemoteURLs: []string{"https://tier1.invalid/one.git"},
			Credentials:        source.GitCredentials{Username: "alice", Password: "secret"},
		},
	}

	session := NewTaskSession(TaskSessionOptions{CheckoutCacheRemotes: remotes})
	remotes[0].RemoteURL = "https://mirror.invalid/mutated.git"
	remotes[0].FallbackRemoteURLs[0] = "https://tier1.invalid/mutated.git"

	got := session.CheckoutCacheRemotes()
	got[0].RemoteURL = "https://mirror.invalid/changed-again.git"
	got[0].FallbackRemoteURLs[0] = "https://tier1.invalid/changed-again.git"
	got[0].Credentials.Username = "mallory"

	again := session.CheckoutCacheRemotes()
	if len(again) != 1 ||
		again[0].RemoteURL != "https://mirror.invalid/one.git" ||
		len(again[0].FallbackRemoteURLs) != 1 ||
		again[0].FallbackRemoteURLs[0] != "https://tier1.invalid/one.git" ||
		again[0].Credentials.Username != "alice" ||
		again[0].Credentials.Password != "secret" {
		t.Fatalf("session checkout cache remotes were not cloned defensively: %+v", again)
	}
}
