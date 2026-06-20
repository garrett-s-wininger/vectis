package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"
	"vectis/sdk/workercore/conformance"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestKubernetesCoreConformance(t *testing.T) {
	conformance.RunCoreSuite(t, func(t *testing.T) sdk.Core {
		t.Helper()
		return newTestCore(&fakeRunner{
			streamLines: []string{"conformance from pod\n"},
			outcome:     jobOutcome{Phase: jobSucceeded},
		})
	}, conformance.Options{
		RequireLogCallback: true,
		Timeout:            5 * time.Second,
	})
}

func TestKubernetesCoreServerConformance(t *testing.T) {
	conformance.RunCoreServerSuite(t, func(t *testing.T) string {
		t.Helper()

		socketPath := shortSocketPath(t, "worker-core-kubernetes.sock")
		server, listener, err := sdk.NewUnixCoreServer(socketPath, newTestCore(&fakeRunner{
			streamLines: []string{"conformance from served pod\n"},
			outcome:     jobOutcome{Phase: jobSucceeded},
		}), sdk.ServiceOptions{})

		if err != nil {
			t.Fatalf("NewUnixCoreServer: %v", err)
		}

		t.Cleanup(server.Stop)

		go func() {
			if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
				t.Errorf("Kubernetes worker core server: %v", err)
			}
		}()

		return socketPath
	}, conformance.Options{
		RequireLogCallback: true,
		Timeout:            5 * time.Second,
	})
}

func TestExecuteTaskAppliesKubernetesJob(t *testing.T) {
	runner := &fakeRunner{outcome: jobOutcome{Phase: jobSucceeded}}
	core := newTestCore(runner)
	result, err := core.ExecuteTask(context.Background(), testTask(t, "builtins/shell", "printf hi"))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Fatalf("outcome = %s message=%q", result.Outcome, result.Message)
	}

	applied := runner.appliedManifest(t)
	for _, want := range []string{
		"kind: Job",
		"namespace: test-vectis",
		"image: alpine:3.20",
		"- sh",
		"- -c",
		"- printf hi",
		"name: VECTIS_RUN_ID",
		"value: run-kubernetes-example",
		"automountServiceAccountToken: false",
	} {
		if !strings.Contains(applied, want) {
			t.Fatalf("manifest missing %q:\n%s", want, applied)
		}
	}
}

func TestExecuteTaskAppliesCustomProcessAction(t *testing.T) {
	runner := &fakeRunner{outcome: jobOutcome{Phase: jobSucceeded}}
	core := newTestCore(runner)

	result, err := core.ExecuteTask(context.Background(), customProcessTask(t, customProcessDescriptor("process"), map[string]string{
		"target":  "prod",
		"dry-run": "false",
	}))

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Fatalf("outcome = %s message=%q", result.Outcome, result.Message)
	}

	applied := runner.appliedManifest(t)
	for _, want := range []string{
		"- ./deploy.sh",
		"name: VECTIS_ACTION_NAME",
		"value: acme/deploy",
		"name: VECTIS_ACTION_VERSION",
		"value: v1",
		"name: VECTIS_ACTION_DIGEST",
		"value: sha256:deploy",
		"name: VECTIS_INPUT_DRY_RUN",
		"value: \"false\"",
		"name: VECTIS_INPUT_TARGET",
		"value: prod",
	} {
		if !strings.Contains(applied, want) {
			t.Fatalf("manifest missing %q:\n%s", want, applied)
		}
	}
}

func TestExecuteTaskMapsFailedJobToProviderFailure(t *testing.T) {
	core := newTestCore(&fakeRunner{outcome: jobOutcome{
		Phase:    jobFailed,
		Reason:   "BackoffLimitExceeded",
		Message:  "pod failed",
		ExitCode: 17,
	}})

	result, err := core.ExecuteTask(context.Background(), testTask(t, "builtins/shell", "false"))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", result.Outcome)
	}

	if result.ReasonCode != reasonJobFailed {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonJobFailed)
	}

	for _, want := range []string{"pod failed", "BackoffLimitExceeded", "exit_code=17"} {
		if !strings.Contains(result.Message, want) {
			t.Fatalf("failure message %q missing %q", result.Message, want)
		}
	}
}

func TestExecuteTaskMapsUnknownJobToProviderUnknown(t *testing.T) {
	core := newTestCore(&fakeRunner{outcome: jobOutcome{
		Phase:   jobUnknown,
		Reason:  "watch_lost",
		Message: "lost watch before terminal state",
	}})

	result, err := core.ExecuteTask(context.Background(), testTask(t, "builtins/shell", "sleep 1"))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_UNKNOWN {
		t.Fatalf("outcome = %s, want unknown", result.Outcome)
	}

	if result.ReasonCode != reasonJobUnknown {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonJobUnknown)
	}

	if !strings.Contains(result.Message, "lost watch") {
		t.Fatalf("unknown message = %q", result.Message)
	}
}

func TestExecuteTaskRejectsUnsupportedTaskShape(t *testing.T) {
	core := newTestCore(&fakeRunner{})

	result, err := core.ExecuteTask(context.Background(), testTask(t, "builtins/parallel", ""))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", result.Outcome)
	}

	if result.ReasonCode != reasonUnsupportedTask {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonUnsupportedTask)
	}
}

func TestExecuteTaskRejectsCustomActionWithoutFrozenDescriptor(t *testing.T) {
	core := newTestCore(&fakeRunner{})

	result, err := core.ExecuteTask(context.Background(), customProcessTask(t, nil, map[string]string{"target": "prod"}))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", result.Outcome)
	}

	if result.ReasonCode != reasonUnsupportedTask {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonUnsupportedTask)
	}

	if !strings.Contains(result.Message, "frozen action descriptor") {
		t.Fatalf("message = %q, want frozen descriptor detail", result.Message)
	}
}

func TestExecuteTaskRejectsUnsupportedCustomRuntime(t *testing.T) {
	core := newTestCore(&fakeRunner{})

	result, err := core.ExecuteTask(context.Background(), customProcessTask(t, customProcessDescriptor("container"), map[string]string{"target": "prod"}))
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", result.Outcome)
	}

	if result.ReasonCode != reasonUnsupportedTask {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonUnsupportedTask)
	}

	if !strings.Contains(result.Message, `runtime "container"`) {
		t.Fatalf("message = %q, want runtime detail", result.Message)
	}
}

func TestExecuteTaskRejectsInvalidCustomInputs(t *testing.T) {
	core := newTestCore(&fakeRunner{})

	result, err := core.ExecuteTask(context.Background(), customProcessTask(t, customProcessDescriptor("process"), map[string]string{
		"dry-run": "false",
	}))

	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}

	if result.Outcome != api.RunOutcome_RUN_OUTCOME_FAILURE {
		t.Fatalf("outcome = %s, want failure", result.Outcome)
	}

	if result.ReasonCode != reasonUnsupportedTask {
		t.Fatalf("reason = %q, want %q", result.ReasonCode, reasonUnsupportedTask)
	}

	if !strings.Contains(result.Message, `input "target" is required`) {
		t.Fatalf("message = %q, want required input detail", result.Message)
	}
}

func TestCancelTaskDeletesDerivedJob(t *testing.T) {
	runner := &fakeRunner{}
	core := newTestCore(runner)

	err := core.CancelTask(context.Background(), sdk.CancelRequest{
		SessionID: "session-cancel",
		RunID:     "run-cancel",
		TaskKey:   "root",
		Reason:    "user requested",
	})

	if err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	want := jobNameFor("run-cancel", "root", "session-cancel")
	if got := runner.deletedJob(t); got != want {
		t.Fatalf("deleted job = %q, want %q", got, want)
	}

	if reason, ok := core.cancelReason("session-cancel"); !ok || reason != "user requested" {
		t.Fatalf("cancel reason = %q ok=%v", reason, ok)
	}
}

func TestExecuteTaskObservesExplicitCancel(t *testing.T) {
	waitBlock := make(chan struct{})
	waitStarted := make(chan struct{})
	runner := &fakeRunner{
		outcome:     jobOutcome{Phase: jobUnknown, Message: "not terminal"},
		waitBlock:   waitBlock,
		waitStarted: waitStarted,
	}

	core := newTestCore(runner)
	done := make(chan sdk.Result, 1)
	go func() {
		result, err := core.ExecuteTask(context.Background(), testTaskWithSession(t, "session-observe-cancel", "builtins/shell", "sleep 300"))
		if err != nil {
			t.Errorf("ExecuteTask: %v", err)
		}

		done <- result
	}()

	<-waitStarted
	if err := core.CancelTask(context.Background(), sdk.CancelRequest{
		SessionID: "session-observe-cancel",
		RunID:     "run-kubernetes-example",
		TaskKey:   "root",
		Reason:    "stop now",
	}); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	close(waitBlock)

	select {
	case result := <-done:
		if result.Outcome != api.RunOutcome_RUN_OUTCOME_UNKNOWN || result.ReasonCode != sdk.ReasonCancelled {
			t.Fatalf("result = %+v, want cancelled unknown result", result)
		}

		if !strings.Contains(result.Message, "stop now") {
			t.Fatalf("cancel result message = %q", result.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ExecuteTask")
	}
}

func TestJobNameForIsStableDNSLabel(t *testing.T) {
	first := jobNameFor("RUN/With_Slashes", "deploy.prod", "session-1")
	second := jobNameFor("RUN/With_Slashes", "deploy.prod", "session-1")
	if first != second {
		t.Fatalf("job names differ: %q != %q", first, second)
	}

	if len(first) > 63 {
		t.Fatalf("job name length = %d, want <= 63: %q", len(first), first)
	}

	if first != strings.ToLower(first) || strings.ContainsAny(first, "_/.") {
		t.Fatalf("job name is not a DNS label: %q", first)
	}
}

func newTestCore(runner *fakeRunner) *kubernetesCore {
	if runner == nil {
		runner = &fakeRunner{outcome: jobOutcome{Phase: jobSucceeded}}
	}

	return newKubernetesCore(coreConfig{
		Namespace:    "test-vectis",
		Image:        "alpine:3.20",
		Kubectl:      "kubectl",
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
	}, runner)
}

func testTask(t *testing.T, uses, command string) sdk.Task {
	t.Helper()
	return testTaskWithSession(t, "session-kubernetes-example", uses, command)
}

func testTaskWithSession(t *testing.T, sessionID, uses, command string) sdk.Task {
	t.Helper()
	session, err := sdk.NewSession(&api.WorkerCoreTaskSession{
		SessionId: proto.String(sessionID),
	})

	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	root := &api.Node{
		Id:   proto.String("root"),
		Uses: proto.String(uses),
	}

	if command != "" {
		root.With = map[string]string{"command": command}
	}

	return sdk.Task{
		Job: &api.Job{
			Id:    proto.String("job-kubernetes-example"),
			RunId: proto.String("run-kubernetes-example"),
			Root:  root,
		},
		TaskKey: "root",
		Session: session,
	}
}

func customProcessTask(t *testing.T, descriptor *api.WorkerCoreActionDescriptor, with map[string]string) sdk.Task {
	t.Helper()

	sessionProto := &api.WorkerCoreTaskSession{
		SessionId: proto.String("session-kubernetes-example"),
	}

	if descriptor != nil {
		sessionProto.ActionLocks = []*api.WorkerCoreActionLock{{
			NodeId:      proto.String("root"),
			NodePath:    proto.String("root"),
			Uses:        proto.String("acme/deploy@v1"),
			Descriptor_: descriptor,
		}}
	}

	session, err := sdk.NewSession(sessionProto)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	root := &api.Node{
		Id:   proto.String("root"),
		Uses: proto.String("acme/deploy@v1"),
		With: cloneStringMap(with),
	}

	return sdk.Task{
		Job: &api.Job{
			Id:    proto.String("job-kubernetes-example"),
			RunId: proto.String("run-kubernetes-example"),
			Root:  root,
		},
		TaskKey: "root",
		Session: session,
	}
}

func customProcessDescriptor(runtime string) *api.WorkerCoreActionDescriptor {
	return &api.WorkerCoreActionDescriptor{
		CanonicalName: proto.String("acme/deploy"),
		DisplayName:   proto.String("Deploy"),
		Version:       proto.String("v1"),
		Digest:        proto.String("sha256:deploy"),
		Source:        proto.String("oci"),
		Runtime:       proto.String(runtime),
		RuntimeConfig: map[string]string{"command": "./deploy.sh"},
		InputSchema: &api.WorkerCoreInputSchema{
			Fields: []*api.WorkerCoreInputField{
				{Name: proto.String("target"), Type: proto.String("string"), Required: proto.Bool(true)},
				{Name: proto.String("dry-run"), Type: proto.String("string")},
			},
		},
	}
}

func shortSocketPath(t *testing.T, name string) string {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-worker-core-kubernetes-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, name)
}

type fakeRunner struct {
	mu sync.Mutex

	applied []appliedJob
	deleted []deletedJob

	streamLines []string
	outcome     jobOutcome
	waitErr     error
	waitBlock   chan struct{}
	waitStarted chan struct{}
	waitOnce    sync.Once
}

type appliedJob struct {
	namespace string
	manifest  []byte
}

type deletedJob struct {
	namespace string
	name      string
}

func (r *fakeRunner) ApplyJob(_ context.Context, namespace string, manifest []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.applied = append(r.applied, appliedJob{namespace: namespace, manifest: append([]byte(nil), manifest...)})
	return nil
}

func (r *fakeRunner) StreamLogs(ctx context.Context, _ string, _ string, send func([]byte) error) error {
	for _, line := range r.streamLines {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := send([]byte(line)); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeRunner) WaitJob(ctx context.Context, _ string, _ string, _ time.Duration, _ time.Duration) (jobOutcome, error) {
	if r.waitStarted != nil {
		r.waitOnce.Do(func() { close(r.waitStarted) })
	}

	if r.waitBlock != nil {
		select {
		case <-r.waitBlock:
		case <-ctx.Done():
			return jobOutcome{Phase: jobCancelled, Message: ctx.Err().Error()}, nil
		}
	}

	if r.outcome.Phase == "" {
		r.outcome.Phase = jobSucceeded
	}

	return r.outcome, r.waitErr
}

func (r *fakeRunner) DeleteJob(_ context.Context, namespace, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.deleted = append(r.deleted, deletedJob{namespace: namespace, name: name})
	return nil
}

func (r *fakeRunner) appliedManifest(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.applied) != 1 {
		t.Fatalf("applied jobs = %d, want 1", len(r.applied))
	}

	return string(r.applied[0].manifest)
}

func (r *fakeRunner) deletedJob(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.deleted) != 1 {
		t.Fatalf("deleted jobs = %d, want 1", len(r.deleted))
	}

	return r.deleted[0].name
}
