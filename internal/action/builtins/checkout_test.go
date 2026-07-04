package builtins

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/gitcmd"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
)

type fakeCheckoutCache struct {
	handled   bool
	err       error
	calls     int
	remoteURL string
	workspace string
}

func (c *fakeCheckoutCache) Checkout(_ context.Context, remoteURL, workspace string, _ interfaces.Logger) (bool, error) {
	c.calls++
	c.remoteURL = remoteURL
	c.workspace = workspace
	return c.handled, c.err
}

type fakeCheckoutCacheRefFetcher struct {
	fakeCheckoutCache
	fetchHandled bool
	fetchErr     error
	fetchCalls   int
	fetchRemote  string
	fetchRefs    []string
}

func (c *fakeCheckoutCacheRefFetcher) FetchRefspecs(_ context.Context, remoteURL, _ string, refspecs []string, _ interfaces.Logger) (bool, error) {
	c.fetchCalls++
	c.fetchRemote = remoteURL
	c.fetchRefs = append([]string(nil), refspecs...)
	return c.fetchHandled, c.fetchErr
}

func TestCheckoutAction_Type(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	if checkoutAction.Type() != "builtins/checkout" {
		t.Errorf("expected 'builtins/checkout', got '%s'", checkoutAction.Type())
	}
}

func TestCheckoutAction_Execute_UsesStateProcessExecutor(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(nil)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)
	state.Workspace = "/tmp/vectis-state-checkout"
	state.ProcessExecutor = mockExecutor

	url := "https://github.com/example/repo.git"
	inputs := map[string]any{
		"url": url,
	}

	result := checkoutAction.Execute(context.Background(), state, inputs, nil)
	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	workDirs := mockExecutor.GetWorkDirs()
	if len(paths) != 1 || paths[0] != "git" {
		t.Fatalf("expected one git execution, got paths=%v", paths)
	}

	wantArgs := gitcmd.NoAutoMaintenanceCloneArgs(url, ".")
	if len(args) != 1 || !reflect.DeepEqual(args[0], wantArgs) {
		t.Fatalf("expected checkout args %+v, got %v", wantArgs, args)
	}

	if len(workDirs) != 1 || workDirs[0] != "/tmp/vectis-state-checkout" {
		t.Fatalf("expected workspace from state, got workDirs=%v", workDirs)
	}
}

func TestCheckoutAction_Execute_UsesCheckoutCache(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	cache := &fakeCheckoutCache{handled: true}
	checkoutAction := NewCheckoutAction(mockExecutor, cache)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)
	state.Workspace = "/tmp/vectis-cache-checkout"

	url := "https://github.com/example/repo.git"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{"url": url}, nil)
	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	if cache.calls != 1 || cache.remoteURL != url || cache.workspace != state.Workspace {
		t.Fatalf("cache call mismatch: %+v", cache)
	}

	if len(mockExecutor.GetPaths()) != 0 {
		t.Fatalf("expected checkout cache to bypass process executor, got %v", mockExecutor.GetPaths())
	}
}

func TestCheckoutAction_Execute_UsesCheckoutCacheForFetchRefspecs(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	cache := &fakeCheckoutCache{handled: true}
	checkoutAction := NewCheckoutAction(mockExecutor, cache)
	state := createTestState(nil)
	state.Workspace = "/tmp/vectis-cache-refspec-checkout"

	refspecs := "+refs/notes/*:refs/notes/*\n+refs/changes/*:refs/changes/*"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{
		"url":            "https://github.com/example/repo.git",
		"fetch_refspecs": refspecs,
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	if cache.calls != 1 {
		t.Fatalf("cache calls = %d, want 1", cache.calls)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	if len(paths) != 1 || paths[0] != "git" {
		t.Fatalf("expected one git fetch execution, got paths=%v", paths)
	}

	wantArgs := gitcmd.NoAutoMaintenanceArgs("fetch", "--no-auto-gc", "--no-tags", "--", "vectis-cache", "+refs/notes/*:refs/notes/*", "+refs/changes/*:refs/changes/*")
	if len(args) != 1 || !reflect.DeepEqual(args[0], wantArgs) {
		t.Fatalf("fetch args = %+v, want %+v", args, wantArgs)
	}
}

func TestCheckoutAction_Execute_UsesCheckoutCacheRefFetcher(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	cache := &fakeCheckoutCacheRefFetcher{
		fakeCheckoutCache: fakeCheckoutCache{handled: true},
		fetchHandled:      true,
	}

	checkoutAction := NewCheckoutAction(mockExecutor, cache)
	state := createTestState(nil)
	url := "https://github.com/example/repo.git"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{
		"url":            url,
		"fetch_refspecs": "+refs/notes/*:refs/notes/*",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	if cache.fetchCalls != 1 || cache.fetchRemote != url || !reflect.DeepEqual(cache.fetchRefs, []string{"+refs/notes/*:refs/notes/*"}) {
		t.Fatalf("fetcher calls=%d remote=%q refs=%+v", cache.fetchCalls, cache.fetchRemote, cache.fetchRefs)
	}

	if len(mockExecutor.GetPaths()) != 0 {
		t.Fatalf("expected cache ref fetcher to bypass process executor, got %v", mockExecutor.GetPaths())
	}
}

func TestCheckoutAction_Execute_PrefersStateCheckoutCache(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	actionCache := &fakeCheckoutCache{handled: true}
	stateCache := &fakeCheckoutCache{handled: true}
	checkoutAction := NewCheckoutAction(mockExecutor, actionCache)
	state := createTestState(nil)
	state.Workspace = "/tmp/vectis-state-cache-checkout"
	state.CheckoutCache = stateCache

	url := "https://github.com/example/repo.git"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{"url": url}, nil)
	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	if stateCache.calls != 1 || stateCache.remoteURL != url {
		t.Fatalf("state cache call mismatch: %+v", stateCache)
	}

	if actionCache.calls != 0 {
		t.Fatalf("expected state cache to override action cache, action cache calls=%d", actionCache.calls)
	}

	if len(mockExecutor.GetPaths()) != 0 {
		t.Fatalf("expected checkout cache to bypass process executor, got %v", mockExecutor.GetPaths())
	}
}

func TestCheckoutAction_Execute_DirectCloneFetchesRefspecsFromOrigin(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	state := createTestState(nil)
	state.Workspace = "/tmp/vectis-direct-refspec-checkout"

	url := "https://github.com/example/repo.git"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{
		"url":            url,
		"fetch_refspecs": "+refs/pull/*/head:refs/remotes/origin/pr/*",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	args := mockExecutor.GetArgs()
	wantArgs := [][]string{
		gitcmd.NoAutoMaintenanceCloneArgs(url, "."),
		gitcmd.NoAutoMaintenanceArgs("fetch", "--no-auto-gc", "--no-tags", "--", "origin", "+refs/pull/*/head:refs/remotes/origin/pr/*"),
	}

	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("git args = %+v, want %+v", args, wantArgs)
	}
}

func TestCheckoutAction_Execute_DirectCloneChecksOutRef(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	state := createTestState(nil)
	state.Workspace = "/tmp/vectis-direct-ref-checkout"

	url := "http://gerrit.example.com/project"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": url,
		"ref": "refs/changes/01/1/1",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	wantArgs := [][]string{
		gitcmd.NoAutoMaintenanceCloneArgs(url, "."),
		gitcmd.NoAutoMaintenanceArgs("fetch", "--no-auto-gc", "--no-tags", "--", "origin", "refs/changes/01/1/1"),
		gitcmd.NoAutoMaintenanceArgs("checkout", "--detach", "FETCH_HEAD"),
	}

	if got := mockExecutor.GetArgs(); !reflect.DeepEqual(got, wantArgs) {
		t.Fatalf("git args = %+v, want %+v", got, wantArgs)
	}

	workDirs := mockExecutor.GetWorkDirs()
	if len(workDirs) != 3 {
		t.Fatalf("workDirs = %v, want 3 calls", workDirs)
	}
	for i, got := range workDirs {
		if got != state.Workspace {
			t.Fatalf("workDir[%d] = %q, want workspace %q", i, got, state.Workspace)
		}
	}
}

func TestCheckoutAction_Execute_RejectsOptionLikeRef(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	checkoutAction := NewCheckoutAction(mockExecutor)
	state := createTestState(nil)

	result := checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": "https://gerrit.example.com/project",
		"ref": "--upload-pack=bad",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "must not begin") {
		t.Fatalf("expected option-like ref error, got %v", result.Error)
	}

	if len(mockExecutor.GetArgs()) != 0 {
		t.Fatalf("expected checkout to reject ref before git invocation, got %v", mockExecutor.GetArgs())
	}
}

func TestCheckoutAction_Execute_ReportsCheckoutCacheFailure(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	cache := &fakeCheckoutCache{handled: true, err: fmt.Errorf("cache unavailable")}
	checkoutAction := NewCheckoutAction(mockExecutor, cache)
	state := createTestState(nil)

	result := checkoutAction.Execute(context.Background(), state, map[string]any{"url": "https://github.com/example/repo.git"}, nil)
	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "cached checkout failed") {
		t.Fatalf("expected cached checkout error, got %v", result.Error)
	}

	if len(mockExecutor.GetPaths()) != 0 {
		t.Fatalf("expected checkout cache failure to avoid direct clone fallback, got %v", mockExecutor.GetPaths())
	}
}

func TestCheckoutAction_ValidateRejectsFetchRefspecOption(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	errs := checkoutAction.ValidateWith(map[string]string{
		"url":            "https://github.com/example/repo.git",
		"fetch_refspecs": "--upload-pack=/tmp/helper",
	})

	for _, err := range errs {
		if err.Field == "fetch_refspecs" {
			return
		}
	}

	t.Fatalf("expected fetch_refspecs validation error, got %+v", errs)
}

func TestCheckoutAction_Execute_MissingUrl(t *testing.T) {
	checkoutAction := NewCheckoutAction(nil)
	state := createTestState(nil)

	result := checkoutAction.Execute(context.Background(), state, map[string]any{}, nil)
	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error for missing url")
	}

	if !strings.Contains(result.Error.Error(), "requires 'url' input") {
		t.Errorf("expected 'requires url input' error, got: %v", result.Error)
	}

	result = checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": "",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure for empty url, got %v", result.Status)
	}

	result = checkoutAction.Execute(context.Background(), state, map[string]any{
		"url": 123,
	}, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure for non-string url, got %v", result.Status)
	}
}

func TestCheckoutAction_Execute_Success(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("Cloning into '.'...\n")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	url := "https://github.com/example/repo.git"
	inputs := map[string]any{
		"url": url,
	}

	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	paths := mockExecutor.GetPaths()
	args := mockExecutor.GetArgs()
	if len(paths) != 1 || len(args) != 1 {
		t.Errorf("expected 1 Start call, got paths=%d args=%d", len(paths), len(args))
	}

	if paths[0] != "git" {
		t.Errorf("expected path 'git', got '%s'", paths[0])
	}

	wantArgs := gitcmd.NoAutoMaintenanceCloneArgs(url, ".")
	if !reflect.DeepEqual(args[0], wantArgs) {
		t.Errorf("expected args %+v, got %v", wantArgs, args[0])
	}

	envs := mockExecutor.GetEnvs()
	if len(envs) != 1 {
		t.Fatalf("expected 1 env, got %d", len(envs))
	}

	if got, ok := testEnvLookup(envs[0], "GIT_TERMINAL_PROMPT"); !ok || got != "0" {
		t.Fatalf("GIT_TERMINAL_PROMPT env = %q, %v; want 0", got, ok)
	}

	if _, ok := testEnvLookup(envs[0], "VECTIS_DATABASE_DSN"); ok {
		t.Fatalf("checkout action leaked worker database DSN env: %v", envs[0])
	}

	if !mockProcess.WaitCalled() {
		t.Error("expected Wait to be called")
	}

	chunks := mockStream.GetChunks()
	foundSuccess := false
	for _, chunk := range chunks {
		if strings.Contains(string(chunk.GetData()), "Checkout completed successfully") {
			foundSuccess = true
			break
		}
	}
	if !foundSuccess {
		t.Error("expected success message to be logged")
	}
}

func TestCheckoutAction_Execute_RejectsCredentialedURL(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	secretURL := "https://user:token@github.com/example/repo.git"
	result := checkoutAction.Execute(context.Background(), state, map[string]any{"url": secretURL}, nil)
	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	args := mockExecutor.GetArgs()
	if len(args) != 0 {
		t.Fatalf("expected credentialed URL to be rejected before git clone, got args %v", args)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "without embedded credentials") {
		t.Fatalf("expected credential error, got %v", result.Error)
	}
}

func TestCheckoutAction_Execute_CloneFailure(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("fatal: repository not found\n")
	mockProcess.SetWaitError(errors.New("exit status 128"))
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := createTestState(mockStream)

	inputs := map[string]any{
		"url": "https://github.com/nonexistent/repo.git",
	}
	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error")
	}

	if !strings.Contains(result.Error.Error(), "git clone failed") {
		t.Errorf("expected 'git clone failed' in error, got: %v", result.Error)
	}

	chunks := mockStream.GetChunks()
	foundStderr := false
	for _, chunk := range chunks {
		if chunk.GetStream() == api.Stream_STREAM_STDERR {
			foundStderr = true
			break
		}
	}
	if !foundStderr {
		t.Error("expected error to be logged to stderr")
	}
}

func TestCheckoutAction_Execute_StartError(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockExecutor.SetError(errors.New("exec: \"git\": executable file not found"))

	checkoutAction := NewCheckoutAction(mockExecutor)
	state := createTestState(nil)

	inputs := map[string]any{
		"url": "https://github.com/example/repo.git",
	}
	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusFailure {
		t.Errorf("expected failure, got %v", result.Status)
	}

	if result.Error == nil {
		t.Error("expected error")
	}

	if !strings.Contains(result.Error.Error(), "failed to start git clone") {
		t.Errorf("expected 'failed to start git clone' in error, got: %v", result.Error)
	}
}

func TestCheckoutAction_Execute_WorkspacePassed(t *testing.T) {
	mockExecutor := mocks.NewMockExecExecutor()
	mockProcess := mocks.NewMockProcess()
	mockProcess.SetStdout("")
	mockProcess.SetStderr("")
	mockProcess.SetWaitError(nil)
	mockExecutor.SetProcess(mockProcess)

	checkoutAction := NewCheckoutAction(mockExecutor)
	mockStream := &mockLogStream{}
	state := &action.ExecutionState{
		JobID:     "test-job",
		Workspace: "/tmp/vectis-checkout-job",
		Logger:    interfaces.NewLogger("test"),
		LogStream: mockStream,
	}

	inputs := map[string]any{
		"url": "https://github.com/example/repo.git",
	}

	result := checkoutAction.Execute(context.Background(), state, inputs, nil)

	if result.Status != action.StatusSuccess {
		t.Errorf("expected success, got %v with error: %v", result.Status, result.Error)
	}

	workDirs := mockExecutor.GetWorkDirs()
	if len(workDirs) != 1 {
		t.Errorf("expected 1 workDir, got %d", len(workDirs))
	}

	if workDirs[0] != "/tmp/vectis-checkout-job" {
		t.Errorf("expected workspace '/tmp/vectis-checkout-job', got '%s'", workDirs[0])
	}
}
