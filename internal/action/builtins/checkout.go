package builtins

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/gitcmd"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
)

const (
	checkoutCacheRemoteName    = "vectis-cache"
	checkoutFetchRefspecsInput = "fetch_refspecs"
)

type CheckoutAction struct {
	executor interfaces.ExecExecutor
	cache    action.CheckoutCache
}

func NewCheckoutAction(executor interfaces.ExecExecutor, caches ...action.CheckoutCache) *CheckoutAction {
	var cache action.CheckoutCache
	if len(caches) > 0 {
		cache = caches[0]
	}

	return &CheckoutAction{
		executor: executor,
		cache:    cache,
	}
}

func (c *CheckoutAction) ValidateWith(with map[string]string) []action.FieldError {
	errs := action.ValidateWithSpec(with, c.InputSchema())
	if len(errs) > 0 {
		return errs
	}

	if _, err := parseCheckoutFetchRefspecs(with[checkoutFetchRefspecsInput]); err != nil {
		errs = append(errs, action.FieldError{Field: checkoutFetchRefspecsInput, Message: err.Error()})
	}

	rawURL := with["url"]
	if hasCredentialedCloneURL(rawURL) {
		errs = append(errs, action.FieldError{Field: "url", Message: "must not include embedded credentials"})
	}

	return errs
}

func (c *CheckoutAction) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "url", Type: action.FieldURL, Required: true},
		{Name: checkoutFetchRefspecsInput, Type: action.FieldString},
	}
}

func (c *CheckoutAction) Type() string {
	return "builtins/checkout"
}

func (c *CheckoutAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	started := time.Now()

	url, ok := inputs["url"].(string)
	if !ok || url == "" {
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyValidation, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonMissingURL, time.Since(started))
		return action.NewFailureResult(fmt.Errorf("checkout action requires 'url' input"))
	}

	if hasCredentialedCloneURL(url) {
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyValidation, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonCredentialedURL, time.Since(started))
		return action.NewFailureResult(fmt.Errorf("checkout action requires url without embedded credentials"))
	}

	fetchRefspecs, err := checkoutFetchRefspecs(inputs)
	if err != nil {
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyValidation, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonInvalidFetchRefspecs, time.Since(started))
		return action.NewFailureResult(err)
	}

	displayURL := redactCloneURL(url)
	cacheState := observability.CheckoutActionCacheOutcomeSkipped

	if cache := c.checkoutCache(state); cache != nil && state != nil {
		cacheStarted := time.Now()
		handled, err := cache.Checkout(ctx, url, state.Workspace, state.Logger)
		if err != nil {
			recordCheckoutActionCacheCheck(ctx, observability.CheckoutActionCacheOutcomeFailed, observability.CheckoutActionReasonCacheError, time.Since(cacheStarted))
			recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyCache, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonCacheError, time.Since(started))
			state.Logger.Error("Cached checkout failed for %s: %v", displayURL, err)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Cached checkout failed: %v", err))
			return action.NewFailureResult(fmt.Errorf("cached checkout failed: %w", err))
		}

		if handled {
			recordCheckoutActionCacheCheck(ctx, observability.CheckoutActionCacheOutcomeHit, observability.CheckoutActionReasonOK, time.Since(cacheStarted))
			if err := c.fetchCachedCheckoutRefspecs(ctx, state, cache, url, fetchRefspecs); err != nil {
				recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyCache, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonGitFetchFailed, time.Since(started))
				state.Logger.Error("Cached checkout ref fetch failed for %s: %v", displayURL, err)
				sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Cached checkout ref fetch failed: %v", err))
				return action.NewFailureResult(fmt.Errorf("cached checkout ref fetch failed: %w", err))
			}

			recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyCache, observability.CheckoutActionOutcomeSuccess, observability.CheckoutActionReasonOK, time.Since(started))
			state.Logger.Info("Checkout completed successfully from worker cache")
			sendLog(state, api.Stream_STREAM_STDOUT, "Checkout completed successfully from worker cache")
			return action.NewSuccessResult(nil)
		}

		cacheState = observability.CheckoutActionCacheOutcomeMiss
		recordCheckoutActionCacheCheck(ctx, cacheState, observability.CheckoutActionReasonNoCache, time.Since(cacheStarted))
	} else {
		recordCheckoutActionCacheCheck(ctx, cacheState, observability.CheckoutActionReasonNoCache, 0)
	}

	state.Logger.Info("Cloning repository: %s", displayURL)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Cloning %s...", displayURL))

	env := action.AppendEnv(state.CommandEnv(), "GIT_TERMINAL_PROMPT", "0")
	cloneStarted := time.Now()
	process, err := c.processExecutor(state).Start(ctx, "git", gitcmd.NoAutoMaintenanceCloneArgs(url, "."), state.Workspace, env)
	if err != nil {
		recordCheckoutActionDirectClone(ctx, cacheState, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonStartFailed, time.Since(cloneStarted))
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonStartFailed, time.Since(started))
		return action.NewFailureResult(fmt.Errorf("failed to start git clone: %w", err))
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(process.Stdout(), state, api.Stream_STREAM_STDOUT)
	}()

	go func() {
		defer wg.Done()
		streamOutput(process.Stderr(), state, api.Stream_STREAM_STDERR)
	}()

	wg.Wait()
	cmdErr := process.Wait()

	if cmdErr != nil {
		recordCheckoutActionDirectClone(ctx, cacheState, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonGitCloneFailed, time.Since(cloneStarted))
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonGitCloneFailed, time.Since(started))
		state.Logger.Error("Git clone failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Git clone failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("git clone failed: %w", cmdErr))
	}

	recordCheckoutActionDirectClone(ctx, cacheState, observability.CheckoutActionOutcomeSuccess, observability.CheckoutActionReasonOK, time.Since(cloneStarted))
	if err := c.fetchCheckoutRefspecs(ctx, state, "origin", fetchRefspecs); err != nil {
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonGitFetchFailed, time.Since(started))
		state.Logger.Error("Checkout ref fetch failed for %s: %v", displayURL, err)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Checkout ref fetch failed: %v", err))
		return action.NewFailureResult(fmt.Errorf("checkout ref fetch failed: %w", err))
	}

	recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeSuccess, observability.CheckoutActionReasonOK, time.Since(started))
	state.Logger.Info("Checkout completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Checkout completed successfully")
	return action.NewSuccessResult(nil)
}

func (c *CheckoutAction) fetchCachedCheckoutRefspecs(ctx context.Context, state *action.ExecutionState, cache action.CheckoutCache, remoteURL string, refspecs []string) error {
	if len(refspecs) == 0 {
		return nil
	}

	if fetcher, ok := cache.(action.CheckoutCacheRefFetcher); ok {
		handled, err := fetcher.FetchRefspecs(ctx, remoteURL, state.Workspace, refspecs, state.Logger)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}

	return c.fetchCheckoutRefspecs(ctx, state, checkoutCacheRemoteName, refspecs)
}

func checkoutFetchRefspecs(inputs map[string]any) ([]string, error) {
	raw, ok := inputs[checkoutFetchRefspecsInput]
	if !ok || raw == nil {
		return nil, nil
	}

	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("checkout action requires %q input to be a string", checkoutFetchRefspecsInput)
	}

	refspecs, err := parseCheckoutFetchRefspecs(value)
	if err != nil {
		return nil, fmt.Errorf("checkout action %q: %w", checkoutFetchRefspecsInput, err)
	}

	return refspecs, nil
}

func parseCheckoutFetchRefspecs(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	fields := strings.Fields(raw)
	seen := make(map[string]struct{}, len(fields))
	out := make([]string, 0, len(fields))
	for _, refspec := range fields {
		if strings.HasPrefix(refspec, "-") {
			return nil, fmt.Errorf("refspec %q must not begin with '-'", refspec)
		}
		if strings.ContainsRune(refspec, 0) {
			return nil, fmt.Errorf("refspec %q contains a NUL byte", refspec)
		}

		if _, ok := seen[refspec]; ok {
			continue
		}

		seen[refspec] = struct{}{}
		out = append(out, refspec)
	}

	return out, nil
}

func (c *CheckoutAction) fetchCheckoutRefspecs(ctx context.Context, state *action.ExecutionState, remote string, refspecs []string) error {
	if len(refspecs) == 0 {
		return nil
	}

	remote = strings.TrimSpace(remote)
	if remote == "" {
		return fmt.Errorf("checkout fetch remote is required")
	}
	if state == nil {
		return fmt.Errorf("checkout fetch requires execution state")
	}

	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Fetching %d additional checkout refspec(s) from %s...", len(refspecs), remote))

	args := gitcmd.NoAutoMaintenanceArgs("fetch", "--no-auto-gc", "--no-tags", "--", remote)
	args = append(args, refspecs...)
	env := action.AppendEnv(state.CommandEnv(), "GIT_TERMINAL_PROMPT", "0")
	process, err := c.processExecutor(state).Start(ctx, "git", args, state.Workspace, env)
	if err != nil {
		return fmt.Errorf("failed to start git fetch: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(process.Stdout(), state, api.Stream_STREAM_STDOUT)
	}()

	go func() {
		defer wg.Done()
		streamOutput(process.Stderr(), state, api.Stream_STREAM_STDERR)
	}()

	wg.Wait()
	if err := process.Wait(); err != nil {
		return fmt.Errorf("git fetch failed: %w", err)
	}

	return nil
}

func (c *CheckoutAction) processExecutor(state *action.ExecutionState) interfaces.ExecExecutor {
	if c.executor != nil {
		return c.executor
	}

	if state != nil && state.ProcessExecutor != nil {
		return state.ProcessExecutor
	}

	return interfaces.NewDirectExecutor()
}

func (c *CheckoutAction) checkoutCache(state *action.ExecutionState) action.CheckoutCache {
	if state != nil {
		if state.CheckoutCache != nil {
			return state.CheckoutCache
		}
	}

	return c.cache
}

func hasCredentialedCloneURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return false
	}

	switch u.Scheme {
	case "http", "https":
		return true
	default:
		return false
	}
}

func redactCloneURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}

	u.User = url.User("redacted")
	return u.String()
}
