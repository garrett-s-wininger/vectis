package builtins

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
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

	rawURL := with["url"]
	if hasCredentialedCloneURL(rawURL) {
		errs = append(errs, action.FieldError{Field: "url", Message: "must not include embedded credentials"})
	}

	return errs
}

func (c *CheckoutAction) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "url", Type: action.FieldURL, Required: true},
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

	displayURL := redactCloneURL(url)

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
			recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyCache, observability.CheckoutActionOutcomeSuccess, observability.CheckoutActionReasonOK, time.Since(started))
			state.Logger.Info("Checkout completed successfully from worker cache")
			sendLog(state, api.Stream_STREAM_STDOUT, "Checkout completed successfully from worker cache")
			return action.NewSuccessResult(nil)
		}

		recordCheckoutActionCacheCheck(ctx, observability.CheckoutActionCacheOutcomeMiss, observability.CheckoutActionReasonNoCache, time.Since(cacheStarted))
	} else {
		recordCheckoutActionCacheCheck(ctx, observability.CheckoutActionCacheOutcomeSkipped, observability.CheckoutActionReasonNoCache, 0)
	}

	state.Logger.Info("Cloning repository: %s", displayURL)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Cloning %s...", displayURL))

	env := action.AppendEnv(state.CommandEnv(), "GIT_TERMINAL_PROMPT", "0")
	process, err := c.processExecutor(state).Start(ctx, "git", []string{"clone", url, "."}, state.Workspace, env)
	if err != nil {
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
		recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeFailed, observability.CheckoutActionReasonGitCloneFailed, time.Since(started))
		state.Logger.Error("Git clone failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Git clone failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("git clone failed: %w", cmdErr))
	}

	recordCheckoutActionResult(ctx, observability.CheckoutActionStrategyDirect, observability.CheckoutActionOutcomeSuccess, observability.CheckoutActionReasonOK, time.Since(started))
	state.Logger.Info("Checkout completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Checkout completed successfully")
	return action.NewSuccessResult(nil)
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
