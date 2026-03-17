package builtins

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/interfaces"
)

type CheckoutAction struct {
	executor interfaces.ExecExecutor
}

func NewCheckoutAction(executor interfaces.ExecExecutor) *CheckoutAction {
	if executor == nil {
		executor = interfaces.NewDirectExecutor()
	}
	return &CheckoutAction{
		executor: executor,
	}
}

func (c *CheckoutAction) Type() string {
	return "builtins/checkout"
}

func (c *CheckoutAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ []*api.Node) action.Result {
	url, ok := inputs["url"].(string)
	if !ok || url == "" {
		return action.NewFailureResult(fmt.Errorf("checkout action requires 'url' input"))
	}

	state.Logger.Info("Cloning repository: %s", url)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Cloning %s...", url))

	process, err := c.executor.Start(ctx, "git", []string{"clone", url, "."}, state.Workspace)
	if err != nil {
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
		state.Logger.Error("Git clone failed: %v", cmdErr)
		sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Git clone failed: %v", cmdErr))
		return action.NewFailureResult(fmt.Errorf("git clone failed: %w", cmdErr))
	}

	state.Logger.Info("Checkout completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Checkout completed successfully")
	return action.NewSuccessResult(nil)
}
