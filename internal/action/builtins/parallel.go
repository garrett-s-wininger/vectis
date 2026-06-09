package builtins

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type ParallelNode struct{}

func (p *ParallelNode) ValidateWith(with map[string]string) []action.FieldError {
	return validateExecutionMode(with)
}

func (p *ParallelNode) Type() string {
	return "builtins/parallel"
}

func (p *ParallelNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, children []*api.Node) action.Result {
	if len(children) == 0 {
		return action.NewSuccessResult(nil)
	}

	state.Logger.Info("Executing parallel with %d children", len(children))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing parallel with %d children", len(children)))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstFailure action.Result
	failed := false

	for i, child := range children {
		i, child := i, child
		wg.Add(1)
		go func() {
			defer wg.Done()

			sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing branch %d/%d", i+1, len(children)))

			result := executeChildNode(ctx, child, state, taskgraph.ActionInputs(child.GetWith()))
			if result.Status != action.StatusFailure {
				return
			}

			mu.Lock()
			defer mu.Unlock()
			if !failed {
				failed = true
				firstFailure = result
			}
		}()
	}

	wg.Wait()
	if failed {
		if firstFailure.Error != nil {
			state.Logger.Error("Parallel failed: %v", firstFailure.Error)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Parallel failed: %v", firstFailure.Error))
		}

		return firstFailure
	}

	state.Logger.Info("Parallel completed successfully")
	sendLog(state, api.Stream_STREAM_STDOUT, "Parallel completed successfully")
	return action.NewSuccessResult(nil)
}
