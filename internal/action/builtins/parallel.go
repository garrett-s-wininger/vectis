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

func (p *ParallelNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{{
		Name:    taskgraph.BranchesPort,
		Max:     action.PortUnlimited,
		Primary: true,
	}}
}

func (p *ParallelNode) Execute(ctx context.Context, state *action.ExecutionState, _ map[string]any, ports action.Ports) action.Result {
	branches := ports.Children(taskgraph.BranchesPort)
	if len(branches) == 0 {
		return action.NewSuccessResult(nil)
	}

	state.Logger.Info("Executing parallel with %d branches", len(branches))
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing parallel with %d branches", len(branches)))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstFailure action.Result
	failed := false

	for i, child := range branches {
		i, child := i, child
		wg.Add(1)
		go func() {
			defer wg.Done()

			sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing branch %d/%d", i+1, len(branches)))

			result := executeChildNode(ctx, child, state)
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
