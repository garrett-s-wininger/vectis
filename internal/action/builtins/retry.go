package builtins

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

const retryDefaultAttempts = 3

type RetryNode struct{}

func (r *RetryNode) ValidateWith(with map[string]string) []action.FieldError {
	errs := validateExecutionMode(with)
	errs = append(errs, action.ValidateWithSpec(with, r.InputSchema())...)

	errs = append(errs, validatePositiveIntField(with, "attempts")...)
	return errs
}

func (r *RetryNode) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "attempts", Type: action.FieldNumber},
	}
}

func (r *RetryNode) Type() string {
	return "builtins/retry"
}

func (r *RetryNode) LocalOnly() bool {
	return true
}

func (r *RetryNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{{
		Name:     taskgraph.BodyPort,
		Min:      1,
		Max:      action.PortUnlimited,
		Required: true,
		Ordered:  true,
	}}
}

func (r *RetryNode) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, ports action.Ports) action.Result {
	body := ports.Children(taskgraph.BodyPort)
	if len(body) == 0 {
		return action.NewFailureResult(fmt.Errorf("retry action requires at least one body node"))
	}

	attempts, err := retryAttempts(inputs)
	if err != nil {
		return action.NewFailureResult(err)
	}

	var last action.Result
	for attempt := 1; attempt <= attempts; attempt++ {
		state.Logger.Info("Executing retry attempt %d/%d", attempt, attempts)
		sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing retry attempt %d/%d", attempt, attempts))

		last = executeOrderedChildren(ctx, state, taskgraph.BodyPort, body)
		if last.Status == action.StatusSuccess {
			if attempt > 1 {
				sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Retry succeeded on attempt %d/%d", attempt, attempts))
			}

			return last
		}

		if ctx.Err() != nil {
			return last
		}

		if attempt < attempts {
			state.Logger.Warn("Retry attempt %d/%d failed: %v", attempt, attempts, last.Error)
			sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Retry attempt %d/%d failed: %v", attempt, attempts, last.Error))
		}
	}

	state.Logger.Error("Retry failed after %d attempt(s): %v", attempts, last.Error)
	sendLog(state, api.Stream_STREAM_STDERR, fmt.Sprintf("Retry failed after %d attempt(s): %v", attempts, last.Error))
	return last
}

func retryAttempts(inputs map[string]any) (int, error) {
	raw, ok := inputs["attempts"].(string)
	if !ok || raw == "" {
		return retryDefaultAttempts, nil
	}

	return positiveIntField(map[string]string{"attempts": raw}, "attempts", retryDefaultAttempts)
}
