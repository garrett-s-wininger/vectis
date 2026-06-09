package builtins

import (
	"context"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/taskgraph"
)

type TimeoutNode struct{}

func (t *TimeoutNode) ValidateWith(with map[string]string) []action.FieldError {
	errs := validateExecutionMode(with)
	errs = append(errs, action.ValidateWithSpec(with, t.InputSchema())...)

	errs = append(errs, validateDurationField(with, "duration")...)
	return errs
}

func (t *TimeoutNode) InputSchema() []action.FieldSpec {
	return []action.FieldSpec{
		{Name: "duration", Type: action.FieldString, Required: true},
	}
}

func (t *TimeoutNode) Type() string {
	return "builtins/timeout"
}

func (t *TimeoutNode) LocalOnly() bool {
	return true
}

func (t *TimeoutNode) PortSchema() []action.PortSpec {
	return []action.PortSpec{{
		Name:     taskgraph.BodyPort,
		Min:      1,
		Max:      action.PortUnlimited,
		Required: true,
		Ordered:  true,
	}}
}

func (t *TimeoutNode) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, ports action.Ports) action.Result {
	body := ports.Children(taskgraph.BodyPort)
	if len(body) == 0 {
		return action.NewFailureResult(fmt.Errorf("timeout action requires at least one body node"))
	}

	duration, err := timeoutDuration(inputs)
	if err != nil {
		return action.NewFailureResult(err)
	}

	state.Logger.Info("Executing timeout body with duration %s", duration)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Executing timeout body with duration %s", duration))

	timeoutCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	result := executeOrderedChildren(timeoutCtx, state, taskgraph.BodyPort, body)
	if timeoutCtx.Err() == context.DeadlineExceeded {
		err := fmt.Errorf("timeout exceeded after %s", duration)
		state.Logger.Error("%v", err)
		sendLog(state, api.Stream_STREAM_STDERR, err.Error())

		return action.NewFailureResult(err)
	}

	return result
}

func timeoutDuration(inputs map[string]any) (time.Duration, error) {
	raw, ok := inputs["duration"].(string)
	if !ok || raw == "" {
		return 0, fmt.Errorf("duration is required")
	}

	duration, err := durationField(map[string]string{"duration": raw}, "duration")
	if err != nil {
		return 0, err
	}

	return duration, nil
}
