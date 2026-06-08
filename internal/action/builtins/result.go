package builtins

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

type ResultAction struct{}

func (r *ResultAction) ValidateWith(with map[string]string) []action.FieldError {
	errs := action.ValidateWithSpec(with, []action.FieldSpec{
		{Name: "success", Type: action.FieldString, Required: true},
	})

	if len(errs) > 0 {
		return errs
	}

	if _, err := parseResultSuccess(with["success"]); err != nil {
		return []action.FieldError{{Field: "success", Message: "must be true or false"}}
	}

	return nil
}

func (r *ResultAction) Type() string {
	return "builtins/result"
}

func (r *ResultAction) Execute(_ context.Context, _ *action.ExecutionState, inputs map[string]any, _ []*api.Node) action.Result {
	raw, ok := inputs["success"].(string)
	if !ok {
		return action.NewFailureResult(fmt.Errorf("result action requires 'success' input"))
	}

	success, err := parseResultSuccess(raw)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("result action success input: %w", err))
	}

	outputs := map[string]any{"success": success}
	if !success {
		return action.Result{
			Status:  action.StatusFailure,
			Outputs: outputs,
			Error:   fmt.Errorf("result action returned false"),
		}
	}

	return action.NewSuccessResult(outputs)
}

func parseResultSuccess(raw string) (bool, error) {
	success, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, err
	}

	return success, nil
}
