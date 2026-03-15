package action

import (
	"context"
	"fmt"
	"sync/atomic"

	"vectis/internal/log"

	api "vectis/api/gen/go"
)

type Status int

const (
	StatusSuccess Status = iota
	StatusFailure
)

func (s Status) String() string {
	switch s {
	case StatusSuccess:
		return "success"
	case StatusFailure:
		return "failure"
	default:
		return "unknown"
	}
}

type Result struct {
	Status  Status
	Outputs map[string]interface{}
	Error   error
}

type ExecutionState struct {
	JobID        string
	Logger       *log.Logger
	LogClient    api.LogServiceClient
	LogStream    api.LogService_StreamLogsClient
	nextSequence int64
}

func (s *ExecutionState) NextSequence() int64 {
	return atomic.AddInt64(&s.nextSequence, 1)
}

type Node interface {
	Type() string
	Execute(ctx context.Context, state *ExecutionState, inputs map[string]interface{}, children []*api.Node) Result
}

type ExecutionError struct {
	NodeID  string
	Action  string
	Message string
	Cause   error
}

func (e *ExecutionError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("execution error in node %s (%s): %s: %v", e.NodeID, e.Action, e.Message, e.Cause)
	}
	return fmt.Sprintf("execution error in node %s (%s): %s", e.NodeID, e.Action, e.Message)
}

func (e *ExecutionError) Unwrap() error {
	return e.Cause
}

func NewSuccessResult(outputs map[string]interface{}) Result {
	return Result{
		Status:  StatusSuccess,
		Outputs: outputs,
	}
}

func NewFailureResult(err error) Result {
	return Result{
		Status: StatusFailure,
		Error:  err,
	}
}
