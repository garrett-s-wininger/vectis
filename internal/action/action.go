package action

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"vectis/internal/interfaces"
	"vectis/internal/workloadidentity"

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
	Outputs map[string]any
	Error   error
}

type ExecutionState struct {
	JobID        string
	RunID        string
	Workspace    string
	ProcessEnv   []string
	Logger       interfaces.Logger
	LogClient    interfaces.LogClient
	LogStream    interfaces.LogStream
	Resolver     Resolver
	Workload     *workloadidentity.Identity
	nextSequence int64
}

func (s *ExecutionState) NextSequence() int64 {
	return atomic.AddInt64(&s.nextSequence, 1)
}

type Resolver interface {
	Resolve(uses string) (Node, error)
}

const PortUnlimited = -1

type Ports map[string][]*api.Node

func (p Ports) Children(name string) []*api.Node {
	if len(p) == 0 {
		return nil
	}

	return p[name]
}

type PortSpec struct {
	Name     string
	Min      int
	Max      int
	Primary  bool
	Ordered  bool
	Required bool
}

type PortSchemaProvider interface {
	PortSchema() []PortSpec
}

type LocalOnlyProvider interface {
	LocalOnly() bool
}

func PortSchema(node Node) []PortSpec {
	provider, ok := node.(PortSchemaProvider)
	if !ok {
		return nil
	}

	specs := provider.PortSchema()
	out := make([]PortSpec, len(specs))
	copy(out, specs)
	return out
}

func LocalOnly(node Node) bool {
	provider, ok := node.(LocalOnlyProvider)
	return ok && provider.LocalOnly()
}

type FieldError struct {
	Field   string
	Message string
}

type FieldType string

const (
	FieldString FieldType = "string"
	FieldURL    FieldType = "url"
	FieldNumber FieldType = "number"
)

type FieldSpec struct {
	Name     string
	Type     FieldType
	Required bool
}

var scpRe = regexp.MustCompile(`^[\w.-]+@[\w.-]+:[\w./~-]+$`)

func ValidateWithSpec(with map[string]string, specs []FieldSpec) []FieldError {
	if len(specs) == 0 {
		return nil
	}

	var errs []FieldError
	known := make(map[string]FieldSpec, len(specs))
	for _, s := range specs {
		known[s.Name] = s
	}

	for key := range with {
		if _, ok := known[key]; !ok {
			errs = append(errs, FieldError{Field: key, Message: fmt.Sprintf("unknown field %q", key)})
		}
	}

	for _, spec := range specs {
		val, exists := with[spec.Name]
		trimmed := strings.TrimSpace(val)

		if spec.Required && (!exists || trimmed == "") {
			errs = append(errs, FieldError{Field: spec.Name, Message: "is required"})
			continue
		}

		if !exists || trimmed == "" {
			continue
		}

		switch spec.Type {
		case FieldURL:
			if !isValidURL(val) {
				errs = append(errs, FieldError{Field: spec.Name, Message: "must be a valid URL"})
			}
		case FieldNumber:
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				errs = append(errs, FieldError{Field: spec.Name, Message: "must be a valid number"})
			}
		}
	}

	return errs
}

func isValidURL(raw string) bool {
	if u, err := url.Parse(raw); err == nil && u.Scheme != "" && u.Host != "" {
		return true
	}
	return scpRe.MatchString(raw)
}

type Node interface {
	Type() string
	Execute(ctx context.Context, state *ExecutionState, inputs map[string]any, ports Ports) Result
	ValidateWith(with map[string]string) []FieldError
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

func NewSuccessResult(outputs map[string]any) Result {
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
