package action

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	JobID                   string
	RunID                   string
	Workspace               string
	ProcessEnv              []string
	Logger                  interfaces.Logger
	LogClient               interfaces.LogClient
	LogStream               interfaces.LogStream
	Artifacts               ArtifactPublisher
	Resolver                Resolver
	Verifier                ActionVerifier
	NodePaths               map[*api.Node]string
	Workload                *workloadidentity.Identity
	ProcessExecutor         interfaces.ExecExecutor
	ProcessExecutorResolver ProcessExecutorResolver
	Isolation               string
	outputsMu               sync.RWMutex
	outputsByNode           map[string]map[string]any
	nextSequence            int64
}

func (s *ExecutionState) NextSequence() int64 {
	return atomic.AddInt64(&s.nextSequence, 1)
}

type Resolver interface {
	Resolve(uses string) (Node, error)
}

type ActionVerifier interface {
	VerifyAction(node *api.Node, path string) error
}

func (s *ExecutionState) NodePath(node *api.Node) string {
	if s == nil || node == nil {
		return ""
	}

	if path := strings.TrimSpace(s.NodePaths[node]); path != "" {
		return path
	}

	return strings.TrimSpace(node.GetId())
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

type InputSchemaProvider interface {
	InputSchema() []FieldSpec
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

func InputSchema(node Node) []FieldSpec {
	provider, ok := node.(InputSchemaProvider)
	if !ok {
		return nil
	}

	specs := provider.InputSchema()
	out := make([]FieldSpec, len(specs))
	copy(out, specs)
	return out
}

func LocalOnly(node Node) bool {
	provider, ok := node.(LocalOnlyProvider)
	return ok && provider.LocalOnly()
}

const (
	IsolationHost = "host"
	IsolationVM   = "vm"
)

// ProcessExecutorResolver maps an effective isolation level to the process
// executor that should run action commands.
type ProcessExecutorResolver interface {
	ResolveProcessExecutor(isolation string) (interfaces.ExecExecutor, string, error)
}

func NormalizeIsolation(isolation string) string {
	return strings.ToLower(strings.TrimSpace(isolation))
}

func IsSupportedIsolation(isolation string) bool {
	switch NormalizeIsolation(isolation) {
	case "", IsolationHost, IsolationVM:
		return true
	default:
		return false
	}
}

func (s *ExecutionState) ApplyNodeIsolation(node *api.Node) (func(), error) {
	restore := func() {}
	if s == nil {
		return restore, nil
	}

	requested := ""
	rawRequested := ""
	if node != nil {
		rawRequested = node.GetIsolation()
		requested = NormalizeIsolation(rawRequested)
	}

	if !IsSupportedIsolation(requested) {
		return nil, fmt.Errorf("unsupported isolation level %q", rawRequested)
	}

	effective := NormalizeIsolation(s.Isolation)
	if effective == "" {
		effective = IsolationHost
	}

	if requested != "" {
		effective = requested
	}

	previousExecutor := s.ProcessExecutor
	previousIsolation := s.Isolation

	if s.ProcessExecutorResolver == nil {
		if requested != "" && effective != IsolationHost {
			return nil, fmt.Errorf("isolation level %q requested but no process executor resolver is configured", requested)
		}

		s.Isolation = effective
		return func() {
			s.ProcessExecutor = previousExecutor
			s.Isolation = previousIsolation
		}, nil
	}

	processExecutor, resolvedIsolation, err := s.ProcessExecutorResolver.ResolveProcessExecutor(effective)
	if err != nil {
		return nil, err
	}

	if resolvedIsolation == "" {
		resolvedIsolation = effective
	}

	s.ProcessExecutor = processExecutor
	s.Isolation = resolvedIsolation
	return func() {
		s.ProcessExecutor = previousExecutor
		s.Isolation = previousIsolation
	}, nil
}

type ArtifactPublisher interface {
	PublishArtifact(ctx context.Context, req ArtifactPublishRequest) (ArtifactPublishResult, error)
}

type ArtifactPublishRequest struct {
	Name         string
	Path         string
	ContentType  string
	MetadataJSON *string
	Reader       io.Reader
	MaxBytes     int64
}

type ArtifactPublishResult struct {
	Name            string
	Path            string
	ContentType     string
	BlobKey         string
	BlobAlgorithm   string
	BlobDigest      string
	SizeBytes       int64
	ArtifactShardID string
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
