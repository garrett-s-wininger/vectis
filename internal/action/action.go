package action

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"vectis/internal/interfaces"
	"vectis/internal/workloadidentity"
	sdkaction "vectis/sdk/action"

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
	CheckoutCache           CheckoutCache
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
	outputs                 *executionOutputStore
	sequence                *executionSequence
}

type executionOutputStore struct {
	mu     sync.RWMutex
	byNode map[string]map[string]any
}

type executionSequence struct {
	next int64
}

var executionStateInitMu sync.Mutex

type CheckoutCache interface {
	Checkout(ctx context.Context, remoteURL, workspace string, logger interfaces.Logger) (bool, error)
}

type CheckoutCacheScopeProvider interface {
	NewCheckoutCacheScope() (CheckoutCache, error)
}

type CheckoutCacheRefFetcher interface {
	FetchRefspecs(ctx context.Context, remoteURL, workspace string, refspecs []string, logger interfaces.Logger) (bool, error)
}

type CheckoutCacheCloser interface {
	Close() error
}

func (s *ExecutionState) NextSequence() int64 {
	if s == nil {
		return 0
	}

	sequence := s.ensureSequence()
	return atomic.AddInt64(&sequence.next, 1)
}

func (s *ExecutionState) ForConcurrentChild() *ExecutionState {
	if s == nil {
		return nil
	}

	outputs := s.ensureOutputStore()
	sequence := s.ensureSequence()
	child := *s
	child.outputs = outputs
	child.sequence = sequence
	return &child
}

func (s *ExecutionState) ensureOutputStore() *executionOutputStore {
	executionStateInitMu.Lock()
	defer executionStateInitMu.Unlock()

	if s.outputs == nil {
		s.outputs = &executionOutputStore{}
	}

	return s.outputs
}

func (s *ExecutionState) ensureSequence() *executionSequence {
	executionStateInitMu.Lock()
	defer executionStateInitMu.Unlock()

	if s.sequence == nil {
		s.sequence = &executionSequence{}
	}

	return s.sequence
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
	isolation = NormalizeIsolation(isolation)
	return isolation == "" || IsSupportedIsolationLevel(isolation)
}

func IsSupportedIsolationLevel(isolation string) bool {
	switch NormalizeIsolation(isolation) {
	case IsolationHost, IsolationVM:
		return true
	default:
		return false
	}
}

func NormalizeSupportedIsolationLevels(levels []string) ([]string, error) {
	if len(levels) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, len(levels))
	normalized := make([]string, 0, len(levels))
	for i, raw := range levels {
		level := NormalizeIsolation(raw)
		if level == "" {
			return nil, fmt.Errorf("supported isolation entry %d must not be empty", i)
		}

		if !IsSupportedIsolationLevel(level) {
			return nil, fmt.Errorf("unsupported isolation %q", raw)
		}

		if _, ok := seen[level]; ok {
			continue
		}

		seen[level] = struct{}{}
		normalized = append(normalized, level)
	}

	return normalized, nil
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
	ExpectedSize int64
	RequireSize  bool
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

type FieldError = sdkaction.FieldError
type FieldType = sdkaction.FieldType

const (
	FieldString = sdkaction.FieldString
	FieldURL    = sdkaction.FieldURL
	FieldNumber = sdkaction.FieldNumber
)

type FieldSpec = sdkaction.FieldSpec

func ValidateWithSpec(with map[string]string, specs []FieldSpec) []FieldError {
	return sdkaction.ValidateWithSpec(with, specs)
}

func isValidURL(raw string) bool {
	return sdkaction.IsValidURL(raw)
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
