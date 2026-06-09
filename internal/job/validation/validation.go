package validation

import (
	"errors"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/builtins"
	"vectis/internal/dal"
	"vectis/internal/taskgraph"
)

const (
	DefaultMaxNodes = 256
	DefaultMaxDepth = 32
)

type Options struct {
	RequireJobID bool
	MaxNodes     int
	MaxDepth     int
	Resolver     interface {
		Resolve(string) (action.Node, error)
	}
}

type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e FieldError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

type Error struct {
	Fields []FieldError
}

func (e *Error) Error() string {
	if e == nil || len(e.Fields) == 0 {
		return "invalid job definition"
	}

	parts := make([]string, 0, len(e.Fields))
	for _, field := range e.Fields {
		parts = append(parts, field.Error())
	}

	return strings.Join(parts, "; ")
}

func ErrorDetails(err error) map[string]any {
	details := map[string]any{}

	var validationErr *Error
	if errors.As(err, &validationErr) {
		fields := make([]FieldError, len(validationErr.Fields))
		copy(fields, validationErr.Fields)
		details["fields"] = fields
	}

	return details
}

func ValidateJob(job *api.Job, opts Options) error {
	opts = normalizeOptions(opts)
	v := validator{
		opts: opts,
		seen: make(map[string]string),
	}

	if job == nil {
		v.add("job", "is required")
		return v.err()
	}

	if opts.RequireJobID && strings.TrimSpace(job.GetId()) == "" {
		v.add("id", "is required")
	}

	if job.GetRoot() == nil {
		v.add("root", "is required")
		return v.err()
	}

	v.walk(job.GetRoot(), "root", 1)
	return v.err()
}

type validator struct {
	opts  Options
	seen  map[string]string
	count int
	errs  []FieldError
}

func normalizeOptions(opts Options) Options {
	if opts.MaxNodes <= 0 {
		opts.MaxNodes = DefaultMaxNodes
	}

	if opts.MaxDepth <= 0 {
		opts.MaxDepth = DefaultMaxDepth
	}

	if opts.Resolver == nil {
		opts.Resolver = builtins.NewRegistry()
	}

	return opts
}

func (v *validator) walk(node *api.Node, path string, depth int) {
	if node == nil {
		v.add(path, "is required")
		return
	}

	v.count++
	if v.count > v.opts.MaxNodes {
		v.add(path, fmt.Sprintf("exceeds maximum node count %d", v.opts.MaxNodes))
		return
	}

	if depth > v.opts.MaxDepth {
		v.add(path, fmt.Sprintf("exceeds maximum depth %d", v.opts.MaxDepth))
		return
	}

	id := strings.TrimSpace(node.GetId())
	if id == "" {
		v.add(path+".id", "is required")
	} else if id == dal.RootTaskKey && path != "root" {
		v.add(path+".id", fmt.Sprintf("%q is reserved for the root task", dal.RootTaskKey))
	} else if firstPath, ok := v.seen[id]; ok {
		v.add(path+".id", fmt.Sprintf("duplicates node id %q first used at %s", id, firstPath+".id"))
	} else {
		v.seen[id] = path
	}

	uses := strings.TrimSpace(node.GetUses())
	if raw, ok := node.GetWith()[taskgraph.ExecutionField]; ok && !taskgraph.ValidExecutionMode(raw) {
		v.add(
			path+".with."+taskgraph.ExecutionField,
			fmt.Sprintf("must be %q or %q, got %q", taskgraph.ExecutionLocal, taskgraph.ExecutionDistributed, strings.TrimSpace(raw)),
		)
	}

	if uses == "" {
		v.add(path+".uses", "is required")
	} else if resolved, err := v.opts.Resolver.Resolve(uses); err != nil {
		v.add(path+".uses", fmt.Sprintf("unknown action %q", uses))
	} else {
		if fieldErrs := resolved.ValidateWith(taskgraph.ActionWith(node.GetWith())); len(fieldErrs) > 0 {
			for _, fe := range fieldErrs {
				v.add(path+".with."+fe.Field, fe.Message)
			}
		}

		v.validatePorts(path, node, resolved)
		v.validateExecutionScope(path, node, resolved)
	}

	for _, ref := range taskgraph.ChildRefs(node, path) {
		v.walk(ref.Node, ref.Path, depth+1)
	}
}

func (v *validator) validatePorts(path string, node *api.Node, resolved action.Node) {
	specs := action.PortSchema(resolved)
	specByName := make(map[string]action.PortSpec, len(specs))
	primaryPort := ""
	for _, spec := range specs {
		specByName[spec.Name] = spec
		if spec.Primary {
			primaryPort = spec.Name
		}
	}

	if len(node.GetSteps()) > 0 {
		if primaryPort == "" {
			v.add(path+".steps", fmt.Sprintf("action %q does not accept child steps", resolved.Type()))
		} else if taskgraph.HasExplicitPort(node, primaryPort) {
			v.add(path+".steps", fmt.Sprintf("cannot be used together with ports.%s", primaryPort))
		}
	}

	for portName, port := range node.GetPorts() {
		spec, ok := specByName[portName]
		if !ok {
			v.add(path+".ports."+portName, fmt.Sprintf("unknown port %q for action %q", portName, resolved.Type()))
			continue
		}

		v.validatePortCardinality(path, portName, len(port.GetNodes()), spec)
	}

	if len(node.GetSteps()) > 0 && primaryPort != "" {
		if spec, ok := specByName[primaryPort]; ok && !taskgraph.HasExplicitPort(node, primaryPort) {
			v.validatePortCardinality(path, primaryPort, len(node.GetSteps()), spec)
		}
	}

	for _, spec := range specs {
		if spec.Min <= 0 && !spec.Required {
			continue
		}

		count := len(taskgraph.ExplicitPortChildren(node, spec.Name))
		if spec.Name == primaryPort && len(node.GetSteps()) > 0 && !taskgraph.HasExplicitPort(node, primaryPort) {
			count = len(node.GetSteps())
		}

		min := spec.Min
		if spec.Required && min == 0 {
			min = 1
		}

		if count < min {
			v.add(path+".ports."+spec.Name, fmt.Sprintf("requires at least %d node(s)", min))
		}
	}
}

func (v *validator) validateExecutionScope(path string, node *api.Node, resolved action.Node) {
	if !action.LocalOnly(resolved) {
		return
	}

	if taskgraph.ExecutionMode(node) == taskgraph.ExecutionDistributed {
		v.add(path+".with."+taskgraph.ExecutionField, fmt.Sprintf("must be %q for action %q", taskgraph.ExecutionLocal, resolved.Type()))
	}

	for _, ref := range taskgraph.ChildRefs(node, path) {
		if taskgraph.ContainsDistributedBoundary(ref.Node) {
			v.add(path+".ports", fmt.Sprintf("action %q only supports local child ports for now; %s contains a distributed boundary", resolved.Type(), ref.Path))
			return
		}
	}
}

func (v *validator) validatePortCardinality(path, portName string, count int, spec action.PortSpec) {
	min := spec.Min
	if spec.Required && min == 0 {
		min = 1
	}

	if count < min {
		v.add(path+".ports."+portName, fmt.Sprintf("requires at least %d node(s)", min))
	}

	if spec.Max >= 0 && count > spec.Max {
		v.add(path+".ports."+portName, fmt.Sprintf("allows at most %d node(s)", spec.Max))
	}
}

func (v *validator) add(field, message string) {
	v.errs = append(v.errs, FieldError{Field: field, Message: message})
}

func (v *validator) err() error {
	if len(v.errs) == 0 {
		return nil
	}

	return &Error{Fields: v.errs}
}
