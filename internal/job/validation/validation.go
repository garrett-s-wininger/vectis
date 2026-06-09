package validation

import (
	"errors"
	"fmt"
	"sort"
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
			for _, fe := range filterBoundRequiredFieldErrors(fieldErrs, node) {
				v.add(path+".with."+fe.Field, fe.Message)
			}
		}

		v.validateInputs(path, node, resolved)
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

func (v *validator) validateInputs(path string, node *api.Node, resolved action.Node) {
	inputs := node.GetInputs()
	if len(inputs) == 0 {
		return
	}

	specs := action.InputSchema(resolved)
	if len(specs) == 0 {
		v.add(path+".inputs", fmt.Sprintf("action %q does not accept bound inputs", resolved.Type()))
		return
	}

	specByName := make(map[string]action.FieldSpec, len(specs))
	for _, spec := range specs {
		specByName[spec.Name] = spec
	}

	for _, inputName := range sortedInputNames(inputs) {
		binding := inputs[inputName]
		if _, ok := specByName[inputName]; !ok {
			v.add(path+".inputs."+inputName, fmt.Sprintf("unknown input %q for action %q", inputName, resolved.Type()))
			continue
		}

		if _, ok := node.GetWith()[inputName]; ok {
			v.add(path+".inputs."+inputName, "cannot be set together with with."+inputName)
		}

		if binding == nil {
			v.add(path+".inputs."+inputName, "is required")
			continue
		}

		from := binding.GetFrom()
		if from == nil {
			v.add(path+".inputs."+inputName+".from", "is required")
			continue
		}

		nodeID := strings.TrimSpace(from.GetNode())
		outputName := strings.TrimSpace(from.GetOutput())
		if nodeID == "" {
			v.add(path+".inputs."+inputName+".from.node", "is required")
		}

		if outputName == "" {
			v.add(path+".inputs."+inputName+".from.output", "is required")
		}

		if nodeID == "" || outputName == "" {
			continue
		}

		if nodeID == strings.TrimSpace(node.GetId()) {
			v.add(path+".inputs."+inputName+".from.node", "cannot reference the same node")
			continue
		}

		if firstPath, ok := v.seen[nodeID]; !ok {
			v.add(path+".inputs."+inputName+".from.node", fmt.Sprintf("must reference an earlier node id, got %q", nodeID))
		} else if firstPath == path {
			v.add(path+".inputs."+inputName+".from.node", "cannot reference the same node")
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

func filterBoundRequiredFieldErrors(errs []action.FieldError, node *api.Node) []action.FieldError {
	if len(errs) == 0 || len(node.GetInputs()) == 0 {
		return errs
	}

	out := make([]action.FieldError, 0, len(errs))
	for _, err := range errs {
		if err.Message == "is required" {
			if _, hasStatic := node.GetWith()[err.Field]; !hasStatic {
				if _, hasBinding := node.GetInputs()[err.Field]; hasBinding {
					continue
				}
			}
		}

		out = append(out, err)
	}

	return out
}

func sortedInputNames(inputs map[string]*api.NodeInput) []string {
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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
