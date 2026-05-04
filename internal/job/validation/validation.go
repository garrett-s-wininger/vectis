package validation

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/builtins"
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
	Field   string
	Message string
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
	} else if firstPath, ok := v.seen[id]; ok {
		v.add(path+".id", fmt.Sprintf("duplicates node id %q first used at %s", id, firstPath+".id"))
	} else {
		v.seen[id] = path
	}

	uses := strings.TrimSpace(node.GetUses())
	if uses == "" {
		v.add(path+".uses", "is required")
	} else if _, err := v.opts.Resolver.Resolve(uses); err != nil {
		v.add(path+".uses", fmt.Sprintf("unknown action %q", uses))
	}

	for i, child := range node.GetSteps() {
		v.walk(child, fmt.Sprintf("%s.steps[%d]", path, i), depth+1)
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
