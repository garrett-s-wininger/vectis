package builtins

import (
	"fmt"
	"strings"

	"vectis/internal/action"
)

type Registry struct {
	nodes map[string]action.Node
}

func NewRegistry() *Registry {
	r := &Registry{
		nodes: make(map[string]action.Node),
	}

	r.Register(&ShellAction{})
	r.Register(&SequenceNode{})

	return r
}

func (r *Registry) Register(n action.Node) {
	r.nodes[n.Type()] = n
}

func (r *Registry) Resolve(uses string) (action.Node, error) {
	// NOTE(garrett): Allow both namespaced and non-namespaced for builtin actions
	// for easier use.
	nodeType := uses
	if !strings.HasPrefix(uses, "builtins/") && !strings.Contains(uses, "/") {
		nodeType = "builtins/" + uses
	}

	n, ok := r.nodes[nodeType]
	if !ok {
		return nil, fmt.Errorf("unknown action: %s", uses)
	}

	return n, nil
}

func (r *Registry) IsBuiltin(uses string) bool {
	_, err := r.Resolve(uses)
	return err == nil
}
