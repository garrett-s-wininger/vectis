package action

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/taskgraph"
)

func ResolveNodeInputs(state *ExecutionState, node *api.Node, specs []FieldSpec) (map[string]any, error) {
	inputs := taskgraph.ActionInputs(node.GetWith())
	if len(node.GetInputs()) == 0 {
		return inputs, nil
	}

	if inputs == nil {
		inputs = map[string]any{}
	}

	specByName := make(map[string]FieldSpec, len(specs))
	for _, spec := range specs {
		specByName[spec.Name] = spec
	}

	for _, name := range sortedInputNames(node.GetInputs()) {
		if _, exists := inputs[name]; exists {
			return nil, fmt.Errorf("input %q cannot be set in both with and inputs", name)
		}

		binding := node.GetInputs()[name]
		if binding == nil || binding.GetFrom() == nil {
			return nil, fmt.Errorf("input %q requires from", name)
		}

		ref := binding.GetFrom()
		nodeID := strings.TrimSpace(ref.GetNode())
		outputName := strings.TrimSpace(ref.GetOutput())
		if nodeID == "" {
			return nil, fmt.Errorf("input %q from.node is required", name)
		}

		if outputName == "" {
			return nil, fmt.Errorf("input %q from.output is required", name)
		}

		value, ok := state.Output(nodeID, outputName)
		if !ok {
			return nil, fmt.Errorf("input %q references unavailable output %s.%s", name, nodeID, outputName)
		}

		if spec, ok := specByName[name]; ok {
			coerced, err := coerceBoundInput(name, value, spec)
			if err != nil {
				return nil, err
			}

			value = coerced
		}

		inputs[name] = value
	}

	return inputs, nil
}

func coerceBoundInput(name string, value any, spec FieldSpec) (any, error) {
	switch spec.Type {
	case FieldString:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("input %q must resolve to a string", name)
		}

		return raw, nil
	case FieldURL:
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("input %q must resolve to a URL string", name)
		}

		if !isValidURL(raw) {
			return nil, fmt.Errorf("input %q must resolve to a valid URL", name)
		}

		return raw, nil
	case FieldNumber:
		raw, err := numberInputString(value)
		if err != nil {
			return nil, fmt.Errorf("input %q %w", name, err)
		}

		return raw, nil
	default:
		return value, nil
	}
}

func numberInputString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return "", fmt.Errorf("must resolve to a valid number")
		}

		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	default:
		return "", fmt.Errorf("must resolve to a number")
	}
}

func (s *ExecutionState) RecordOutputs(nodeID string, outputs map[string]any) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}

	copied := make(map[string]any, len(outputs))
	for key, value := range outputs {
		copied[key] = value
	}

	store := s.ensureOutputStore()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.byNode == nil {
		store.byNode = map[string]map[string]any{}
	}

	store.byNode[nodeID] = copied
}

func (s *ExecutionState) Output(nodeID, outputName string) (any, bool) {
	if s == nil {
		return nil, false
	}

	nodeID = strings.TrimSpace(nodeID)
	outputName = strings.TrimSpace(outputName)
	if nodeID == "" || outputName == "" {
		return nil, false
	}

	store := s.ensureOutputStore()
	store.mu.RLock()
	defer store.mu.RUnlock()
	outputs, ok := store.byNode[nodeID]
	if !ok {
		return nil, false
	}

	value, ok := outputs[outputName]
	return value, ok
}

func sortedInputNames(inputs map[string]*api.NodeInput) []string {
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}
