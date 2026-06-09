package secrets

import (
	"context"
	"fmt"
	"path"
	"strings"
)

type AccessPolicy interface {
	AuthorizeResolve(ctx context.Context, req ResolveRequest) error
}

type StaticAccessPolicy struct {
	rules []AccessRule
}

type AccessRule struct {
	Namespace string
	Job       string
	Task      string
	Ref       string
}

func NewAccessPolicy(specs []string) (*StaticAccessPolicy, error) {
	rules := make([]AccessRule, 0, len(specs))
	for i, spec := range specs {
		rule, err := ParseAccessRule(spec)
		if err != nil {
			return nil, fmt.Errorf("secret access policy rule %d: %w", i+1, err)
		}

		rules = append(rules, rule)
	}

	return &StaticAccessPolicy{rules: rules}, nil
}

func (p *StaticAccessPolicy) AuthorizeResolve(ctx context.Context, req ResolveRequest) error {
	if p == nil {
		return nil
	}

	if len(p.rules) == 0 {
		return fmt.Errorf("%w: no secret access policy rule matched", ErrDenied)
	}

	for _, ref := range req.Secrets {
		if err := ctx.Err(); err != nil {
			return err
		}

		if !p.allows(req.Scope, ref) {
			return fmt.Errorf("%w: secret %q is not allowed for this execution", ErrDenied, ref.ID)
		}
	}

	return nil
}

func (p *StaticAccessPolicy) allows(scope ExecutionScope, ref Reference) bool {
	for _, rule := range p.rules {
		if rule.matches(scope, ref) {
			return true
		}
	}

	return false
}

func (r AccessRule) matches(scope ExecutionScope, ref Reference) bool {
	return matchPolicyPattern(r.Namespace, normalizePolicyNamespace(scope.NamespacePath)) &&
		matchPolicyPattern(r.Job, strings.TrimSpace(scope.JobID)) &&
		matchPolicyPattern(r.Task, strings.TrimSpace(scope.TaskKey)) &&
		matchPolicyPattern(r.Ref, strings.TrimSpace(ref.Ref))
}

func ParseAccessRule(spec string) (AccessRule, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return AccessRule{}, fmt.Errorf("rule is empty")
	}

	rule := AccessRule{
		Namespace: "*",
		Job:       "*",
		Task:      "*",
	}

	seen := map[string]struct{}{}
	for _, part := range strings.Split(spec, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return AccessRule{}, fmt.Errorf("part %q must be key=value", part)
		}

		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return AccessRule{}, fmt.Errorf("part %q must include non-empty key and value", part)
		}

		if _, ok := seen[key]; ok {
			return AccessRule{}, fmt.Errorf("duplicate key %q", key)
		}

		seen[key] = struct{}{}

		switch key {
		case "namespace", "namespace_path":
			rule.Namespace = normalizePolicyNamespacePattern(value)
		case "job", "job_id":
			rule.Job = value
		case "task", "task_key":
			rule.Task = value
		case "ref", "secret", "secret_ref":
			rule.Ref = value
		default:
			return AccessRule{}, fmt.Errorf("unknown key %q", key)
		}
	}

	if strings.TrimSpace(rule.Ref) == "" {
		return AccessRule{}, fmt.Errorf("ref is required")
	}

	return rule, nil
}

func normalizePolicyNamespacePattern(pattern string) string {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "*" {
		return "*"
	}

	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		if prefix == "" {
			return "*"
		}

		if !strings.HasPrefix(prefix, "/") {
			prefix = "/" + prefix
		}

		hadTrailingSlash := strings.HasSuffix(prefix, "/")
		prefix = path.Clean(prefix)
		if hadTrailingSlash && prefix != "/" {
			prefix += "/"
		}

		return prefix + "*"
	}

	return normalizePolicyNamespace(pattern)
}

func normalizePolicyNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" || namespace == "/" {
		return "/"
	}

	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}

	return path.Clean(namespace)
}

func matchPolicyPattern(pattern, value string) bool {
	pattern = strings.TrimSpace(pattern)
	value = strings.TrimSpace(value)
	if pattern == "" || pattern == "*" {
		return true
	}

	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(value, strings.TrimSuffix(pattern, "*"))
	}

	return pattern == value
}
