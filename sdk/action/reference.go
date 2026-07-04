package action

import (
	"fmt"
	"regexp"
	"strings"
)

type SelectorKind string

const (
	SelectorNone    SelectorKind = ""
	SelectorVersion SelectorKind = "version"
	SelectorDigest  SelectorKind = "digest"
)

var (
	referencePartRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	selectorRe      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+:-]*$`)
	sha256DigestRe  = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

type Reference struct {
	Raw          string
	Namespace    string
	Name         string
	Selector     string
	SelectorKind SelectorKind
}

func ParseReference(raw string) (Reference, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return Reference{}, fmt.Errorf("action reference is required")
	}

	namePart, selector, hasSelector := strings.Cut(trimmed, "@")
	if strings.Contains(selector, "@") {
		return Reference{}, fmt.Errorf("action reference must contain at most one selector")
	}

	parts := strings.Split(namePart, "/")
	if len(parts) != 2 {
		return Reference{}, fmt.Errorf("action reference must use namespace/name")
	}

	namespace := parts[0]
	name := parts[1]
	if !ValidReferencePart(namespace) {
		return Reference{}, fmt.Errorf("action namespace %q is invalid", namespace)
	}

	if !ValidReferencePart(name) {
		return Reference{}, fmt.Errorf("action name %q is invalid", name)
	}

	ref := Reference{
		Raw:       trimmed,
		Namespace: namespace,
		Name:      name,
	}

	if !hasSelector {
		return ref, nil
	}

	if selector == "" {
		return Reference{}, fmt.Errorf("action selector is required after @")
	}

	if ValidSHA256Digest(selector) {
		ref.Selector = selector
		ref.SelectorKind = SelectorDigest
		return ref, nil
	}

	if strings.HasPrefix(selector, "sha256:") {
		return Reference{}, fmt.Errorf("action digest selector %q is invalid", selector)
	}

	if !ValidSelector(selector) {
		return Reference{}, fmt.Errorf("action selector %q is invalid", selector)
	}

	ref.Selector = selector
	ref.SelectorKind = SelectorVersion
	return ref, nil
}

func ParseBuiltinReference(uses string) (Reference, error) {
	trimmed := strings.TrimSpace(uses)
	if trimmed != "" && !strings.Contains(trimmed, "/") {
		name, selector, hasSelector := strings.Cut(trimmed, "@")
		trimmed = "builtins/" + name
		if hasSelector {
			trimmed += "@" + selector
		}
	}

	return ParseReference(trimmed)
}

func ValidReferencePart(value string) bool {
	return referencePartRe.MatchString(value)
}

func ValidSelector(value string) bool {
	return selectorRe.MatchString(value)
}

func ValidSHA256Digest(value string) bool {
	return sha256DigestRe.MatchString(value)
}

func (r Reference) CanonicalName() string {
	if r.Namespace == "" || r.Name == "" {
		return ""
	}

	return r.Namespace + "/" + r.Name
}

func (r Reference) String() string {
	base := r.CanonicalName()
	if r.Selector == "" {
		return base
	}

	return base + "@" + r.Selector
}
