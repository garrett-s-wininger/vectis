package action

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

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
			if !IsValidURL(val) {
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

func IsValidURL(raw string) bool {
	if u, err := url.Parse(raw); err == nil && u.Scheme != "" && u.Host != "" {
		return true
	}
	return scpRe.MatchString(raw)
}

func sortFieldErrors(errs []FieldError) {
	sort.Slice(errs, func(i, j int) bool {
		return errs[i].Field < errs[j].Field
	})
}
