package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

var (
	httpMethods        = map[string]bool{"delete": true, "get": true, "head": true, "options": true, "patch": true, "post": true, "put": true, "trace": true}
	pathItemFields     = map[string]bool{"$ref": true, "description": true, "parameters": true, "servers": true, "summary": true}
	operationIDPattern = regexp.MustCompile(`^[a-z][A-Za-z0-9]*$`)
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [openapi-json]\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Checks the Vectis OpenAPI contract for route metadata and reference consistency.")
	}

	flag.Parse()
	target := "website/static/openapi/v1.json"
	if flag.NArg() > 0 {
		target = flag.Arg(0)
	}

	diagnostics, err := lintFile(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openapi-lint: %v\n", err)
		os.Exit(2)
	}

	for _, diag := range diagnostics {
		fmt.Fprintln(os.Stderr, diag)
	}

	if len(diagnostics) > 0 {
		os.Exit(1)
	}
}

func lintFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	return lintDocument(doc), nil
}

func lintDocument(doc map[string]any) []string {
	var diagnostics []string
	add := func(format string, args ...any) {
		diagnostics = append(diagnostics, fmt.Sprintf(format, args...))
	}

	openapi := stringField(doc, "openapi")
	if openapi == "" {
		add("openapi: missing OpenAPI version")
	} else if !strings.HasPrefix(openapi, "3.0.") {
		add("openapi: expected OpenAPI 3.0.x, got %q", openapi)
	}

	info, ok := objectField(doc, "info")
	if !ok {
		add("info: missing info object")
	} else {
		for _, field := range []string{"title", "version", "description"} {
			if stringField(info, field) == "" {
				add("info.%s: missing non-empty value", field)
			}
		}
	}

	declaredTags := collectDeclaredTags(doc, add)
	securitySchemes := collectSecuritySchemes(doc, add)
	paths, ok := objectField(doc, "paths")
	if !ok || len(paths) == 0 {
		add("paths: missing non-empty paths object")
	}

	seenOperationIDs := map[string]string{}
	pathNames := sortedKeys(paths)
	for _, path := range pathNames {
		item, ok := asObject(paths[path])
		if !ok {
			add("paths.%s: path item must be an object", path)
			continue
		}

		lintPathItem(path, item, declaredTags, securitySchemes, seenOperationIDs, add)
	}

	walkRefs(doc, "$", doc, add)

	sort.Strings(diagnostics)
	return diagnostics
}

func collectDeclaredTags(doc map[string]any, add func(string, ...any)) map[string]bool {
	declared := map[string]bool{}
	rawTags, ok := asArray(doc["tags"])
	if !ok || len(rawTags) == 0 {
		add("tags: missing non-empty top-level tags array")
		return declared
	}

	for i, raw := range rawTags {
		tag, ok := asObject(raw)
		if !ok {
			add("tags[%d]: tag must be an object", i)
			continue
		}

		name := stringField(tag, "name")
		if name == "" {
			add("tags[%d].name: missing non-empty value", i)
			continue
		}
		if declared[name] {
			add("tags[%d].name: duplicate tag %q", i, name)
		}
		declared[name] = true
	}

	return declared
}

func collectSecuritySchemes(doc map[string]any, add func(string, ...any)) map[string]bool {
	components, ok := objectField(doc, "components")
	if !ok {
		add("components: missing components object")
		return nil
	}

	securitySchemes, ok := objectField(components, "securitySchemes")
	if !ok {
		add("components.securitySchemes: missing security schemes")
		return nil
	}

	declared := map[string]bool{}
	for name := range securitySchemes {
		declared[name] = true
	}

	bearerAuth, ok := objectField(securitySchemes, "bearerAuth")
	if !ok {
		add("components.securitySchemes.bearerAuth: missing bearer auth scheme")
		return declared
	}

	if stringField(bearerAuth, "type") != "http" || stringField(bearerAuth, "scheme") != "bearer" {
		add("components.securitySchemes.bearerAuth: expected HTTP bearer scheme")
	}

	return declared
}

func lintPathItem(path string, item map[string]any, declaredTags, securitySchemes map[string]bool, seenOperationIDs map[string]string, add func(string, ...any)) {
	for _, key := range sortedKeys(item) {
		if httpMethods[key] {
			op, ok := asObject(item[key])
			if !ok {
				add("%s %s: operation must be an object", strings.ToUpper(key), path)
				continue
			}

			lintOperation(path, key, op, declaredTags, securitySchemes, seenOperationIDs, add)
			continue
		}

		if !pathItemFields[key] {
			add("paths.%s.%s: unsupported path item field", path, key)
		}
	}
}

func lintOperation(path, method string, op map[string]any, declaredTags, securitySchemes map[string]bool, seenOperationIDs map[string]string, add func(string, ...any)) {
	context := fmt.Sprintf("%s %s", strings.ToUpper(method), path)
	operationID := stringField(op, "operationId")
	if operationID == "" {
		add("%s: missing operationId", context)
	} else {
		if !operationIDPattern.MatchString(operationID) {
			add("%s: operationId %q must be lowerCamelCase alphanumeric", context, operationID)
		}

		if previous, exists := seenOperationIDs[operationID]; exists {
			add("%s: duplicate operationId %q also used by %s", context, operationID, previous)
		}

		seenOperationIDs[operationID] = context
	}

	if stringField(op, "summary") == "" {
		add("%s: missing summary", context)
	}

	rawTags, ok := asArray(op["tags"])
	if !ok || len(rawTags) == 0 {
		add("%s: missing non-empty tags array", context)
	} else {
		for i, raw := range rawTags {
			tag, ok := raw.(string)
			if !ok || strings.TrimSpace(tag) == "" {
				add("%s: tags[%d] must be a non-empty string", context, i)
				continue
			}

			if !declaredTags[tag] {
				add("%s: tag %q is not declared in top-level tags", context, tag)
			}
		}
	}

	hasBearerAuth := lintOperationSecurity(context, op, securitySchemes, add)
	authAction, hasAuthAction := op["x-vectis-auth-action"]
	if hasAuthAction {
		authActionString, ok := authAction.(string)
		if !ok || strings.TrimSpace(authActionString) == "" {
			add("%s: x-vectis-auth-action must be a non-empty string when present", context)
		}
	}

	if hasBearerAuth && !hasAuthAction {
		add("%s: bearer-protected operation must declare x-vectis-auth-action", context)
	}

	responses, ok := objectField(op, "responses")
	if !ok || len(responses) == 0 {
		add("%s: missing non-empty responses object", context)
		return
	}

	if _, ok := responses["default"]; !ok {
		add("%s: missing default error response", context)
	}

	for status, rawResponse := range responses {
		response, ok := asObject(rawResponse)
		if !ok {
			add("%s responses.%s: response must be an object", context, status)
			continue
		}

		if stringField(response, "$ref") == "" && stringField(response, "description") == "" {
			add("%s responses.%s: inline response must have a description", context, status)
		}
	}
}

func lintOperationSecurity(context string, op map[string]any, securitySchemes map[string]bool, add func(string, ...any)) bool {
	rawSecurity, ok := asArray(op["security"])
	if !ok {
		add("%s: missing explicit security array", context)
		return false
	}

	hasBearerAuth := false
	for i, rawRequirement := range rawSecurity {
		requirement, ok := asObject(rawRequirement)
		if !ok {
			add("%s security[%d]: requirement must be an object", context, i)
			continue
		}

		for scheme := range requirement {
			if !securitySchemes[scheme] {
				add("%s security[%d]: undeclared security scheme %q", context, i, scheme)
			}

			if scheme == "bearerAuth" {
				hasBearerAuth = true
			}
		}
	}

	return hasBearerAuth
}

func walkRefs(value any, path string, root map[string]any, add func(string, ...any)) {
	switch typed := value.(type) {
	case map[string]any:
		if ref := stringField(typed, "$ref"); ref != "" {
			if err := resolveLocalRef(root, ref); err != nil {
				add("%s.$ref: %v", path, err)
			}
		}

		for _, key := range sortedKeys(typed) {
			walkRefs(typed[key], path+"."+key, root, add)
		}
	case []any:
		for i, item := range typed {
			walkRefs(item, fmt.Sprintf("%s[%d]", path, i), root, add)
		}
	}
}

func resolveLocalRef(root map[string]any, ref string) error {
	if !strings.HasPrefix(ref, "#/") {
		return fmt.Errorf("external references are not allowed: %q", ref)
	}

	var current any = root
	for _, part := range strings.Split(strings.TrimPrefix(ref, "#/"), "/") {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		object, ok := asObject(current)
		if !ok {
			return fmt.Errorf("unresolvable reference %q", ref)
		}

		current, ok = object[part]
		if !ok {
			return fmt.Errorf("unresolvable reference %q", ref)
		}
	}

	return nil
}

func objectField(object map[string]any, field string) (map[string]any, bool) {
	return asObject(object[field])
}

func stringField(object map[string]any, field string) string {
	value, ok := object[field].(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

func asObject(value any) (map[string]any, bool) {
	object, ok := value.(map[string]any)
	return object, ok
}

func asArray(value any) ([]any, bool) {
	array, ok := value.([]any)
	return array, ok
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}
