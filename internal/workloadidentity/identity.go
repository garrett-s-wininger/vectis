package workloadidentity

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"vectis/internal/serviceidentity"
)

const DefaultSPIFFEPathTemplate = "/cell/{cell}/namespace/{namespace}/job/{job}/run/{run}/execution/{execution}"

type Execution struct {
	CellID            string
	NamespacePath     string
	JobID             string
	RunID             string
	RunIndex          int
	SegmentID         string
	ExecutionID       string
	Attempt           int
	DefinitionVersion int
	DefinitionHash    string
}

type Identity struct {
	SPIFFEID      string
	TrustDomain   string
	NamespacePath string
	CellID        string
	JobID         string
	RunID         string
	ExecutionID   string
}

func SPIFFEID(trustDomain, pathTemplate string, execution Execution) (string, error) {
	trustDomain = strings.TrimSpace(trustDomain)
	if trustDomain == "" {
		return "", fmt.Errorf("workload identity: trust domain is required")
	}

	if strings.ContainsAny(trustDomain, "/?#") {
		return "", fmt.Errorf("workload identity: trust domain %q must not contain path, query, or fragment separators", trustDomain)
	}

	path, err := renderSPIFFEPath(pathTemplate, execution)
	if err != nil {
		return "", err
	}

	u := url.URL{
		Scheme: "spiffe",
		Host:   strings.ToLower(trustDomain),
		Path:   path,
	}
	id := u.String()
	normalized, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{id})
	if err != nil {
		return "", fmt.Errorf("workload identity: %w", err)
	}

	return normalized[0], nil
}

func NewIdentity(trustDomain, pathTemplate string, execution Execution) (*Identity, error) {
	id, err := SPIFFEID(trustDomain, pathTemplate, execution)
	if err != nil {
		return nil, err
	}

	namespacePath := strings.TrimSpace(execution.NamespacePath)
	if namespacePath == "" {
		namespacePath = "/"
	}

	return &Identity{
		SPIFFEID:      id,
		TrustDomain:   strings.ToLower(strings.TrimSpace(trustDomain)),
		NamespacePath: namespacePath,
		CellID:        strings.TrimSpace(execution.CellID),
		JobID:         strings.TrimSpace(execution.JobID),
		RunID:         strings.TrimSpace(execution.RunID),
		ExecutionID:   strings.TrimSpace(execution.ExecutionID),
	}, nil
}

func renderSPIFFEPath(pathTemplate string, execution Execution) (string, error) {
	pathTemplate = strings.TrimSpace(pathTemplate)
	if pathTemplate == "" {
		pathTemplate = DefaultSPIFFEPathTemplate
	}

	if !strings.HasPrefix(pathTemplate, "/") {
		return "", fmt.Errorf("workload identity: SPIFFE path template must start with /")
	}

	values, err := templateValues(execution)
	if err != nil {
		return "", err
	}

	var out strings.Builder
	for i := 0; i < len(pathTemplate); {
		switch pathTemplate[i] {
		case '{':
			end := strings.IndexByte(pathTemplate[i+1:], '}')
			if end < 0 {
				return "", fmt.Errorf("workload identity: unterminated placeholder in SPIFFE path template")
			}

			name := pathTemplate[i+1 : i+1+end]
			value, ok := values[name]
			if !ok {
				return "", fmt.Errorf("workload identity: unknown SPIFFE path placeholder %q", name)
			}

			out.WriteString(value)
			i += end + 2
		case '}':
			return "", fmt.Errorf("workload identity: unmatched } in SPIFFE path template")
		default:
			out.WriteByte(pathTemplate[i])
			i++
		}
	}

	rendered := out.String()
	if rendered == "/" {
		return "", fmt.Errorf("workload identity: SPIFFE path template rendered an empty workload path")
	}

	return rendered, nil
}

func templateValues(execution Execution) (map[string]string, error) {
	namespace, err := namespacePathValue(execution.NamespacePath)
	if err != nil {
		return nil, err
	}

	values := map[string]string{
		"cell":               pathSegment("cell", execution.CellID),
		"namespace":          namespace,
		"job":                pathSegment("job", execution.JobID),
		"run":                pathSegment("run", execution.RunID),
		"run_index":          positiveIntSegment("run_index", execution.RunIndex),
		"segment":            pathSegment("segment", execution.SegmentID),
		"execution":          pathSegment("execution", execution.ExecutionID),
		"attempt":            positiveIntSegment("attempt", execution.Attempt),
		"definition_version": positiveIntSegment("definition_version", execution.DefinitionVersion),
		"definition_hash":    pathSegment("definition_hash", execution.DefinitionHash),
	}

	for key, value := range values {
		if value == "" {
			return nil, fmt.Errorf("workload identity: %s is required for SPIFFE path template", key)
		}
	}

	return values, nil
}

func namespacePathValue(namespacePath string) (string, error) {
	namespacePath = strings.TrimSpace(namespacePath)
	if namespacePath == "" || namespacePath == "/" {
		return "root", nil
	}

	namespacePath = strings.Trim(namespacePath, "/")
	if namespacePath == "" {
		return "root", nil
	}

	parts := strings.Split(namespacePath, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("workload identity: namespace path %q contains an empty segment", namespacePath)
		}

		out = append(out, url.PathEscape(part))
	}

	return strings.Join(out, "/"), nil
}

func pathSegment(name, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	return url.PathEscape(value)
}

func positiveIntSegment(name string, value int) string {
	if value <= 0 {
		return ""
	}

	return strconv.Itoa(value)
}
