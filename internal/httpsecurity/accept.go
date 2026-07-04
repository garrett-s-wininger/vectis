package httpsecurity

import (
	"mime"
	"strconv"
	"strings"
)

const (
	MediaTypeEventStream     = "text/event-stream"
	MediaTypeJSON            = "application/json"
	MediaTypeOpenMetricsText = "application/openmetrics-text"
	MediaTypePlainText       = "text/plain"
)

// AcceptsAny reports whether an Accept header permits at least one of the
// supplied concrete media types. Empty Accept means the client accepts any type.
func AcceptsAny(header string, allowed ...string) bool {
	if strings.TrimSpace(header) == "" {
		return true
	}

	allowed = normalizeMediaTypes(allowed)
	if len(allowed) == 0 {
		return false
	}

	accepted := parseAcceptHeader(header)
	for _, mediaType := range allowed {
		if bestAcceptQ(mediaType, accepted) > 0 {
			return true
		}
	}

	return false
}

func normalizeMediaTypes(mediaTypes []string) []string {
	out := make([]string, 0, len(mediaTypes))
	for _, mediaType := range mediaTypes {
		mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(mediaType))
		if err != nil || mediaType == "" {
			continue
		}

		out = append(out, strings.ToLower(mediaType))
	}

	return out
}

type acceptRange struct {
	mediaRange string
	q          float64
}

func parseAcceptHeader(header string) []acceptRange {
	var out []acceptRange
	for _, part := range splitAcceptHeader(header) {
		mediaRange, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}

		q, ok := acceptQ(params["q"])
		if !ok {
			continue
		}

		out = append(out, acceptRange{mediaRange: mediaRange, q: q})
	}

	return out
}

func acceptQ(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 1, true
	}

	q, err := strconv.ParseFloat(raw, 64)
	return q, err == nil && q >= 0 && q <= 1
}

func bestAcceptQ(mediaType string, accepted []acceptRange) float64 {
	bestSpecificity := -1
	bestQ := float64(0)
	for _, acceptedRange := range accepted {
		specificity := mediaRangeSpecificity(acceptedRange.mediaRange, mediaType)
		if specificity < 0 {
			continue
		}
		if specificity < bestSpecificity {
			continue
		}

		if specificity > bestSpecificity || acceptedRange.q > bestQ {
			bestSpecificity = specificity
			bestQ = acceptedRange.q
		}
	}

	return bestQ
}

func mediaRangeSpecificity(mediaRange, mediaType string) int {
	mediaRange = strings.ToLower(strings.TrimSpace(mediaRange))
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	rangeType, rangeSubType, ok := strings.Cut(mediaRange, "/")
	if !ok || rangeType == "" || rangeSubType == "" {
		return -1
	}

	if mediaRange == mediaType {
		return 2
	}

	allowedType, _, ok := strings.Cut(mediaType, "/")
	if ok && rangeSubType == "*" && rangeType == allowedType {
		return 1
	}

	if mediaRange == "*/*" {
		return 0
	}

	return -1
}

func splitAcceptHeader(header string) []string {
	var parts []string
	start := 0
	inQuote := false
	escaped := false

	for i, r := range header {
		switch {
		case escaped:
			escaped = false
		case r == '\\' && inQuote:
			escaped = true
		case r == '"':
			inQuote = !inQuote
		case r == ',' && !inQuote:
			parts = append(parts, header[start:i])
			start = i + 1
		}
	}

	parts = append(parts, header[start:])
	return parts
}
