package api

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestStreamingReferenceMentionsSSEContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "using", "streaming-reference.md"))
	if err != nil {
		t.Fatalf("read streaming reference: %v", err)
	}
	docText := string(raw)

	var missing []string
	for _, token := range streamingReferenceTokens() {
		if !streamingReferenceContainsToken(docText, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("streaming reference is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func streamingReferenceTokens() []string {
	tokens := []string{
		"text/event-stream",
		"run:read",
		"Accept",
		"Last-Event-ID",
		"since_sequence",
		"tail",
		"replay_limit",
		"after_index",
		"timestamp",
		"stream",
		"sequence",
		"data",
		"completed",
		"STREAM_STDOUT",
		"STREAM_STDERR",
		"STREAM_CONTROL",
		"RUN_OUTCOME_UNSPECIFIED",
		"RUN_OUTCOME_SUCCESS",
		"RUN_OUTCOME_FAILURE",
		"RUN_OUTCOME_UNKNOWN",
		"start",
		"completed",
		"replay_truncated",
		"synthetic",
		"job_not_found",
		"run_not_found",
		"log_service_unavailable",
		"log_service_error",
		"invalid_since_sequence",
		"invalid_tail",
		"invalid_replay_limit",
		"streaming_unsupported",
		strconv.Itoa(defaultLogReplayLimit),
		strconv.Itoa(maxLogReplayLimit),
		strconv.Itoa(maxLogTail),
	}

	runEventType := reflect.TypeOf(RunEvent{})
	for i := 0; i < runEventType.NumField(); i++ {
		tag := runEventType.Field(i).Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name != "" && name != "-" {
			tokens = append(tokens, name)
		}
	}

	server := &APIServer{}
	for _, route := range server.routeSpecs(false) {
		if strings.Contains(route.Pattern, "/sse/") || strings.HasSuffix(route.Pattern, "/logs") {
			tokens = append(tokens, route.Pattern)
		}
	}

	return tokens
}

func streamingReferenceContainsToken(docText, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
		" " + token + " ",
	} {
		if strings.Contains(docText, needle) {
			return true
		}
	}

	return false
}
