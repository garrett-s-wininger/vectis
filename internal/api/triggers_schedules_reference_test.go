package api

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"vectis/internal/dal"
)

func TestTriggersAndSchedulesReferenceMentionsRoutesAndConstants(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "website", "docs", "using", "triggers-and-schedules-reference.md"))
	if err != nil {
		t.Fatalf("read triggers and schedules reference: %v", err)
	}
	doc := string(raw)

	tokens := []string{
		dal.TriggerTypeManual,
		dal.TriggerTypeCron,
		dal.TriggerTypeReplay,
		dal.TriggerTypeWebhook,
		"job_triggers",
		"cron_trigger_specs",
		"cron_schedule_fires",
		"trigger_invocations",
		"trigger_invocation_id",
		"trigger_id",
		"trigger_type",
		"trigger_payload_hash",
		"requested_cells",
		"replay_of_run_id",
		"definition_version",
		"definition_hash",
		"execution_payload_hash",
		"Idempotency-Key",
		"cron.claim_ttl",
		"VECTIS_CRON_CLAIM_TTL",
		"VECTIS_CRON_INSTANCE_ID",
		"cron.schedules",
		"source_run_not_replayable",
	}

	statusType := reflect.TypeOf(cronStatusResponse{})
	for i := 0; i < statusType.NumField(); i++ {
		name, _, _ := strings.Cut(statusType.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			tokens = append(tokens, name)
		}
	}

	server := &APIServer{}
	for _, route := range server.routeSpecs(false) {
		if strings.Contains(route.Pattern, "/jobs/run") ||
			strings.Contains(route.Pattern, "/jobs/trigger") ||
			strings.Contains(route.Pattern, "/runs/{id}/replay") ||
			strings.Contains(route.Pattern, "/cron/status") {
			tokens = append(tokens, route.Pattern)
		}
	}

	var missing []string
	for _, token := range tokens {
		if !triggersReferenceContains(doc, token) {
			missing = append(missing, token)
		}
	}

	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("triggers and schedules reference is missing tokens: %s", strings.Join(missing, ", "))
	}
}

func triggersReferenceContains(doc, token string) bool {
	for _, needle := range []string{
		"`" + token + "`",
		`"` + token + `"`,
		" " + token + " ",
	} {
		if strings.Contains(doc, needle) {
			return true
		}
	}

	return false
}
