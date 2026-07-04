package dal

import (
	"strings"
	"testing"
)

func TestSourceCountDeclaredIDsCTEUsesPlaceholders(t *testing.T) {
	cte, args := sourceCountDeclaredIDsCTE([]string{
		" declared-ready ",
		"declared-ready",
		"stale-failed' OR 1=1 --",
	})

	if strings.Contains(cte, "declared-ready") || strings.Contains(cte, "1=1") {
		t.Fatalf("expected declaration values to stay out of SQL, got %q", cte)
	}

	if got := strings.Count(cte, "?"); got != 2 {
		t.Fatalf("placeholder count=%d, want 2 in %q", got, cte)
	}

	if !strings.Contains(cte, "VALUES") || strings.Contains(cte, "UNION ALL") {
		t.Fatalf("expected declaration CTE to use VALUES without UNION ALL, got %q", cte)
	}

	if len(args) != 2 || args[0] != "declared-ready" || args[1] != "stale-failed' OR 1=1 --" {
		t.Fatalf("unexpected args: %#v", args)
	}
}
