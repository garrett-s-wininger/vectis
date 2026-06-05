package observability

import "testing"

func TestRegisterSQLDBPoolMetrics_nilDB(t *testing.T) {
	t.Parallel()

	err := RegisterSQLDBPoolMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil *sql.DB")
	}
}

func TestRegisterTaskDispatchBacklogMetrics_nilDB(t *testing.T) {
	t.Parallel()

	err := RegisterTaskDispatchBacklogMetrics(nil)
	if err == nil {
		t.Fatal("expected error for nil *sql.DB")
	}
}
