//go:build integration

package postgres_test

import (
	"testing"

	"vectis/internal/testutil/pgtest"
	"vectis/internal/testutil/runfixture"
)

func TestPostgres_TerminalSnapshotFencesRetryGeneration(t *testing.T) {
	runfixture.AssertTerminalSnapshotRetryFencing(t, pgtest.NewTestDB(t))
}
