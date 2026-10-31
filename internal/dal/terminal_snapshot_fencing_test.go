package dal_test

import (
	"testing"

	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/runfixture"
)

func TestRunsRepository_TerminalSnapshotFencesRetryGeneration(t *testing.T) {
	runfixture.AssertTerminalSnapshotRetryFencing(t, dbtest.NewTestDB(t))
}
