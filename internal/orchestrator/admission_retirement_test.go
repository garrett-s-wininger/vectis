package orchestrator_test

import (
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/orchestrator"
)

func TestServiceDiscardAdmissionFencesGenerations(t *testing.T) {
	svc := orchestrator.New(1)
	t.Cleanup(svc.Close)

	ctx := t.Context()
	client, cleanup := newOrchestratorTestClient(t, svc)
	t.Cleanup(cleanup)

	loadAndClaim := func() (orchestrator.LoadResult, dal.ExecutionClaimResult) {
		t.Helper()
		loaded, err := svc.LoadRun(ctx, orchestrator.RunSpec{RunID: "discard", Tasks: []orchestrator.TaskSpec{{TaskKey: "child", ParentTaskKey: dal.RootTaskKey}}})
		if err != nil {
			t.Fatal(err)
		}

		claim, err := svc.ClaimExecution(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", time.Now().Add(time.Hour))
		if err != nil || !claim.Claimed {
			t.Fatalf("claim: %+v %v", claim, err)
		}

		return loaded, claim
	}

	loaded, first := loadAndClaim()
	if err := svc.DiscardRunAdmission(ctx, loaded.RunID, "wrong-token"); err != nil {
		t.Fatal(err)
	}

	if runs, _, _, _ := svc.RetentionStats(); runs != 1 {
		t.Fatal("wrong token removed admission")
	}

	for range 2 {
		if _, err := client.DiscardRunAdmission(ctx, &api.DiscardRunAdmissionRequest{RunId: testString(loaded.RunID), ClaimToken: testString(first.ClaimToken)}); err != nil {
			t.Fatal(err)
		}
	}

	loaded, second := loadAndClaim()
	if err := svc.DiscardRunAdmission(ctx, loaded.RunID, first.ClaimToken); err != nil {
		t.Fatal(err)
	}

	if runs, _, _, _ := svc.RetentionStats(); runs != 1 {
		t.Fatal("old token removed a newer admission")
	}

	result, err := svc.CompleteExecutionByClaim(ctx, loaded.RunID, loaded.Root.ExecutionID, "worker", second.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil || len(result.Children) != 1 {
		t.Fatalf("activation: %+v %v", result, err)
	}

	if err := svc.DiscardRunAdmission(ctx, loaded.RunID, second.ClaimToken); err != nil {
		t.Fatal(err)
	}

	if runs, _, _, _ := svc.RetentionStats(); runs != 1 {
		t.Fatal("discard removed a graph after execution progressed")
	}
}
