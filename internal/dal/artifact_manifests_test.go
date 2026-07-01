package dal_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestArtifactsRepository_RecordGetListAndUpdate(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	runID := createArtifactTestRun(t, ctx, repos, "job-artifacts")
	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	metadata := `{"kind":"coverage"}`
	rec, err := repos.Artifacts().Record(ctx, dal.ArtifactCreate{
		RunID:           runID,
		TaskID:          dispatch.TaskID,
		TaskAttemptID:   dispatch.TaskAttemptID,
		ExecutionID:     dispatch.ExecutionID,
		Name:            "coverage",
		Path:            "coverage/out.json",
		ContentType:     "application/json",
		BlobKey:         "sha256:aaaaaaaa",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "aaaaaaaa",
		SizeBytes:       12,
		ArtifactShardID: "artifact-1",
		MetadataJSON:    &metadata,
	})

	if err != nil {
		t.Fatalf("record artifact: %v", err)
	}

	if rec.ID == 0 || rec.RunID != runID || rec.Name != "coverage" || rec.Path != "coverage/out.json" {
		t.Fatalf("unexpected artifact record: %+v", rec)
	}

	if rec.CellID != dal.DefaultCellID {
		t.Fatalf("expected default cell, got %q", rec.CellID)
	}

	if rec.TaskID == nil || *rec.TaskID != dispatch.TaskID {
		t.Fatalf("task id not recorded: %+v", rec)
	}

	if rec.MetadataJSON == nil || *rec.MetadataJSON != metadata {
		t.Fatalf("metadata not recorded: %+v", rec.MetadataJSON)
	}

	got, err := repos.Artifacts().GetByRunAndName(ctx, runID, "coverage")
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}

	if got.ID != rec.ID || got.BlobKey != "sha256:aaaaaaaa" {
		t.Fatalf("get mismatch: got %+v want %+v", got, rec)
	}

	if _, err := repos.Artifacts().Record(ctx, dal.ArtifactCreate{
		RunID:           runID,
		Name:            "raw-log",
		BlobKey:         "sha256:bbbbbbbb",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "bbbbbbbb",
		SizeBytes:       4,
		ArtifactShardID: "artifact-1",
	}); err != nil {
		t.Fatalf("record second artifact: %v", err)
	}

	filtered, next, err := repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 10, dal.ArtifactListFilter{
		TaskID:        " " + dispatch.TaskID + " ",
		TaskAttemptID: dispatch.TaskAttemptID,
		ExecutionID:   dispatch.ExecutionID,
	})
	if err != nil {
		t.Fatalf("list filtered artifacts: %v", err)
	}

	if len(filtered) != 1 || filtered[0].Name != "coverage" || next != 0 {
		t.Fatalf("unexpected filtered artifacts: artifacts=%+v next=%d", filtered, next)
	}

	filtered, next, err = repos.Artifacts().ListByRunFiltered(ctx, runID, 0, 10, dal.ArtifactListFilter{
		ExecutionID: "missing-execution",
	})
	if err != nil {
		t.Fatalf("list artifacts with missing execution: %v", err)
	}

	if len(filtered) != 0 || next != 0 {
		t.Fatalf("expected no artifacts for missing execution, got artifacts=%+v next=%d", filtered, next)
	}

	firstPage, next, err := repos.Artifacts().ListByRun(ctx, runID, 0, 1)
	if err != nil {
		t.Fatalf("list first page: %v", err)
	}

	if len(firstPage) != 1 || firstPage[0].Name != "coverage" || next == 0 {
		t.Fatalf("unexpected first page: artifacts=%+v next=%d", firstPage, next)
	}

	secondPage, next, err := repos.Artifacts().ListByRun(ctx, runID, next, 1)
	if err != nil {
		t.Fatalf("list second page: %v", err)
	}

	if len(secondPage) != 1 || secondPage[0].Name != "raw-log" || next != 0 {
		t.Fatalf("unexpected second page: artifacts=%+v next=%d", secondPage, next)
	}

	updated, err := repos.Artifacts().Record(ctx, dal.ArtifactCreate{
		RunID:           runID,
		Name:            "coverage",
		BlobKey:         "sha256:cccccccc",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "cccccccc",
		SizeBytes:       24,
		ArtifactShardID: "artifact-2",
	})

	if err != nil {
		t.Fatalf("update artifact: %v", err)
	}

	if updated.ID != rec.ID {
		t.Fatalf("expected upsert to preserve artifact id: got %d want %d", updated.ID, rec.ID)
	}

	if updated.Path != "coverage" {
		t.Fatalf("expected empty path to default to name, got %q", updated.Path)
	}

	if updated.SizeBytes != 24 || updated.BlobKey != "sha256:cccccccc" || updated.ArtifactShardID != "artifact-2" {
		t.Fatalf("artifact update did not persist: %+v", updated)
	}

	if updated.MetadataJSON != nil {
		t.Fatalf("expected metadata to clear on update, got %q", *updated.MetadataJSON)
	}

	usage, err := repos.Artifacts().GetRunUsageExcludingName(ctx, runID, "")
	if err != nil {
		t.Fatalf("get artifact usage: %v", err)
	}

	if usage.Count != 2 || usage.SizeBytes != 28 {
		t.Fatalf("usage = %+v, want count=2 bytes=28", usage)
	}

	usage, err = repos.Artifacts().GetRunUsageExcludingName(ctx, runID, "coverage")
	if err != nil {
		t.Fatalf("get artifact usage excluding coverage: %v", err)
	}

	if usage.Count != 1 || usage.SizeBytes != 4 {
		t.Fatalf("usage excluding coverage = %+v, want count=1 bytes=4", usage)
	}
}

func TestArtifactsRepository_Validation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	artifacts := dal.NewSQLRepositories(db).Artifacts()
	ctx := context.Background()

	valid := dal.ArtifactCreate{
		RunID:           "run-a",
		Name:            "artifact-a",
		BlobKey:         "sha256:aaaaaaaa",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "aaaaaaaa",
		SizeBytes:       1,
		ArtifactShardID: "artifact-1",
	}

	tests := []struct {
		name   string
		mutate func(*dal.ArtifactCreate)
	}{
		{name: "missing run", mutate: func(c *dal.ArtifactCreate) { c.RunID = "" }},
		{name: "missing name", mutate: func(c *dal.ArtifactCreate) { c.Name = "" }},
		{name: "missing blob key", mutate: func(c *dal.ArtifactCreate) { c.BlobKey = "" }},
		{name: "missing algorithm", mutate: func(c *dal.ArtifactCreate) { c.BlobAlgorithm = "" }},
		{name: "missing digest", mutate: func(c *dal.ArtifactCreate) { c.BlobDigest = "" }},
		{name: "negative size", mutate: func(c *dal.ArtifactCreate) { c.SizeBytes = -1 }},
		{name: "missing shard", mutate: func(c *dal.ArtifactCreate) { c.ArtifactShardID = "" }},
		{name: "invalid metadata", mutate: func(c *dal.ArtifactCreate) {
			bad := "{nope"
			c.MetadataJSON = &bad
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			create := valid
			tt.mutate(&create)
			if _, err := artifacts.Record(ctx, create); !dal.IsConflict(err) {
				t.Fatalf("expected conflict validation error, got %v", err)
			}
		})
	}

	if _, err := artifacts.GetByRunAndName(ctx, "run-a", "missing"); !dal.IsNotFound(err) {
		t.Fatalf("expected not found for missing artifact, got %v", err)
	}
}

func TestArtifactsMigration_DeclaresRunDeleteCascade(t *testing.T) {
	db := dbtest.NewTestDB(t)

	rows, err := db.Query("PRAGMA foreign_key_list(run_artifacts)")
	if err != nil {
		t.Fatalf("query run_artifacts foreign keys: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, seq int
		var table, from, to, onUpdate, onDelete, match string
		if err := rows.Scan(&id, &seq, &table, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			t.Fatalf("scan foreign key: %v", err)
		}

		if table == "job_runs" && from == "run_id" && to == "run_id" && onDelete == "CASCADE" {
			return
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate foreign keys: %v", err)
	}

	t.Fatal("run_artifacts.run_id should reference job_runs(run_id) ON DELETE CASCADE")
}

func createArtifactTestRun(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, jobID string) string {
	t.Helper()

	def := `{"id":"` + jobID + `","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	return runID
}
