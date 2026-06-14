package queue

import (
	"fmt"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"

	"google.golang.org/protobuf/proto"
)

func queueTestJobRequest(t testing.TB, job *api.Job) *api.JobRequest {
	t.Helper()
	return queueTestJobRequestWithDeadline(t, job, 0)
}

func queueTestJobRequestWithDeadline(t testing.TB, job *api.Job, deadlineUnixNano int64) *api.JobRequest {
	t.Helper()
	return queueTestRequestWithDeadline(t, &api.JobRequest{Job: job}, deadlineUnixNano)
}

func queueTestRequest(t testing.TB, req *api.JobRequest) *api.JobRequest {
	t.Helper()
	return queueTestRequestWithDeadline(t, req, 0)
}

func queueTestRequestWithDeadline(t testing.TB, req *api.JobRequest, deadlineUnixNano int64) *api.JobRequest {
	t.Helper()

	if req == nil {
		t.Fatal("job request is required")
	}

	cloned, ok := proto.Clone(req).(*api.JobRequest)
	if !ok {
		t.Fatal("clone job request")
	}

	if cloned.Job == nil {
		cloned.Job = &api.Job{}
	}

	jobID := strings.TrimSpace(cloned.Job.GetId())
	if jobID == "" {
		jobID = "queue-test-job"
		cloned.Job.Id = &jobID
	}

	runID := strings.TrimSpace(cloned.Job.GetRunId())
	if runID == "" {
		runID = jobID + ":run"
		cloned.Job.RunId = &runID
	}

	if cloned.Job.GetRoot() == nil {
		cloned.Job.Root = queueTestNode("root", "builtins/shell")
	}

	taskKey := strings.TrimSpace(cloned.GetMetadata()[cell.ExecutionTaskKeyMetadataKey])
	if taskKey == "" {
		taskKey = dal.RootTaskKey
	}

	dispatch := dal.ExecutionDispatchRecord{
		RunID:                 runID,
		JobID:                 jobID,
		TaskID:                runID + ":" + taskKey,
		TaskKey:               taskKey,
		TaskName:              taskKey,
		TaskAttemptID:         fmt.Sprintf("%s:%s:attempt:1", runID, taskKey),
		SegmentID:             runID + ":" + taskKey + ":segment",
		ExecutionID:           runID + ":" + taskKey + ":attempt:1:execution",
		CellID:                dal.DefaultCellID,
		Attempt:               1,
		DefinitionVersion:     1,
		DefinitionHash:        "sha256:queue-test",
		StartDeadlineUnixNano: deadlineUnixNano,
	}

	if _, err := cell.AttachExecutionEnvelope(cloned, dispatch, time.Now().UnixNano()); err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	return cloned
}
