package workercore

import (
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/workloadidentity"
)

func ValidateTaskSessionIdentity(job *api.Job, sessionID string, identity *workloadidentity.Identity) error {
	if identity == nil {
		return nil
	}

	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("worker core task session identity requires a session id")
	}

	if strings.TrimSpace(identity.SPIFFEID) == "" {
		return fmt.Errorf("worker core task session identity requires a SPIFFE ID")
	}

	if job == nil {
		return fmt.Errorf("worker core task session identity requires a job")
	}

	if err := requireIdentityMatch("job_id", identity.JobID, job.GetId()); err != nil {
		return err
	}

	if err := requireIdentityMatch("run_id", identity.RunID, job.GetRunId()); err != nil {
		return err
	}

	if err := requireIdentityMatch("execution_id", identity.ExecutionID, sessionID); err != nil {
		return err
	}

	return nil
}

func requireIdentityMatch(field, got, want string) error {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	if got == "" {
		return fmt.Errorf("worker core task session identity requires %s", field)
	}

	if want == "" {
		return fmt.Errorf("worker core task session identity cannot verify %s", field)
	}

	if got != want {
		return fmt.Errorf("worker core task session identity %s %q does not match %q", field, got, want)
	}

	return nil
}
