package workertest

import (
	"context"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/job"
	"vectis/internal/secrets"
	"vectis/internal/spire"
	"vectis/internal/workloadidentity"
)

type SecretResolverFactory func(*workloadidentity.Identity) (secrets.Resolver, func(), error)

type Runner struct {
	Logger         interfaces.Logger
	WorkerID       string
	Queue          interfaces.QueueClient
	LogClient      interfaces.LogClient
	Executor       *job.Executor
	Store          dal.RunsRepository
	DequeueWait    time.Duration
	ExecutionTTL   time.Duration
	ActionResolver actionregistry.Resolver

	TrustDomain               string
	SPIFFEPathTemplate        string
	ParentSPIFFEID            string
	Selectors                 []spire.Selector
	Registrar                 spire.Registrar
	SVIDSource                spire.X509SVIDSource
	RegistrationTTL           time.Duration
	RegistrationMinTTL        time.Duration
	RegistrationMaxTTL        time.Duration
	SecretResolverForWorkload SecretResolverFactory
}

type Result struct {
	Request      *api.JobRequest
	Envelope     *cell.ExecutionEnvelope
	Claim        dal.ExecutionClaimResult
	Workload     *workloadidentity.Identity
	Finalization dal.ExecutionFinalizationResult
}

func (r *Runner) RunOne(ctx context.Context) (Result, error) {
	if err := r.validate(); err != nil {
		return Result{}, err
	}

	req, err := r.dequeue(ctx)
	if err != nil {
		return Result{}, err
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil {
		return Result{}, fmt.Errorf("execution envelope: %w", err)
	}

	if !ok {
		return Result{}, fmt.Errorf("execution envelope is missing")
	}

	if err := r.Queue.Ack(ctx, req.GetJob().GetDeliveryId()); err != nil {
		return Result{}, fmt.Errorf("ack delivery: %w", err)
	}

	claim, err := r.Store.TryClaimExecution(ctx, env.ExecutionID, r.workerID(), time.Now().Add(r.executionTTL()))
	if err != nil {
		return Result{}, fmt.Errorf("claim execution: %w", err)
	}

	if !claim.Claimed {
		return Result{}, fmt.Errorf("execution %s was not claimed", env.ExecutionID)
	}

	workload, handle, err := r.prepareWorkload(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer r.releaseRegistration(ctx, handle)

	secretFiles, err := r.resolveSecrets(ctx, req.GetJob(), env, claim.ClaimToken, workload)
	if err != nil {
		return Result{}, fmt.Errorf("resolve secrets: %w", err)
	}

	if err := r.Store.MarkExecutionStarted(ctx, env.ExecutionID); err != nil {
		return Result{}, fmt.Errorf("mark execution started: %w", err)
	}

	execErr := r.Executor.ExecuteTaskWithOptions(ctx, req.GetJob(), env.TaskKey, r.LogClient, r.Logger, job.ExecuteOptions{
		WorkloadIdentity: workload,
		SecretFiles:      secretFiles,
		ActionResolver:   r.ActionResolver,
		ActionLocks:      env.ActionLocks,
	})
	if execErr != nil {
		finalization, _ := r.Store.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, r.workerID(), claim.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, execErr.Error())
		return Result{Request: req, Envelope: env, Claim: claim, Workload: workload, Finalization: finalization}, execErr
	}

	finalization, err := r.Store.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, r.workerID(), claim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		return Result{}, fmt.Errorf("complete execution: %w", err)
	}

	return Result{Request: req, Envelope: env, Claim: claim, Workload: workload, Finalization: finalization}, nil
}

func (r *Runner) validate() error {
	if r == nil {
		return fmt.Errorf("worker test runner is required")
	}
	if r.Queue == nil {
		return fmt.Errorf("queue client is required")
	}
	if r.LogClient == nil {
		return fmt.Errorf("log client is required")
	}
	if r.Store == nil {
		return fmt.Errorf("runs repository is required")
	}
	if r.Logger == nil {
		return fmt.Errorf("logger is required")
	}
	if r.Executor == nil {
		r.Executor = job.NewExecutor()
	}
	return nil
}

func (r *Runner) dequeue(ctx context.Context) (*api.JobRequest, error) {
	wait := r.DequeueWait
	if wait <= 0 {
		wait = 5 * time.Second
	}

	dequeueCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	req, err := r.Queue.Dequeue(dequeueCtx)
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	if req == nil || req.GetJob() == nil {
		return nil, fmt.Errorf("dequeue returned empty job")
	}
	return req, nil
}

func (r *Runner) prepareWorkload(ctx context.Context, env *cell.ExecutionEnvelope) (*workloadidentity.Identity, spire.RegistrationHandle, error) {
	if r.TrustDomain == "" && r.Registrar == nil && r.SVIDSource == nil {
		return nil, spire.RegistrationHandle{}, nil
	}
	if r.TrustDomain == "" {
		return nil, spire.RegistrationHandle{}, fmt.Errorf("trust domain is required for workload identity")
	}

	workload, err := workloadidentity.NewIdentity(r.TrustDomain, r.SPIFFEPathTemplate, executionFromEnvelope(env))
	if err != nil {
		return nil, spire.RegistrationHandle{}, fmt.Errorf("execution workload identity: %w", err)
	}

	handle, err := r.ensureRegistration(ctx, workload, env)
	if err != nil {
		return nil, handle, err
	}

	if r.SVIDSource == nil {
		return workload, handle, nil
	}

	svid, err := spire.FetchX509SVID(ctx, r.SVIDSource, workload.SPIFFEID)
	if err != nil {
		return nil, handle, fmt.Errorf("fetch execution X.509-SVID: %w", err)
	}

	return workload.WithX509SVID(workloadidentity.X509SVID{
		SPIFFEID:     svid.SPIFFEID,
		Certificates: svid.Certificates,
		PrivateKey:   svid.PrivateKey,
	}), handle, nil
}

func (r *Runner) ensureRegistration(ctx context.Context, workload *workloadidentity.Identity, env *cell.ExecutionEnvelope) (spire.RegistrationHandle, error) {
	if r.Registrar == nil {
		return spire.RegistrationHandle{}, nil
	}
	if r.ParentSPIFFEID == "" {
		return spire.RegistrationHandle{}, fmt.Errorf("parent SPIFFE ID is required for workload registration")
	}
	if len(r.Selectors) == 0 {
		return spire.RegistrationHandle{}, fmt.Errorf("at least one selector is required for workload registration")
	}

	now := time.Now().UTC()
	ttl := r.RegistrationTTL
	if ttl <= 0 {
		ttl = time.Minute
	}
	intent, err := spire.NewExecutionRegistrationIntent(workload.SPIFFEID, executionFromEnvelope(env), spire.ExecutionRegistrationOptions{
		ParentSPIFFEID: r.ParentSPIFFEID,
		Selectors:      r.Selectors,
		ExpiresAt:      now.Add(ttl),
		Now:            now,
		MinTTL:         r.RegistrationMinTTL,
		MaxTTL:         r.RegistrationMaxTTL,
	})
	if err != nil {
		return spire.RegistrationHandle{}, fmt.Errorf("execution SPIFFE registration intent: %w", err)
	}

	result, err := r.Registrar.EnsureRegistration(ctx, intent)
	if err != nil {
		return result.Handle, fmt.Errorf("ensure execution SPIFFE registration: %w", err)
	}
	return result.Handle, nil
}

func (r *Runner) releaseRegistration(ctx context.Context, handle spire.RegistrationHandle) {
	if r == nil || r.Registrar == nil || !handle.Managed {
		return
	}
	_ = r.Registrar.ReleaseRegistration(context.WithoutCancel(ctx), handle)
}

func (r *Runner) resolveSecrets(ctx context.Context, runJob *api.Job, env *cell.ExecutionEnvelope, claimToken string, workload *workloadidentity.Identity) ([]secrets.FileMaterial, error) {
	refs := secrets.ReferencesForTask(runJob, env.TaskKey)
	if len(refs) == 0 {
		return nil, nil
	}
	if r.SecretResolverForWorkload == nil {
		return nil, fmt.Errorf("job declares secrets but worker secrets resolver is not configured")
	}

	resolver, cleanup, err := r.SecretResolverForWorkload(workload)
	if err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, fmt.Errorf("job declares secrets but worker secrets resolver is not configured")
	}
	if cleanup != nil {
		defer cleanup()
	}

	req := secrets.ResolveRequest{
		RunID:               env.RunID,
		ExecutionID:         env.ExecutionID,
		ExecutionClaimToken: claimToken,
		Workload:            secrets.WorkloadIdentityFromInternal(workload),
		Secrets:             refs,
	}

	if err := secrets.ValidateResolveIdentityBinding(&req); err != nil {
		return nil, fmt.Errorf("validate secret resolve identity: %w", err)
	}

	bundle, err := resolver.Resolve(ctx, req)
	if err != nil {
		return nil, err
	}

	return bundle.Files, nil
}

func (r *Runner) workerID() string {
	if r.WorkerID != "" {
		return r.WorkerID
	}
	return "workertest"
}

func (r *Runner) executionTTL() time.Duration {
	if r.ExecutionTTL > 0 {
		return r.ExecutionTTL
	}
	return dal.DefaultLeaseTTL
}

func executionFromEnvelope(env *cell.ExecutionEnvelope) workloadidentity.Execution {
	return workloadidentity.Execution{
		CellID:            env.CellID,
		NamespacePath:     env.NamespacePath,
		JobID:             env.Job.GetId(),
		RunID:             env.RunID,
		RunIndex:          env.RunIndex,
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		Attempt:           env.Attempt,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}
}
