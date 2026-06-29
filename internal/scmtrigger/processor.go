package scmtrigger

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/dispatchmeta"
	"vectis/internal/interfaces"
	jobdef "vectis/internal/job"
	"vectis/internal/trigger"
	"vectis/sdk/scm"
)

type EventRepository interface {
	RecordEvent(ctx context.Context, event dal.SCMTriggerEvent) (dal.SCMTriggerEventRecord, bool, error)
}

type Processor struct {
	Events         EventRepository
	Jobs           dal.JobsRepository
	Runs           dal.RunsRepository
	Dispatch       dal.DispatchEventsRepository
	Invocations    dal.TriggerInvocationsRepository
	QueueClient    interfaces.QueueService
	Ingress        cell.ExecutionIngress
	Logger         interfaces.Logger
	Clock          interfaces.Clock
	ActionResolver actionregistry.Resolver
	Source         string
	SourceInstance string
}

func (p Processor) HandleEvent(ctx context.Context, spec dal.SCMPollTriggerSpec, event scm.Event) error {
	rec, _, err := p.recordEvent(ctx, spec.TriggerID, event)
	if err != nil {
		return err
	}

	if p.Jobs == nil || p.Runs == nil {
		return nil
	}

	if rec.RunID != nil && strings.TrimSpace(*rec.RunID) != "" {
		p.logger().Debug("scm-trigger: scm event %s already has run %s", rec.EventKey, *rec.RunID)
		return nil
	}

	return p.dispatchEventRun(ctx, spec, event)
}

func (p Processor) recordEvent(ctx context.Context, triggerID int64, event scm.Event) (dal.SCMTriggerEventRecord, bool, error) {
	if p.Events == nil {
		return dal.SCMTriggerEventRecord{}, false, fmt.Errorf("scm trigger event repository is not set")
	}

	key := strings.TrimSpace(event.Key)
	if key == "" {
		return dal.SCMTriggerEventRecord{}, false, fmt.Errorf("scm provider returned event without key")
	}

	rec, created, err := p.Events.RecordEvent(ctx, dal.SCMTriggerEvent{
		TriggerID:   triggerID,
		EventKey:    key,
		PayloadJSON: event.PayloadJSON,
	})

	if err != nil {
		return dal.SCMTriggerEventRecord{}, false, fmt.Errorf("record scm trigger event %q: %w", key, err)
	}

	if created {
		p.logger().Info("scm-trigger: recorded new scm event %s", key)
	} else {
		p.logger().Debug("scm-trigger: scm event %s was already recorded", key)
	}

	return rec, created, nil
}

func (p Processor) dispatchEventRun(ctx context.Context, spec dal.SCMPollTriggerSpec, event scm.Event) error {
	job, definitionVersion, definitionHash, err := p.getJobDefinitionWithVersion(ctx, spec.JobID)
	if err != nil {
		return err
	}

	job.Id = &spec.JobID
	invocationID, err := p.recordTriggerInvocation(ctx, spec, event)
	if err != nil {
		return err
	}

	audit := dal.RunAuditMetadata{
		TriggerInvocationID:   invocationID,
		StartDeadlineUnixNano: dispatchmeta.DeadlineUnixNano(p.clock().Now(), config.DispatchStartTTL()),
	}

	runID, _, created, err := p.Runs.CreateSCMEventRun(ctx, spec.TriggerID, strings.TrimSpace(event.Key), spec.JobID, definitionVersion, audit)
	if err != nil {
		return fmt.Errorf("create scm event run: %w", err)
	}

	if !created {
		p.logger().Info("scm-trigger: reusing existing run %s for scm event %s", runID, strings.TrimSpace(event.Key))
	}

	return p.dispatcher().DispatchRun(ctx, job, runID, definitionHash)
}

func (p Processor) getJobDefinitionWithVersion(ctx context.Context, jobID string) (*api.Job, int, string, error) {
	definitionJSON, version, err := p.Jobs.GetLatestDefinition(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			return nil, 0, "", fmt.Errorf("job not found: %s", jobID)
		}

		return nil, 0, "", fmt.Errorf("database error: %w", err)
	}

	var job api.Job
	if err := jobdef.DecodeDefinitionJSON([]byte(definitionJSON), &job); err != nil {
		return nil, 0, "", fmt.Errorf("failed to parse job definition: %w", err)
	}

	return &job, version, dal.DefinitionHash(definitionJSON), nil
}

func (p Processor) recordTriggerInvocation(ctx context.Context, spec dal.SCMPollTriggerSpec, event scm.Event) (string, error) {
	if p.Invocations == nil {
		return "", nil
	}

	triggerID := spec.TriggerID
	payload := map[string]string{
		"job_id":     spec.JobID,
		"trigger_id": strconv.FormatInt(spec.TriggerID, 10),
		"provider":   spec.Provider,
		"base_url":   spec.BaseURL,
		"project":    spec.Project,
		"branch":     spec.Branch,
		"query":      spec.Query,
		"event_key":  strings.TrimSpace(event.Key),
		"triggered":  p.clock().Now().UTC().Format(time.RFC3339Nano),
	}

	if strings.TrimSpace(event.PayloadJSON) != "" {
		payload["event_payload_hash"] = dal.PayloadHash(event.PayloadJSON)
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal scm trigger payload: %w", err)
	}

	rec, err := p.Invocations.Record(ctx, dal.TriggerInvocation{
		TriggerID:          &triggerID,
		JobID:              spec.JobID,
		TriggerType:        dal.TriggerTypeSCMPoll,
		SourceInstance:     p.InstanceID(),
		TriggerPayloadHash: dal.PayloadHash(string(payloadJSON)),
		RequestedCells:     []string{config.CellID()},
	})

	if err != nil {
		return "", fmt.Errorf("record scm trigger invocation: %w", err)
	}

	return rec.InvocationID, nil
}

func (p Processor) dispatcher() trigger.Dispatcher {
	source := strings.TrimSpace(p.Source)
	if source == "" {
		source = "scm_trigger"
	}

	return trigger.Dispatcher{
		Runs:           p.Runs,
		Dispatch:       p.Dispatch,
		QueueClient:    p.QueueClient,
		Ingress:        p.Ingress,
		Logger:         p.logger(),
		Clock:          p.clock(),
		ActionResolver: p.ActionResolver,
		Source:         source,
		SourceInstance: p.InstanceID(),
	}
}

func (p Processor) InstanceID() string {
	if id := strings.TrimSpace(p.SourceInstance); id != "" {
		return id
	}

	return "scm-trigger"
}

func (p Processor) logger() interfaces.Logger {
	if p.Logger != nil {
		return p.Logger
	}

	return interfaces.NewLogger("scm-trigger")
}

func (p Processor) clock() interfaces.Clock {
	if p.Clock != nil {
		return p.Clock
	}

	return interfaces.SystemClock{}
}
