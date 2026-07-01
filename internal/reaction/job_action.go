package reaction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"vectis/internal/dal"
)

const defaultJobTriggerCycleDepth = 16

type JobTriggerStore interface {
	GetDefinition(ctx context.Context, jobID string) (definitionJSON string, version int, err error)
	Record(ctx context.Context, invocation dal.TriggerInvocation) (dal.TriggerInvocationRecord, error)
	CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit dal.RunAuditMetadata) ([]dal.CreatedRun, error)
	ListRunsByTriggerInvocation(ctx context.Context, triggerInvocationID string) ([]dal.CreatedRun, error)
	ListJobTriggerEdges(ctx context.Context) ([]dal.ReactionJobTriggerEdge, error)
}

type TriggerJobDispatcher interface {
	DispatchTriggeredRun(ctx context.Context, run dal.CreatedRun) error
}

type DALJobTriggerStore struct {
	Jobs      dal.JobsRepository
	Runs      dal.RunsRepository
	Triggers  dal.TriggerInvocationsRepository
	Reactions interface {
		ListJobTriggerEdges(ctx context.Context) ([]dal.ReactionJobTriggerEdge, error)
	}
}

func (s *DALJobTriggerStore) GetDefinition(ctx context.Context, jobID string) (string, int, error) {
	if s == nil || s.Jobs == nil {
		return "", 0, fmt.Errorf("trigger job reaction action requires jobs repository")
	}

	return s.Jobs.GetLatestDefinition(ctx, jobID)
}

func (s *DALJobTriggerStore) Record(ctx context.Context, invocation dal.TriggerInvocation) (dal.TriggerInvocationRecord, error) {
	if s == nil || s.Triggers == nil {
		return dal.TriggerInvocationRecord{}, fmt.Errorf("trigger job reaction action requires trigger invocation repository")
	}

	return s.Triggers.Record(ctx, invocation)
}

func (s *DALJobTriggerStore) CreateRunsInCellsWithAudit(ctx context.Context, jobID string, runIndex *int, definitionVersion int, targetCellIDs []string, audit dal.RunAuditMetadata) ([]dal.CreatedRun, error) {
	if s == nil || s.Runs == nil {
		return nil, fmt.Errorf("trigger job reaction action requires runs repository")
	}

	return s.Runs.CreateRunsInCellsWithAudit(ctx, jobID, runIndex, definitionVersion, targetCellIDs, audit)
}

func (s *DALJobTriggerStore) ListRunsByTriggerInvocation(ctx context.Context, triggerInvocationID string) ([]dal.CreatedRun, error) {
	if s == nil || s.Runs == nil {
		return nil, fmt.Errorf("trigger job reaction action requires runs repository")
	}

	return s.Runs.ListRunsByTriggerInvocation(ctx, triggerInvocationID)
}

func (s *DALJobTriggerStore) ListJobTriggerEdges(ctx context.Context) ([]dal.ReactionJobTriggerEdge, error) {
	if s == nil || s.Reactions == nil {
		return nil, fmt.Errorf("trigger job reaction action requires reactions repository")
	}

	return s.Reactions.ListJobTriggerEdges(ctx)
}

type TriggerJobAction struct {
	Store      JobTriggerStore
	Dispatcher TriggerJobDispatcher
}

type TriggerJobActionRequest struct {
	Event      dal.ReactionEventRecord
	Invocation dal.ReactionInvocationRecord
	Payload    []byte
}

type TriggerJobResult struct {
	TriggerInvocation dal.TriggerInvocationRecord
	Runs              []dal.CreatedRun
	Reused            bool
	Dispatched        int
}

type triggerJobConfig struct {
	JobID         string   `json:"job_id"`
	TargetCellID  string   `json:"target_cell_id,omitempty"`
	TargetCellIDs []string `json:"target_cell_ids,omitempty"`
	MaxDepth      int      `json:"max_depth,omitempty"`
}

type triggerJobPayload struct {
	ReactionEventID        string   `json:"reaction_event_id"`
	ReactionInvocationID   string   `json:"reaction_invocation_id"`
	ReactionEventType      string   `json:"reaction_event_type"`
	SourceJobID            string   `json:"source_job_id,omitempty"`
	SourceRunID            string   `json:"source_run_id,omitempty"`
	TargetJobID            string   `json:"target_job_id"`
	RequestedTargetCellIDs []string `json:"requested_target_cell_ids,omitempty"`
}

func (a *TriggerJobAction) Type() string {
	return dal.ReactionActionTriggerJob
}

func (a *TriggerJobAction) ExecuteInvocation(ctx context.Context, req ActionRequest) error {
	_, err := a.Execute(ctx, TriggerJobActionRequest(req))

	return err
}

func (a *TriggerJobAction) Execute(ctx context.Context, req TriggerJobActionRequest) (TriggerJobResult, error) {
	if a == nil || a.Store == nil {
		return TriggerJobResult{}, fmt.Errorf("trigger job reaction action requires a store")
	}

	invocationID := strings.TrimSpace(req.Invocation.InvocationID)
	if invocationID == "" {
		return TriggerJobResult{}, fmt.Errorf("trigger job reaction action requires invocation_id")
	}

	eventID := strings.TrimSpace(req.Event.EventID)
	if eventID == "" {
		eventID = strings.TrimSpace(req.Invocation.EventID)
	}

	if eventID == "" {
		return TriggerJobResult{}, fmt.Errorf("trigger job reaction action requires event_id")
	}

	config, err := parseTriggerJobConfig(req.Invocation.TargetConfigJSON)
	if err != nil {
		return TriggerJobResult{}, err
	}

	targetJobID := strings.TrimSpace(config.JobID)
	if targetJobID == "" {
		return TriggerJobResult{}, fmt.Errorf("%w: trigger job target config requires job_id", dal.ErrConflict)
	}

	targetCellIDs := normalizeTriggerJobTargetCells(config)
	_, version, err := a.Store.GetDefinition(ctx, targetJobID)
	if err != nil {
		return TriggerJobResult{}, err
	}

	sourceJobID := reactionEventJobID(req.Event)
	sourceRunID := reactionEventRunID(req.Event)
	edges, err := a.Store.ListJobTriggerEdges(ctx)
	if err != nil {
		return TriggerJobResult{}, err
	}

	if err := rejectJobTriggerCycle(sourceJobID, targetJobID, config.MaxDepth, edges); err != nil {
		return TriggerJobResult{}, err
	}

	payload, err := json.Marshal(triggerJobPayload{
		ReactionEventID:        eventID,
		ReactionInvocationID:   invocationID,
		ReactionEventType:      strings.TrimSpace(req.Event.EventType),
		SourceJobID:            sourceJobID,
		SourceRunID:            sourceRunID,
		TargetJobID:            targetJobID,
		RequestedTargetCellIDs: targetCellIDs,
	})

	if err != nil {
		return TriggerJobResult{}, err
	}

	trigger, err := a.Store.Record(ctx, dal.TriggerInvocation{
		InvocationID:       invocationID,
		JobID:              targetJobID,
		TriggerType:        dal.TriggerTypeReaction,
		TriggerPayloadHash: dal.PayloadHash(string(payload)),
		RequestedCells:     targetCellIDs,
	})

	if err != nil {
		return TriggerJobResult{}, err
	}

	existing, err := a.Store.ListRunsByTriggerInvocation(ctx, trigger.InvocationID)
	if err != nil {
		return TriggerJobResult{}, err
	}

	if len(existing) > 0 {
		if !createdRunsMatchTargetCells(existing, targetCellIDs) {
			return TriggerJobResult{}, fmt.Errorf("%w: trigger invocation %s already created runs for different target cells", dal.ErrConflict, trigger.InvocationID)
		}

		dispatched, err := a.dispatchRuns(ctx, existing)
		if err != nil {
			return TriggerJobResult{}, err
		}

		return TriggerJobResult{TriggerInvocation: trigger, Runs: existing, Reused: true, Dispatched: dispatched}, nil
	}

	created, err := a.Store.CreateRunsInCellsWithAudit(ctx, targetJobID, nil, version, targetCellIDs, dal.RunAuditMetadata{
		TriggerInvocationID: trigger.InvocationID,
	})

	if err != nil {
		return TriggerJobResult{}, err
	}

	dispatched, err := a.dispatchRuns(ctx, created)
	if err != nil {
		return TriggerJobResult{}, err
	}

	return TriggerJobResult{TriggerInvocation: trigger, Runs: created, Dispatched: dispatched}, nil
}

func (a *TriggerJobAction) dispatchRuns(ctx context.Context, runs []dal.CreatedRun) (int, error) {
	if a.Dispatcher == nil {
		return 0, nil
	}

	dispatched := 0
	for _, run := range runs {
		if err := a.Dispatcher.DispatchTriggeredRun(ctx, run); err != nil {
			return dispatched, fmt.Errorf("dispatch triggered run %s: %w", run.RunID, err)
		}

		dispatched++
	}

	return dispatched, nil
}

func parseTriggerJobConfig(configJSON []byte) (triggerJobConfig, error) {
	configJSON = bytes.TrimSpace(configJSON)
	if len(configJSON) == 0 {
		configJSON = []byte("{}")
	}

	var config triggerJobConfig
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return triggerJobConfig{}, fmt.Errorf("trigger job reaction action config: %w", err)
	}

	config.JobID = strings.TrimSpace(config.JobID)
	config.TargetCellID = strings.TrimSpace(config.TargetCellID)
	cells := make([]string, 0, len(config.TargetCellIDs))
	for _, cellID := range config.TargetCellIDs {
		cellID = strings.TrimSpace(cellID)
		if cellID == "" || slices.Contains(cells, cellID) {
			continue
		}

		cells = append(cells, cellID)
	}

	config.TargetCellIDs = cells
	return config, nil
}

func normalizeTriggerJobTargetCells(config triggerJobConfig) []string {
	out := make([]string, 0, len(config.TargetCellIDs)+1)
	if config.TargetCellID != "" {
		out = append(out, config.TargetCellID)
	}

	for _, cellID := range config.TargetCellIDs {
		if cellID == "" || slices.Contains(out, cellID) {
			continue
		}

		out = append(out, cellID)
	}

	return out
}

func reactionEventJobID(event dal.ReactionEventRecord) string {
	if jobID := strings.TrimSpace(event.JobID); jobID != "" {
		return jobID
	}

	return reactionPayloadString(event.PayloadJSON, "job_id")
}

func reactionEventRunID(event dal.ReactionEventRecord) string {
	if runID := strings.TrimSpace(event.RunID); runID != "" {
		return runID
	}

	return reactionPayloadString(event.PayloadJSON, "run_id")
}

func reactionPayloadString(payload []byte, field string) string {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		return ""
	}

	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return ""
	}

	value, ok := object[field].(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

func rejectJobTriggerCycle(sourceJobID, targetJobID string, maxDepth int, edges []dal.ReactionJobTriggerEdge) error {
	sourceJobID = strings.TrimSpace(sourceJobID)
	targetJobID = strings.TrimSpace(targetJobID)
	if sourceJobID == "" {
		return nil
	}

	if targetJobID == sourceJobID {
		return fmt.Errorf("%w: trigger job reaction would create a self-cycle for job %s", dal.ErrConflict, targetJobID)
	}

	if maxDepth <= 0 {
		maxDepth = defaultJobTriggerCycleDepth
	}

	explicit := map[string][]string{}
	var wildcardTargets []string
	for _, edge := range edges {
		if !edgeMatchesRunCompletion(edge) {
			continue
		}

		edgeSource := strings.TrimSpace(edge.SourceJobID)
		edgeTarget := strings.TrimSpace(edge.TargetJobID)
		if edgeTarget == "" {
			continue
		}

		if edgeSource == "" {
			if edgeTarget == targetJobID {
				return fmt.Errorf("%w: trigger job reaction would let wildcard completion subscription %s self-trigger job %s", dal.ErrConflict, edge.SubscriptionID, targetJobID)
			}

			if !slices.Contains(wildcardTargets, edgeTarget) {
				wildcardTargets = append(wildcardTargets, edgeTarget)
			}

			continue
		}

		if edgeSource == edgeTarget && edgeTarget == targetJobID {
			return fmt.Errorf("%w: trigger job reaction would use self-cycling subscription %s for job %s", dal.ErrConflict, edge.SubscriptionID, targetJobID)
		}

		if !slices.Contains(explicit[edgeSource], edgeTarget) {
			explicit[edgeSource] = append(explicit[edgeSource], edgeTarget)
		}
	}

	if !slices.Contains(explicit[sourceJobID], targetJobID) {
		explicit[sourceJobID] = append(explicit[sourceJobID], targetJobID)
	}

	type queued struct {
		job   string
		depth int
	}

	queue := []queued{{job: targetJobID, depth: 0}}
	seen := map[string]struct{}{targetJobID: {}}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if current.depth >= maxDepth {
			return fmt.Errorf("%w: trigger job cycle check exceeded max_depth %d while checking %s -> %s", dal.ErrConflict, maxDepth, sourceJobID, targetJobID)
		}

		neighbors := append([]string{}, explicit[current.job]...)
		neighbors = append(neighbors, wildcardTargets...)
		for _, next := range neighbors {
			next = strings.TrimSpace(next)
			if next == "" {
				continue
			}

			if next == sourceJobID {
				return fmt.Errorf("%w: trigger job reaction would create a cycle %s -> %s", dal.ErrConflict, sourceJobID, targetJobID)
			}

			if _, ok := seen[next]; ok {
				continue
			}

			seen[next] = struct{}{}
			queue = append(queue, queued{job: next, depth: current.depth + 1})
		}
	}

	return nil
}

func edgeMatchesRunCompletion(edge dal.ReactionJobTriggerEdge) bool {
	eventType := strings.TrimSpace(edge.EventType)
	if eventType != "" && eventType != dal.ReactionEventTypeRunCompleted {
		return false
	}

	triggerType := strings.TrimSpace(edge.TriggerType)
	if triggerType != "" && triggerType != dal.TriggerTypeReaction {
		return false
	}

	return true
}

func createdRunsMatchTargetCells(runs []dal.CreatedRun, targetCellIDs []string) bool {
	if len(targetCellIDs) == 0 {
		return true
	}

	if len(runs) != len(targetCellIDs) {
		return false
	}

	for i, run := range runs {
		if strings.TrimSpace(run.TargetCellID) != targetCellIDs[i] {
			return false
		}
	}

	return true
}

var _ Action = (*TriggerJobAction)(nil)
