package scmpoller

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vectis/internal/action/actionregistry"
	"vectis/internal/backoff"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
	"vectis/internal/scmtrigger"
	triggerrunner "vectis/internal/trigger"
	"vectis/sdk/scm"
)

const (
	DefaultBatchSize = 100
	DefaultInterval  = 30 * time.Second
	DefaultClaimTTL  = 5 * time.Minute
)

type Provider = scm.PollProvider
type PollSpec = scm.PollSpec
type PollResult = scm.PollResult
type Event = scm.Event

type Service struct {
	polls     dal.SCMPollTriggersRepository
	jobs      dal.JobsRepository
	runs      dal.RunsRepository
	dispatch  dal.DispatchEventsRepository
	triggers  dal.TriggerInvocationsRepository
	logger    interfaces.Logger
	clock     interfaces.Clock
	providers map[string]Provider

	queueClient    interfaces.QueueService
	queueClose     func()
	ingress        cell.ExecutionIngress
	retryMetrics   backoff.RetryMetrics
	actionResolver actionregistry.Resolver

	instanceID string
	claimTTL   time.Duration
	batchSize  int
	claimSeq   atomic.Uint64
	mu         sync.Mutex
}

func NewService(logger interfaces.Logger, db *sql.DB) *Service {
	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	s := NewServiceWithRepository(logger, repos.SCMPollTriggers(), interfaces.SystemClock{})
	s.jobs = repos.Jobs()
	s.runs = repos.Runs()
	s.dispatch = repos.DispatchEvents()
	s.triggers = repos.TriggerInvocations()
	return s
}

func NewServiceWithRepository(logger interfaces.Logger, polls dal.SCMPollTriggersRepository, clock interfaces.Clock) *Service {
	if logger == nil {
		logger = interfaces.NewLogger("scm-poller")
	}
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	return &Service{
		polls:      polls,
		logger:     logger,
		clock:      clock,
		providers:  map[string]Provider{},
		instanceID: "scm-poller",
		claimTTL:   DefaultClaimTTL,
		batchSize:  DefaultBatchSize,
	}
}

func (s *Service) SetInstanceID(id string) {
	if id = strings.TrimSpace(id); id != "" {
		s.instanceID = id
	}
}

func (s *Service) InstanceID() string {
	if strings.TrimSpace(s.instanceID) == "" {
		return "scm-poller"
	}
	return s.instanceID
}

func (s *Service) SetClaimTTL(ttl time.Duration) {
	if ttl > 0 {
		s.claimTTL = ttl
	}
}

func (s *Service) SetBatchSize(size int) {
	if size > 0 {
		s.batchSize = size
	}
}

func (s *Service) SetQueueClient(client interfaces.QueueService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}

	s.queueClient = client
}

func (s *Service) SetExecutionIngress(ingress cell.ExecutionIngress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingress = ingress
}

func (s *Service) SetClock(clock interfaces.Clock) {
	if clock != nil {
		s.clock = clock
	}
}

func (s *Service) SetRetryMetrics(m backoff.RetryMetrics) {
	s.retryMetrics = m
}

func (s *Service) SetActionDescriptorResolver(resolver actionregistry.Resolver) {
	s.actionResolver = resolver
}

func (s *Service) SetTriggerInvocations(triggers dal.TriggerInvocationsRepository) {
	s.triggers = triggers
}

func (s *Service) CloseQueueDial() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
}

func (s *Service) ConnectToQueue(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
	s.queueClient = nil

	pin := config.SCMPollerQueueAddress()
	mq, err := queueclient.NewManagingQueuePoolService(ctx, s.logger, queueclient.QueuePoolOptions{
		PinnedAddress:   pin,
		RegistryAddress: config.SCMPollerRegistryDialAddress(),
		RetryMetrics:    s.retryMetrics,
	})

	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient = mq
	s.queueClose = func() { _ = mq.Close() }
	if pin == "" {
		s.logger.Info("Connected to queue via registry resolution")
	}

	return nil
}

func (s *Service) RegisterProvider(name string, provider Provider) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" || provider == nil {
		return
	}
	s.providers[name] = provider
}

func (s *Service) Process(ctx context.Context) error {
	if s.polls == nil {
		return fmt.Errorf("scm poll trigger repository is not set")
	}

	now := s.clock.Now().UTC()
	ready, err := s.polls.GetReady(ctx, now, s.batchSize)
	if err != nil {
		return fmt.Errorf("list ready scm poll triggers: %w", err)
	}
	if len(ready) == 0 {
		return nil
	}

	s.logger.Info("scm-poller: processing %d trigger(s)", len(ready))
	for _, spec := range ready {
		if err := s.processSpec(ctx, spec, now); err != nil {
			s.logger.Error("scm-poller: trigger %d (%s/%s): %v", spec.TriggerID, spec.Provider, spec.Project, err)
		}
	}

	return nil
}

func (s *Service) processSpec(ctx context.Context, spec dal.SCMPollTriggerSpec, now time.Time) error {
	token := s.nextClaimToken(spec)
	claimedUntil := now.Add(s.effectiveClaimTTL())
	claimed, err := s.polls.ClaimDue(ctx, spec.ID, spec.NextPollAt, token, claimedUntil, now)
	if err != nil {
		return fmt.Errorf("claim scm poll trigger: %w", err)
	}
	if !claimed {
		s.logger.Debug("scm-poller: trigger %d already claimed or advanced", spec.TriggerID)
		return nil
	}

	completed := false
	defer func() {
		if completed {
			return
		}
		if err := s.polls.ReleaseClaim(context.Background(), spec.ID, token); err != nil {
			s.logger.Error("scm-poller: release claim for trigger %d failed: %v", spec.TriggerID, err)
		}
	}()

	providerName := strings.ToLower(strings.TrimSpace(spec.Provider))
	provider := s.providers[providerName]
	if provider == nil {
		nextPoll := nextPollTime(now, spec.Interval)
		ok, err := s.polls.CompleteClaim(ctx, spec.ID, token, nextPoll, spec.Cursor)
		if err != nil {
			return fmt.Errorf("complete unsupported provider poll: %w", err)
		}
		if !ok {
			return fmt.Errorf("complete unsupported provider poll: claim was lost")
		}
		completed = true
		s.logger.Warn("scm-poller: no provider registered for %q; next poll at %s", spec.Provider, nextPoll.Format(time.RFC3339))
		return nil
	}

	result, err := provider.Poll(ctx, pollSpecFromDAL(spec))
	if err != nil {
		return fmt.Errorf("poll provider %q: %w", spec.Provider, err)
	}

	for _, event := range result.Events {
		if err := s.handleEvent(ctx, spec, event); err != nil {
			return err
		}
	}

	nextPoll := nextPollTime(now, spec.Interval)
	cursor := result.Cursor
	if strings.TrimSpace(cursor) == "" {
		cursor = spec.Cursor
	}

	ok, err := s.polls.CompleteClaim(ctx, spec.ID, token, nextPoll, cursor)
	if err != nil {
		return fmt.Errorf("complete scm poll trigger: %w", err)
	}
	if !ok {
		return fmt.Errorf("complete scm poll trigger: claim was lost")
	}

	completed = true
	return nil
}

func (s *Service) handleEvent(ctx context.Context, spec dal.SCMPollTriggerSpec, event Event) error {
	_, err := s.eventProcessor().HandleEvent(ctx, spec, event)
	return err
}

func (s *Service) eventProcessor() scmtrigger.Processor {
	s.mu.Lock()
	qc := s.queueClient
	ingress := s.ingress
	s.mu.Unlock()

	return scmtrigger.Processor{
		Events:         s.polls,
		Jobs:           s.jobs,
		Runs:           s.runs,
		Dispatch:       s.dispatch,
		Invocations:    s.triggers,
		QueueClient:    qc,
		Ingress:        ingress,
		Logger:         s.logger,
		Clock:          s.clock,
		ActionResolver: s.actionResolver,
		Source:         dal.DispatchSourceSCMPoller,
		SourceInstance: s.InstanceID(),
	}
}

func (s *Service) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultInterval
	}

	return triggerrunner.Runner{
		Name:      "scm-poller",
		Logger:    s.logger,
		Clock:     s.clock,
		Interval:  interval,
		Processor: s,
	}.Run(ctx)
}

func (s *Service) effectiveClaimTTL() time.Duration {
	if s.claimTTL > 0 {
		return s.claimTTL
	}

	if ttl := config.SCMPollerClaimTTL(); ttl > 0 {
		return ttl
	}

	return DefaultClaimTTL
}

func (s *Service) nextClaimToken(spec dal.SCMPollTriggerSpec) string {
	seq := s.claimSeq.Add(1)
	return fmt.Sprintf("%s:%d:%d:%d", s.InstanceID(), spec.ID, spec.NextPollAt.UTC().Unix(), seq)
}

func nextPollTime(now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		interval = DefaultInterval
	}
	return now.UTC().Add(interval).Truncate(time.Second)
}

func pollSpecFromDAL(spec dal.SCMPollTriggerSpec) PollSpec {
	return PollSpec{
		TriggerID: spec.TriggerID,
		JobID:     spec.JobID,
		Provider:  spec.Provider,
		BaseURL:   spec.BaseURL,
		Project:   spec.Project,
		Branch:    spec.Branch,
		Query:     spec.Query,
		Cursor:    spec.Cursor,
	}
}
