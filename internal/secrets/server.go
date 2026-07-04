package secrets

import (
	"context"
	"errors"
	"strconv"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/serviceidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	resolveOutcomeSuccess  = "success"
	resolveOutcomeDenied   = "denied"
	resolveOutcomeNotFound = "not_found"
	resolveOutcomeFailed   = "failed"

	resolveReasonOK                  = "ok"
	resolveReasonUnknown             = "unknown"
	resolveReasonMissingProvider     = "missing_provider"
	resolveReasonMissingAuthorizer   = "missing_authorizer"
	resolveReasonAuthorizationDenied = "authorization_denied"
	resolveReasonProviderDenied      = "provider_denied"
	resolveReasonProviderNotFound    = "provider_not_found"
	resolveReasonProviderError       = "provider_error"
	resolveReasonInvalidBundle       = "invalid_bundle"
)

type Authorizer interface {
	AuthorizeResolve(ctx context.Context, req *ResolveRequest) error
}

type ResolveMetrics interface {
	RecordResolve(ctx context.Context, outcome, reason, provider string, duration time.Duration)
}

type Server struct {
	api.UnimplementedSecretsServiceServer

	provider   Provider
	authorizer Authorizer
	metrics    ResolveMetrics
	logger     interfaces.Logger
}

type ServerOption func(*Server)

func WithMetrics(metrics ResolveMetrics) ServerOption {
	return func(s *Server) {
		s.metrics = metrics
	}
}

func WithLogger(logger interfaces.Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

func NewServer(provider Provider, authorizer Authorizer, opts ...ServerOption) *Server {
	s := &Server{
		provider:   provider,
		authorizer: authorizer,
	}

	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}

	return s
}

func (s *Server) ResolveSecrets(ctx context.Context, req *api.ResolveSecretsRequest) (*api.ResolveSecretsResponse, error) {
	start := time.Now()
	if s == nil || s.provider == nil {
		if s != nil {
			s.observeResolve(ctx, ResolveRequest{}, resolveObservation{
				outcome:  resolveOutcomeFailed,
				reason:   resolveReasonMissingProvider,
				provider: providerKind(nil),
				duration: time.Since(start),
			})
		}

		return nil, status.Error(codes.FailedPrecondition, "secret provider is not configured")
	}

	provider := providerKindForRequest(s.provider, nil)
	if s.authorizer == nil {
		s.observeResolve(ctx, resolveRequestFromProto(req), resolveObservation{
			outcome:  resolveOutcomeFailed,
			reason:   resolveReasonMissingAuthorizer,
			provider: provider,
			duration: time.Since(start),
		})

		return nil, status.Error(codes.FailedPrecondition, "secret authorizer is not configured")
	}

	domainReq := resolveRequestFromProto(req)
	domainReq.PeerSPIFFEID = peerSPIFFEID(ctx)
	provider = providerKindForRequest(s.provider, domainReq.Secrets)

	if err := s.authorizer.AuthorizeResolve(ctx, &domainReq); err != nil {
		s.observeResolve(ctx, domainReq, resolveObservation{
			outcome:  resolveOutcomeDenied,
			reason:   resolveReasonAuthorizationDenied,
			provider: provider,
			duration: time.Since(start),
		})

		return nil, status.Error(codes.PermissionDenied, "secret resolution is not authorized")
	}

	bundle, err := s.provider.Resolve(ctx, domainReq)
	if err != nil {
		outcome, reason := classifyProviderError(err)
		s.observeResolve(ctx, domainReq, resolveObservation{
			outcome:  outcome,
			reason:   reason,
			provider: provider,
			duration: time.Since(start),
		})

		return nil, status.Error(codeForProviderError(err), "secret resolution failed")
	}

	if err := ValidateResolvedBundle(domainReq, bundle); err != nil {
		s.observeResolve(ctx, domainReq, resolveObservation{
			outcome:  resolveOutcomeFailed,
			reason:   resolveReasonInvalidBundle,
			provider: provider,
			duration: time.Since(start),
		})

		return nil, status.Error(codes.Internal, "secret resolution failed")
	}

	resp := &api.ResolveSecretsResponse{
		Files: make([]*api.SecretFileMaterial, 0, len(bundle.Files)),
	}

	for _, file := range bundle.Files {
		mode := uint32(file.Mode)
		resp.Files = append(resp.Files, &api.SecretFileMaterial{
			Id:   &file.ID,
			Path: &file.Path,
			Data: append([]byte(nil), file.Data...),
			Mode: &mode,
		})
	}

	s.observeResolve(ctx, domainReq, resolveObservation{
		outcome:  resolveOutcomeSuccess,
		reason:   resolveReasonOK,
		provider: provider,
		duration: time.Since(start),
		files:    len(bundle.Files),
	})

	return resp, nil
}

type resolveObservation struct {
	outcome  string
	reason   string
	provider string
	duration time.Duration
	files    int
}

func (s *Server) observeResolve(ctx context.Context, req ResolveRequest, obs resolveObservation) {
	if obs.outcome == "" {
		obs.outcome = resolveOutcomeFailed
	}

	if obs.reason == "" {
		obs.reason = resolveReasonUnknown
	}

	if obs.provider == "" {
		obs.provider = "unknown"
	}

	if s.metrics != nil {
		s.metrics.RecordResolve(ctx, obs.outcome, obs.reason, obs.provider, obs.duration)
	}

	if s.logger == nil {
		return
	}

	fields := resolveLogFields(req, obs)
	logger := s.logger.WithFields(fields)
	switch obs.outcome {
	case resolveOutcomeSuccess:
		logger.Info("Secret resolve completed")
	case resolveOutcomeDenied, resolveOutcomeNotFound:
		logger.Warn("Secret resolve rejected")
	default:
		logger.Error("Secret resolve failed")
	}
}

func resolveLogFields(req ResolveRequest, obs resolveObservation) map[string]string {
	scope := req.Scope
	runID := firstNonEmpty(scope.RunID, req.RunID)
	executionID := firstNonEmpty(scope.ExecutionID, req.ExecutionID)

	fields := map[string]string{
		"outcome":      obs.outcome,
		"reason":       obs.reason,
		"provider":     obs.provider,
		"run_id":       runID,
		"execution_id": executionID,
		"secrets":      strconv.Itoa(len(req.Secrets)),
		"files":        strconv.Itoa(obs.files),
	}

	if scope.NamespacePath != "" {
		fields["namespace"] = scope.NamespacePath
	}

	if scope.JobID != "" {
		fields["job_id"] = scope.JobID
	}

	if scope.TaskKey != "" {
		fields["task_key"] = scope.TaskKey
	}

	if scope.CellID != "" {
		fields["cell_id"] = scope.CellID
	}

	if req.PeerSPIFFEID != "" {
		fields["peer_spiffe_id"] = req.PeerSPIFFEID
	}

	return fields
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

func resolveRequestFromProto(req *api.ResolveSecretsRequest) ResolveRequest {
	if req == nil {
		return ResolveRequest{}
	}

	return ResolveRequest{
		RunID:               req.GetRunId(),
		ExecutionID:         req.GetExecutionId(),
		ExecutionClaimToken: req.GetExecutionClaimToken(),
		Secrets:             referencesFromProto(req.GetSecrets()),
	}
}

func referencesFromProto(refs []*api.SecretReference) []Reference {
	if len(refs) == 0 {
		return nil
	}

	out := make([]Reference, 0, len(refs))
	for _, ref := range refs {
		if ref == nil {
			continue
		}

		out = append(out, ReferenceFromProto(ref))
	}

	return out
}

func peerSPIFFEID(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return ""
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}

	ids := serviceidentity.PeerURISANs(tlsInfo.State.PeerCertificates)
	if len(ids) == 0 {
		return ""
	}

	return ids[0]
}

func codeForProviderError(err error) codes.Code {
	switch {
	case errors.Is(err, ErrNotFound):
		return codes.NotFound
	case errors.Is(err, ErrDenied):
		return codes.PermissionDenied
	default:
		return codes.Internal
	}
}

func classifyProviderError(err error) (string, string) {
	switch {
	case errors.Is(err, ErrDenied):
		return resolveOutcomeDenied, resolveReasonProviderDenied
	case errors.Is(err, ErrNotFound):
		return resolveOutcomeNotFound, resolveReasonProviderNotFound
	default:
		return resolveOutcomeFailed, resolveReasonProviderError
	}
}

func providerKindForRequest(provider Provider, refs []Reference) string {
	if requestKinded, ok := provider.(RequestKindedProvider); ok {
		if kind := requestKinded.ProviderKindForRefs(refs); kind != "" {
			return kind
		}
	}

	return providerKind(provider)
}

func providerKind(provider Provider) string {
	if provider == nil {
		return "none"
	}

	if kinded, ok := provider.(KindedProvider); ok {
		if kind := kinded.ProviderKind(); kind != "" {
			return kind
		}
	}

	return providerKindUnknown
}
