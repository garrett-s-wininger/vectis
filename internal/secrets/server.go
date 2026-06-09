package secrets

import (
	"context"
	"errors"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/serviceidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	ErrNotFound = errors.New("secrets: not found")
	ErrDenied   = errors.New("secrets: denied")
)

type Authorizer interface {
	AuthorizeResolve(ctx context.Context, req ResolveRequest) error
}

type Server struct {
	api.UnimplementedSecretsServiceServer

	provider   Provider
	authorizer Authorizer
}

func NewServer(provider Provider, authorizer Authorizer) *Server {
	return &Server{
		provider:   provider,
		authorizer: authorizer,
	}
}

func (s *Server) ResolveSecrets(ctx context.Context, req *api.ResolveSecretsRequest) (*api.ResolveSecretsResponse, error) {
	if s == nil || s.provider == nil {
		return nil, status.Error(codes.FailedPrecondition, "secret provider is not configured")
	}

	if s.authorizer == nil {
		return nil, status.Error(codes.FailedPrecondition, "secret authorizer is not configured")
	}

	domainReq := resolveRequestFromProto(req)
	domainReq.PeerSPIFFEID = peerSPIFFEID(ctx)

	if err := s.authorizer.AuthorizeResolve(ctx, domainReq); err != nil {
		return nil, status.Error(codes.PermissionDenied, "secret resolution is not authorized")
	}

	bundle, err := s.provider.Resolve(ctx, domainReq)
	if err != nil {
		return nil, status.Error(codeForProviderError(err), "secret resolution failed")
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

	return resp, nil
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

func (r ResolveRequest) String() string {
	return fmt.Sprintf("run=%s execution=%s secrets=%d peer=%s", r.RunID, r.ExecutionID, len(r.Secrets), r.PeerSPIFFEID)
}
