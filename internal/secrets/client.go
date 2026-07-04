package secrets

import (
	"context"
	"io/fs"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type Resolver interface {
	Resolve(ctx context.Context, req ResolveRequest) (Bundle, error)
}

type GRPCResolver struct {
	client api.SecretsServiceClient
}

func NewGRPCResolver(conn grpc.ClientConnInterface) *GRPCResolver {
	return &GRPCResolver{client: api.NewSecretsServiceClient(conn)}
}

func (r *GRPCResolver) Resolve(ctx context.Context, req ResolveRequest) (Bundle, error) {
	resp, err := r.client.ResolveSecrets(ctx, resolveRequestToProto(req))
	if err != nil {
		return Bundle{}, err
	}

	files := make([]FileMaterial, 0, len(resp.GetFiles()))
	for _, file := range resp.GetFiles() {
		if file == nil {
			continue
		}

		files = append(files, FileMaterial{
			ID:   file.GetId(),
			Path: file.GetPath(),
			Data: append([]byte(nil), file.GetData()...),
			Mode: fileMode(file.GetMode()),
		})
	}

	return Bundle{Files: files}, nil
}

func resolveRequestToProto(req ResolveRequest) *api.ResolveSecretsRequest {
	return &api.ResolveSecretsRequest{
		RunId:               strptr(req.RunID),
		ExecutionId:         strptr(req.ExecutionID),
		ExecutionClaimToken: strptr(req.ExecutionClaimToken),
		Secrets:             referencesToProto(req.Secrets),
	}
}

func referencesToProto(refs []Reference) []*api.SecretReference {
	if len(refs) == 0 {
		return nil
	}

	out := make([]*api.SecretReference, 0, len(refs))
	for _, ref := range refs {
		deliveryType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_UNSPECIFIED
		if ref.Delivery.Type == DeliveryTypeFile {
			deliveryType = api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
		}

		out = append(out, &api.SecretReference{
			Id:  strptr(ref.ID),
			Ref: strptr(ref.Ref),
			Delivery: &api.SecretDelivery{
				Type: &deliveryType,
				Path: strptr(ref.Delivery.Path),
			},
			TaskKeys: append([]string(nil), ref.TaskKeys...),
		})
	}

	return out
}

func strptr(s string) *string {
	return &s
}

func fileMode(mode uint32) fs.FileMode {
	if mode == 0 {
		return DefaultFileMode
	}

	return fs.FileMode(mode)
}
