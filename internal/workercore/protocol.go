package workercore

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/workloadidentity"

	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = "workercore.v1alpha1"

type CoreDescription struct {
	ProtocolVersion    string
	Capabilities       []CoreCapability
	SupportedIsolation []string
	Metadata           map[string]string
}

type CoreCapability struct {
	Name     string
	Version  string
	Metadata map[string]string
}

type RemoteCore struct {
	client api.WorkerCoreServiceClient
}

func NewRemoteCore(client api.WorkerCoreServiceClient) *RemoteCore {
	return &RemoteCore{client: client}
}

func (c *RemoteCore) Describe(ctx context.Context) (CoreDescription, error) {
	if c == nil || c.client == nil {
		return CoreDescription{}, fmt.Errorf("remote worker core is not configured")
	}

	resp, err := c.client.DescribeCore(ctx, &api.DescribeWorkerCoreRequest{})
	if err != nil {
		return CoreDescription{}, fmt.Errorf("describe remote worker core: %w", err)
	}

	return coreDescriptionFromProto(resp), nil
}

func (c *RemoteCore) ExecuteTask(ctx context.Context, req ExecuteTaskRequest) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("remote worker core is not configured")
	}

	protoReq, err := ExecuteTaskRequestProto(req)
	if err != nil {
		return err
	}

	session := protoReq.GetSession()
	if strings.TrimSpace(session.GetSessionId()) == "" {
		return fmt.Errorf("remote worker core requires a task session id")
	}

	if (session.GetLogsEnabled() || session.GetArtifactsEnabled()) && strings.TrimSpace(session.GetShellEndpoint()) == "" {
		return fmt.Errorf("remote worker core requires a shell endpoint for callback-enabled sessions")
	}

	resp, err := c.client.ExecuteTask(ctx, protoReq)
	if err != nil {
		return fmt.Errorf("remote worker core execute task: %w", err)
	}

	switch resp.GetOutcome() {
	case api.RunOutcome_RUN_OUTCOME_FAILURE:
		if msg := strings.TrimSpace(resp.GetMessage()); msg != "" {
			return fmt.Errorf("remote worker core task failed: %s", msg)
		}
		return fmt.Errorf("remote worker core task failed")
	case api.RunOutcome_RUN_OUTCOME_UNKNOWN:
		if msg := strings.TrimSpace(resp.GetMessage()); msg != "" {
			return fmt.Errorf("remote worker core task outcome unknown: %s", msg)
		}
		return fmt.Errorf("remote worker core task outcome unknown")
	default:
		return nil
	}
}

var _ Core = (*RemoteCore)(nil)

func ExecuteTaskRequestProto(req ExecuteTaskRequest) (*api.ExecuteWorkerCoreTaskRequest, error) {
	if req.Job == nil {
		return nil, fmt.Errorf("worker core protocol requires a job")
	}

	if strings.TrimSpace(req.TaskKey) == "" {
		return nil, fmt.Errorf("worker core protocol requires a task key")
	}

	session, err := TaskSessionProto(req.Session)
	if err != nil {
		return nil, err
	}

	return &api.ExecuteWorkerCoreTaskRequest{
		Job:     req.Job,
		TaskKey: proto.String(req.TaskKey),
		Session: session,
	}, nil
}

func TaskSessionProto(session TaskSession) (*api.WorkerCoreTaskSession, error) {
	if session == nil {
		return nil, fmt.Errorf("worker core protocol requires a task session")
	}

	return &api.WorkerCoreTaskSession{
		SessionId:        proto.String(session.SessionID()),
		ShellEndpoint:    proto.String(session.ShellEndpoint()),
		WorkloadIdentity: workloadIdentityProto(session.WorkloadIdentity()),
		ActionLocks:      actionLocksProto(session.ActionLocks()),
		LogsEnabled:      proto.Bool(session.LogClient() != nil),
		ArtifactsEnabled: proto.Bool(session.ArtifactPublisher() != nil),
	}, nil
}

func CoreDescriptionProto(desc CoreDescription) *api.DescribeWorkerCoreResponse {
	protocolVersion := strings.TrimSpace(desc.ProtocolVersion)
	if protocolVersion == "" {
		protocolVersion = ProtocolVersion
	}

	return &api.DescribeWorkerCoreResponse{
		ProtocolVersion:    proto.String(protocolVersion),
		Capabilities:       coreCapabilitiesProto(desc.Capabilities),
		SupportedIsolation: append([]string(nil), desc.SupportedIsolation...),
		Metadata:           cloneStringMap(desc.Metadata),
	}
}

func ArtifactMetadataProto(sessionID string, req action.ArtifactPublishRequest) *api.WorkerCoreArtifactMetadata {
	metadata := &api.WorkerCoreArtifactMetadata{
		SessionId:    proto.String(sessionID),
		Name:         proto.String(req.Name),
		Path:         proto.String(req.Path),
		ContentType:  proto.String(req.ContentType),
		ExpectedSize: proto.Int64(req.ExpectedSize),
		RequireSize:  proto.Bool(req.RequireSize),
		MaxBytes:     proto.Int64(req.MaxBytes),
	}

	if req.MetadataJSON != nil {
		metadata.MetadataJson = proto.String(*req.MetadataJSON)
	}

	return metadata
}

func ArtifactResultFromProto(in *api.WorkerCoreArtifact) action.ArtifactPublishResult {
	if in == nil {
		return action.ArtifactPublishResult{}
	}

	return action.ArtifactPublishResult{
		Name:            in.GetName(),
		Path:            in.GetPath(),
		ContentType:     in.GetContentType(),
		BlobKey:         in.GetBlobKey(),
		BlobAlgorithm:   in.GetBlobAlgorithm(),
		BlobDigest:      in.GetBlobDigest(),
		SizeBytes:       in.GetSizeBytes(),
		ArtifactShardID: in.GetArtifactShardId(),
	}
}

func ArtifactResultProto(in action.ArtifactPublishResult) *api.WorkerCoreArtifact {
	return &api.WorkerCoreArtifact{
		Name:            proto.String(in.Name),
		Path:            proto.String(in.Path),
		ContentType:     proto.String(in.ContentType),
		BlobKey:         proto.String(in.BlobKey),
		BlobAlgorithm:   proto.String(in.BlobAlgorithm),
		BlobDigest:      proto.String(in.BlobDigest),
		SizeBytes:       proto.Int64(in.SizeBytes),
		ArtifactShardId: proto.String(in.ArtifactShardID),
	}
}

func coreDescriptionFromProto(in *api.DescribeWorkerCoreResponse) CoreDescription {
	if in == nil {
		return CoreDescription{}
	}

	out := CoreDescription{
		ProtocolVersion:    in.GetProtocolVersion(),
		SupportedIsolation: append([]string(nil), in.GetSupportedIsolation()...),
		Metadata:           cloneStringMap(in.GetMetadata()),
	}

	for _, capability := range in.GetCapabilities() {
		if capability == nil {
			continue
		}
		out.Capabilities = append(out.Capabilities, CoreCapability{
			Name:     capability.GetName(),
			Version:  capability.GetVersion(),
			Metadata: cloneStringMap(capability.GetMetadata()),
		})
	}

	return out
}

func coreCapabilitiesProto(in []CoreCapability) []*api.WorkerCoreCapability {
	if len(in) == 0 {
		return nil
	}

	out := make([]*api.WorkerCoreCapability, 0, len(in))
	for _, capability := range in {
		out = append(out, &api.WorkerCoreCapability{
			Name:     proto.String(capability.Name),
			Version:  proto.String(capability.Version),
			Metadata: cloneStringMap(capability.Metadata),
		})
	}

	return out
}

func workloadIdentityProto(identity *workloadidentity.Identity) *api.WorkerCoreWorkloadIdentity {
	if identity == nil {
		return nil
	}

	out := &api.WorkerCoreWorkloadIdentity{
		SpiffeId:      proto.String(identity.SPIFFEID),
		TrustDomain:   proto.String(identity.TrustDomain),
		NamespacePath: proto.String(identity.NamespacePath),
		CellId:        proto.String(identity.CellID),
		JobId:         proto.String(identity.JobID),
		RunId:         proto.String(identity.RunID),
		ExecutionId:   proto.String(identity.ExecutionID),
	}

	if identity.X509SVID != nil {
		out.X509SvidSpiffeId = proto.String(identity.X509SVID.SPIFFEID)
	}

	return out
}

func actionLocksProto(locks []actionregistry.ActionLock) []*api.WorkerCoreActionLock {
	if len(locks) == 0 {
		return nil
	}

	out := make([]*api.WorkerCoreActionLock, 0, len(locks))
	for _, lock := range locks {
		out = append(out, &api.WorkerCoreActionLock{
			NodeId:      proto.String(lock.NodeID),
			NodePath:    proto.String(lock.NodePath),
			Uses:        proto.String(lock.Uses),
			Descriptor_: descriptorProto(lock.Descriptor),
		})
	}

	return out
}

func descriptorProto(desc actionregistry.Descriptor) *api.WorkerCoreActionDescriptor {
	return &api.WorkerCoreActionDescriptor{
		CanonicalName: proto.String(desc.CanonicalName),
		DisplayName:   proto.String(desc.DisplayName),
		Version:       proto.String(desc.Version),
		Digest:        proto.String(desc.Digest),
		Source:        proto.String(string(desc.Source)),
		Runtime:       proto.String(string(desc.Runtime)),
		RuntimeConfig: cloneStringMap(desc.RuntimeConfig),
		InputSchema:   inputSchemaProto(desc.InputSchema),
		PortSchema:    portSchemaProto(desc.PortSchema),
		LocalOnly:     proto.Bool(desc.LocalOnly),
		Capabilities:  capabilitiesProto(desc.Capabilities),
		Status:        proto.String(string(desc.Status)),
		StatusReason:  proto.String(desc.StatusReason),
	}
}

func inputSchemaProto(schema actionregistry.InputSchema) *api.WorkerCoreInputSchema {
	out := &api.WorkerCoreInputSchema{
		AllowUnknown: proto.Bool(schema.AllowUnknown),
	}

	if len(schema.Fields) == 0 {
		return out
	}

	out.Fields = make([]*api.WorkerCoreInputField, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		out.Fields = append(out.Fields, &api.WorkerCoreInputField{
			Name:     proto.String(field.Name),
			Type:     proto.String(string(field.Type)),
			Required: proto.Bool(field.Required),
		})
	}

	return out
}

func portSchemaProto(schema []actionregistry.PortSpec) []*api.WorkerCorePortSpec {
	if len(schema) == 0 {
		return nil
	}

	out := make([]*api.WorkerCorePortSpec, 0, len(schema))
	for _, port := range schema {
		out = append(out, &api.WorkerCorePortSpec{
			Name:     proto.String(port.Name),
			Min:      proto.Int32(int32(port.Min)),
			Max:      proto.Int32(int32(port.Max)),
			Primary:  proto.Bool(port.Primary),
			Ordered:  proto.Bool(port.Ordered),
			Required: proto.Bool(port.Required),
		})
	}

	return out
}

func capabilitiesProto(in []actionregistry.Capability) []string {
	if len(in) == 0 {
		return nil
	}

	out := make([]string, 0, len(in))
	for _, capability := range in {
		out = append(out, string(capability))
	}

	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}
