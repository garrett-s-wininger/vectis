package workercore

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/secrets"
	"vectis/internal/workloadidentity"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = workersdk.ProtocolVersion

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

func RequiredWorkerCoreCapabilities() []string {
	return []string{
		workersdk.CapabilityExecute,
		workersdk.CapabilityCancelTask,
		workersdk.CapabilityShellLogCallback,
		workersdk.CapabilityShellArtifactPush,
	}
}

func ValidateCoreDescription(desc CoreDescription, requiredCapabilities []string) error {
	protocolVersion := strings.TrimSpace(desc.ProtocolVersion)
	if protocolVersion != ProtocolVersion {
		return fmt.Errorf("worker core protocol version %q is not supported; want %q", protocolVersion, ProtocolVersion)
	}

	supportedIsolation, err := action.NormalizeSupportedIsolationLevels(desc.SupportedIsolation)
	if err != nil {
		return fmt.Errorf("worker core supported isolation: %w", err)
	}
	if len(supportedIsolation) == 0 {
		return fmt.Errorf("worker core supported isolation is required")
	}

	missing := make([]string, 0, len(requiredCapabilities))
	for _, required := range requiredCapabilities {
		required = strings.TrimSpace(required)
		if required == "" || HasCoreCapability(desc, required) {
			continue
		}

		missing = append(missing, required)
	}

	if len(missing) > 0 {
		return fmt.Errorf("worker core missing required capabilities: %s", strings.Join(missing, ", "))
	}

	return nil
}

func HasCoreCapability(desc CoreDescription, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}

	for _, capability := range desc.Capabilities {
		if capability.Name == name {
			return true
		}
	}

	return false
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
		return NewTaskResultError(resp.GetOutcome(), resp.GetReasonCode(), resp.GetMessage())
	case api.RunOutcome_RUN_OUTCOME_UNKNOWN:
		return NewTaskResultError(resp.GetOutcome(), resp.GetReasonCode(), resp.GetMessage())
	default:
		return nil
	}
}

func (c *RemoteCore) CancelTask(ctx context.Context, req CancelTaskRequest) error {
	if c == nil || c.client == nil {
		return fmt.Errorf("remote worker core is not configured")
	}

	if strings.TrimSpace(req.SessionID) == "" {
		return fmt.Errorf("remote worker core cancel requires a task session id")
	}

	if _, err := c.client.CancelTask(ctx, &api.CancelWorkerCoreTaskRequest{
		SessionId: proto.String(req.SessionID),
		RunId:     proto.String(req.RunID),
		TaskKey:   proto.String(req.TaskKey),
		Reason:    proto.String(req.Reason),
	}); err != nil {
		return fmt.Errorf("remote worker core cancel task: %w", err)
	}

	return nil
}

var _ Core = (*RemoteCore)(nil)
var _ CancellableCore = (*RemoteCore)(nil)

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

	if err := ValidateTaskSessionIdentity(req.Job, session.GetSessionId(), req.Session.WorkloadIdentity()); err != nil {
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
		SecretFiles:      secretFilesProto(session.SecretFiles()),
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

func workloadIdentityFromProto(in *api.WorkerCoreWorkloadIdentity) *workloadidentity.Identity {
	if in == nil {
		return nil
	}

	out := &workloadidentity.Identity{
		SPIFFEID:      in.GetSpiffeId(),
		TrustDomain:   in.GetTrustDomain(),
		NamespacePath: in.GetNamespacePath(),
		CellID:        in.GetCellId(),
		JobID:         in.GetJobId(),
		RunID:         in.GetRunId(),
		ExecutionID:   in.GetExecutionId(),
	}
	if svidID := strings.TrimSpace(in.GetX509SvidSpiffeId()); svidID != "" {
		out.X509SVID = &workloadidentity.X509SVID{SPIFFEID: svidID}
	}
	return out
}

func secretFilesFromProto(in []*api.SecretFileMaterial) []secrets.FileMaterial {
	if len(in) == 0 {
		return nil
	}

	out := make([]secrets.FileMaterial, 0, len(in))
	for _, file := range in {
		if file == nil {
			continue
		}

		out = append(out, secrets.FileMaterial{
			ID:   file.GetId(),
			Path: file.GetPath(),
			Data: append([]byte(nil), file.GetData()...),
			Mode: secretsModeFromProto(file.GetMode()),
		})
	}

	return out
}

func secretsModeFromProto(mode uint32) fs.FileMode {
	if mode == 0 {
		return secrets.DefaultFileMode
	}

	return fs.FileMode(mode)
}

func actionLocksFromProto(in []*api.WorkerCoreActionLock) []actionregistry.ActionLock {
	if len(in) == 0 {
		return nil
	}

	out := make([]actionregistry.ActionLock, 0, len(in))
	for _, lock := range in {
		if lock == nil {
			continue
		}
		out = append(out, actionregistry.ActionLock{
			NodeID:     lock.GetNodeId(),
			NodePath:   lock.GetNodePath(),
			Uses:       lock.GetUses(),
			Descriptor: descriptorFromProto(lock.GetDescriptor_()),
		})
	}
	return out
}

func descriptorFromProto(in *api.WorkerCoreActionDescriptor) actionregistry.Descriptor {
	if in == nil {
		return actionregistry.Descriptor{}
	}

	return actionregistry.Descriptor{
		CanonicalName: in.GetCanonicalName(),
		DisplayName:   in.GetDisplayName(),
		Version:       in.GetVersion(),
		Digest:        in.GetDigest(),
		Source:        actionregistry.SourceType(in.GetSource()),
		Runtime:       actionregistry.RuntimeType(in.GetRuntime()),
		RuntimeConfig: cloneStringMap(in.GetRuntimeConfig()),
		InputSchema:   inputSchemaFromProto(in.GetInputSchema()),
		PortSchema:    portSchemaFromProto(in.GetPortSchema()),
		LocalOnly:     in.GetLocalOnly(),
		Capabilities:  capabilitiesFromProto(in.GetCapabilities()),
		Status:        actionregistry.DescriptorStatus(in.GetStatus()),
		StatusReason:  in.GetStatusReason(),
	}
}

func inputSchemaFromProto(in *api.WorkerCoreInputSchema) actionregistry.InputSchema {
	if in == nil {
		return actionregistry.InputSchema{}
	}

	out := actionregistry.InputSchema{AllowUnknown: in.GetAllowUnknown()}
	for _, field := range in.GetFields() {
		if field == nil {
			continue
		}
		out.Fields = append(out.Fields, actionregistry.InputField{
			Name:     field.GetName(),
			Type:     action.FieldType(field.GetType()),
			Required: field.GetRequired(),
		})
	}
	return out
}

func portSchemaFromProto(in []*api.WorkerCorePortSpec) []actionregistry.PortSpec {
	if len(in) == 0 {
		return nil
	}

	out := make([]actionregistry.PortSpec, 0, len(in))
	for _, port := range in {
		if port == nil {
			continue
		}
		out = append(out, actionregistry.PortSpec{
			Name:     port.GetName(),
			Min:      int(port.GetMin()),
			Max:      int(port.GetMax()),
			Primary:  port.GetPrimary(),
			Ordered:  port.GetOrdered(),
			Required: port.GetRequired(),
		})
	}
	return out
}

func capabilitiesFromProto(in []string) []actionregistry.Capability {
	if len(in) == 0 {
		return nil
	}

	out := make([]actionregistry.Capability, 0, len(in))
	for _, capability := range in {
		out = append(out, actionregistry.Capability(capability))
	}
	return out
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

func secretFilesProto(files []secrets.FileMaterial) []*api.SecretFileMaterial {
	if len(files) == 0 {
		return nil
	}

	out := make([]*api.SecretFileMaterial, 0, len(files))
	for _, file := range files {
		out = append(out, &api.SecretFileMaterial{
			Id:   proto.String(file.ID),
			Path: proto.String(file.Path),
			Data: append([]byte(nil), file.Data...),
			Mode: proto.Uint32(uint32(file.Mode)),
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
