package workercore

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = "workercore.v1alpha1"

const (
	CapabilityExecute           = "worker-core.execute"
	CapabilityCancelTask        = "worker-core.cancel_task"
	CapabilityShellLogCallback  = "worker-core.shell.logs"
	CapabilityShellArtifactPush = "worker-core.shell.artifacts"
)

type Description struct {
	ProtocolVersion    string
	Capabilities       []Capability
	SupportedIsolation []string
	Metadata           map[string]string
}

type Capability struct {
	Name     string
	Version  string
	Metadata map[string]string
}

func HasCapability(desc Description, name string) bool {
	for _, capability := range desc.Capabilities {
		if capability.Name == name {
			return true
		}
	}

	return false
}

type Task struct {
	Job     *api.Job
	TaskKey string
	Session Session
}

type Core interface {
	Describe(context.Context) (Description, error)
	ExecuteTask(context.Context, Task) (Result, error)
	CancelTask(context.Context, CancelRequest) error
}

type CancelRequest struct {
	SessionID string
	RunID     string
	TaskKey   string
	Reason    string
}

type Result struct {
	Outcome api.RunOutcome
	Message string
}

func Success() Result {
	return Result{Outcome: api.RunOutcome_RUN_OUTCOME_SUCCESS}
}

func Failure(message string) Result {
	return Result{
		Outcome: api.RunOutcome_RUN_OUTCOME_FAILURE,
		Message: strings.TrimSpace(message),
	}
}

func Failuref(format string, args ...any) Result {
	return Failure(fmt.Sprintf(format, args...))
}

func Unknown(message string) Result {
	return Result{
		Outcome: api.RunOutcome_RUN_OUTCOME_UNKNOWN,
		Message: strings.TrimSpace(message),
	}
}

func Unknownf(format string, args ...any) Result {
	return Unknown(fmt.Sprintf(format, args...))
}

func descriptionProto(desc Description) *api.DescribeWorkerCoreResponse {
	protocolVersion := strings.TrimSpace(desc.ProtocolVersion)
	if protocolVersion == "" {
		protocolVersion = ProtocolVersion
	}

	return &api.DescribeWorkerCoreResponse{
		ProtocolVersion:    proto.String(protocolVersion),
		Capabilities:       capabilitiesProto(desc.Capabilities),
		SupportedIsolation: append([]string(nil), desc.SupportedIsolation...),
		Metadata:           cloneStringMap(desc.Metadata),
	}
}

func capabilitiesProto(in []Capability) []*api.WorkerCoreCapability {
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

func resultProto(result Result) *api.ExecuteWorkerCoreTaskResponse {
	outcome := result.Outcome
	if outcome == api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
		outcome = api.RunOutcome_RUN_OUTCOME_UNKNOWN
	}

	return &api.ExecuteWorkerCoreTaskResponse{
		Outcome: outcome.Enum(),
		Message: proto.String(strings.TrimSpace(result.Message)),
	}
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
