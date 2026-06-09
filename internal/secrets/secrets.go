// Package secrets defines the provider-facing contract for Vectis job secrets.
package secrets

import (
	"context"
	"io/fs"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/workloadidentity"
)

const (
	DeliveryTypeFile DeliveryType = "file"

	DefaultFileMode fs.FileMode = 0o400
)

type DeliveryType string

type Reference struct {
	ID       string
	Ref      string
	Delivery Delivery
	TaskKeys []string
}

type Delivery struct {
	Type DeliveryType
	Path string
}

type ResolveRequest struct {
	RunID               string
	ExecutionID         string
	ExecutionClaimToken string
	PeerSPIFFEID        string
	Workload            *workloadidentity.Identity
	Secrets             []Reference
}

type FileMaterial struct {
	ID   string
	Path string
	Data []byte
	Mode fs.FileMode
}

type Bundle struct {
	Files []FileMaterial
}

type Provider interface {
	ValidateRef(ctx context.Context, ref Reference) error
	Resolve(ctx context.Context, req ResolveRequest) (Bundle, error)
}

func ReferencesFromJob(job *api.Job) []Reference {
	if job == nil || len(job.GetSecrets()) == 0 {
		return nil
	}

	out := make([]Reference, 0, len(job.GetSecrets()))
	for _, ref := range job.GetSecrets() {
		if ref == nil {
			continue
		}

		out = append(out, ReferenceFromProto(ref))
	}

	return out
}

func ReferencesForTask(job *api.Job, taskKey string) []Reference {
	refs := ReferencesFromJob(job)
	if len(refs) == 0 {
		return nil
	}

	taskKey = strings.TrimSpace(taskKey)
	out := make([]Reference, 0, len(refs))
	for _, ref := range refs {
		if len(ref.TaskKeys) == 0 {
			out = append(out, ref)
			continue
		}

		for _, key := range ref.TaskKeys {
			if strings.TrimSpace(key) == taskKey {
				out = append(out, ref)
				break
			}
		}
	}

	return out
}

func ReferenceFromProto(ref *api.SecretReference) Reference {
	if ref == nil {
		return Reference{}
	}

	return Reference{
		ID:       strings.TrimSpace(ref.GetId()),
		Ref:      strings.TrimSpace(ref.GetRef()),
		Delivery: DeliveryFromProto(ref.GetDelivery()),
		TaskKeys: append([]string(nil), ref.GetTaskKeys()...),
	}
}

func DeliveryFromProto(delivery *api.SecretDelivery) Delivery {
	if delivery == nil {
		return Delivery{}
	}

	switch delivery.GetType() {
	case api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE:
		return Delivery{
			Type: DeliveryTypeFile,
			Path: strings.TrimSpace(delivery.GetPath()),
		}
	default:
		return Delivery{Path: strings.TrimSpace(delivery.GetPath())}
	}
}
