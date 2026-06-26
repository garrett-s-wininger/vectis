// Package secrets defines the provider-facing contract for Vectis job secrets.
package secrets

import (
	"crypto/x509"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/workloadidentity"
	sdksecrets "vectis/sdk/secrets"
)

const (
	DeliveryTypeFile = sdksecrets.DeliveryTypeFile

	DefaultFileMode       = sdksecrets.DefaultFileMode
	DefaultMaxSecretBytes = sdksecrets.DefaultMaxSecretBytes

	providerKindMulti   = sdksecrets.ProviderKindMulti
	providerKindMixed   = sdksecrets.ProviderKindMixed
	providerKindUnknown = sdksecrets.ProviderKindUnknown

	rootTaskKey string = "root"
)

var (
	ErrNotFound = sdksecrets.ErrNotFound
	ErrDenied   = sdksecrets.ErrDenied
)

type DeliveryType = sdksecrets.DeliveryType
type Reference = sdksecrets.Reference
type Delivery = sdksecrets.Delivery
type ResolveRequest = sdksecrets.ResolveRequest
type WorkloadIdentity = sdksecrets.WorkloadIdentity
type X509SVID = sdksecrets.X509SVID
type ExecutionScope = sdksecrets.ExecutionScope
type FileMaterial = sdksecrets.FileMaterial
type Bundle = sdksecrets.Bundle
type Provider = sdksecrets.Provider
type KindedProvider = sdksecrets.KindedProvider
type RequestKindedProvider = sdksecrets.RequestKindedProvider

func WorkloadIdentityFromInternal(identity *workloadidentity.Identity) *WorkloadIdentity {
	if identity == nil {
		return nil
	}

	out := &WorkloadIdentity{
		SPIFFEID:      identity.SPIFFEID,
		TrustDomain:   identity.TrustDomain,
		NamespacePath: identity.NamespacePath,
		CellID:        identity.CellID,
		JobID:         identity.JobID,
		RunID:         identity.RunID,
		ExecutionID:   identity.ExecutionID,
	}

	if identity.X509SVID != nil {
		out.X509SVID = &X509SVID{
			SPIFFEID:     identity.X509SVID.SPIFFEID,
			Certificates: append([]*x509.Certificate(nil), identity.X509SVID.Certificates...),
			PrivateKey:   identity.X509SVID.PrivateKey,
		}
	}

	return out
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

	taskKeys := taskKeyAliases(job, taskKey)
	out := make([]Reference, 0, len(refs))
	for _, ref := range refs {
		if len(ref.TaskKeys) == 0 {
			out = append(out, ref)
			continue
		}

		for _, key := range ref.TaskKeys {
			if _, ok := taskKeys[strings.TrimSpace(key)]; ok {
				out = append(out, ref)
				break
			}
		}
	}

	return out
}

func taskKeyAliases(job *api.Job, taskKey string) map[string]struct{} {
	aliases := map[string]struct{}{}
	taskKey = strings.TrimSpace(taskKey)
	if taskKey != "" {
		aliases[taskKey] = struct{}{}
	}

	if taskKey == rootTaskKey && job != nil && job.GetRoot() != nil {
		if rootID := strings.TrimSpace(job.GetRoot().GetId()); rootID != "" {
			aliases[rootID] = struct{}{}
		}
	}

	return aliases
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
