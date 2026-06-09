package config

import (
	"fmt"

	"vectis/internal/serviceidentity"

	"github.com/spf13/viper"
)

type ServiceIdentityRole int

const (
	ServiceIdentityRoleNone ServiceIdentityRole = iota
	ServiceIdentityRoleRegistry
	ServiceIdentityRoleQueue
	ServiceIdentityRoleLog
	ServiceIdentityRoleArtifact
	ServiceIdentityRoleWorkerControl
	ServiceIdentityRoleSecrets
)

func init() {
	_ = viper.BindEnv(
		"service_identity.registry_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_REGISTRY_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_REGISTRY_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.queue_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_QUEUE_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_QUEUE_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.log_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_LOG_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_LOG_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.artifact_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_ARTIFACT_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_ARTIFACT_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.worker_control_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_WORKER_CONTROL_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.secrets_allowed_client_identities",
		"VECTIS_SERVICE_IDENTITY_SECRETS_ALLOWED_CLIENT_IDENTITIES",
		"VECTIS_SECRETS_ALLOWED_CLIENT_IDENTITIES",
	)

	_ = viper.BindEnv(
		"service_identity.cell_ingress_allowed_producer_identities",
		"VECTIS_SERVICE_IDENTITY_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES",
		"VECTIS_CELL_INGRESS_ALLOWED_PRODUCER_IDENTITIES",
	)
}

func (r ServiceIdentityRole) String() string {
	switch r {
	case ServiceIdentityRoleNone:
		return "none"
	case ServiceIdentityRoleRegistry:
		return "registry"
	case ServiceIdentityRoleQueue:
		return "queue"
	case ServiceIdentityRoleLog:
		return "log"
	case ServiceIdentityRoleArtifact:
		return "artifact"
	case ServiceIdentityRoleWorkerControl:
		return "worker_control"
	case ServiceIdentityRoleSecrets:
		return "secrets"
	default:
		return fmt.Sprintf("unknown_%d", r)
	}
}

func ServiceIdentityAllowedClientIdentities(role ServiceIdentityRole) []string {
	d := MustDefaults().ServiceID
	switch role {
	case ServiceIdentityRoleNone:
		return nil
	case ServiceIdentityRoleRegistry:
		return serviceIdentityList(
			"service_identity.registry_allowed_client_identities",
			d.RegistryAllowedClientIdentities,
		)
	case ServiceIdentityRoleQueue:
		return serviceIdentityList(
			"service_identity.queue_allowed_client_identities",
			d.QueueAllowedClientIdentities,
		)
	case ServiceIdentityRoleLog:
		return serviceIdentityList(
			"service_identity.log_allowed_client_identities",
			d.LogAllowedClientIdentities,
		)
	case ServiceIdentityRoleArtifact:
		return serviceIdentityList(
			"service_identity.artifact_allowed_client_identities",
			d.ArtifactAllowedClientIdentities,
		)
	case ServiceIdentityRoleWorkerControl:
		return serviceIdentityList(
			"service_identity.worker_control_allowed_client_identities",
			d.WorkerControlAllowedClientIdentities,
		)
	case ServiceIdentityRoleSecrets:
		return serviceIdentityList(
			"service_identity.secrets_allowed_client_identities",
			d.SecretsAllowedClientIdentities,
		)
	default:
		return nil
	}
}

func CellIngressAllowedProducerIdentities() []string {
	d := MustDefaults().ServiceID
	return serviceIdentityList(
		"service_identity.cell_ingress_allowed_producer_identities",
		d.CellIngressAllowedProducerIdentities,
	)
}

func validateServiceIdentityAllowlist(label string, identities []string) ([]string, error) {
	normalized, err := serviceidentity.NormalizeSPIFFEAllowlist(identities)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", label, err)
	}

	return normalized, nil
}

func serviceIdentityList(key string, fallback []string) []string {
	return coalesceStringSlices(
		stringSliceFromViper(key),
		fallback,
	)
}
