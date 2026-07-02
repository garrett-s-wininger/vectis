package config

import (
	_ "embed"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"vectis/internal/workloadidentity"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/viper"
)

//go:embed defaults.toml
var defaultsToml string

// NOTE(garrett): This custom type allows us to automatically parse durations from the TOML file.
type tomlDuration time.Duration

func (d *tomlDuration) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		*d = 0
		return nil
	}

	p, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}

	*d = tomlDuration(p)
	return nil
}

type Defaults struct {
	Cell           CellDefaults            `toml:"cell"`
	API            APIDefaults             `toml:"api"`
	Queue          QueueDefaults           `toml:"queue"`
	Orchestrator   OrchestratorDefaults    `toml:"orchestrator"`
	Registry       RegistryDefaults        `toml:"registry"`
	Log            LogDefaults             `toml:"log"`
	Artifact       ArtifactDefaults        `toml:"artifact"`
	LogForwarder   LogForwarderDefaults    `toml:"log_forwarder"`
	Secrets        SecretsDefaults         `toml:"secrets"`
	Discovery      DiscoveryDefaults       `toml:"discovery"`
	Dispatch       DispatchDefaults        `toml:"dispatch"`
	Database       DatabaseDefaults        `toml:"database"`
	Source         SourceDefaults          `toml:"source"`
	Worker         WorkerDefaults          `toml:"worker"`
	WorkerCore     WorkerCoreDefaults      `toml:"worker_core"`
	Cron           CronDefaults            `toml:"cron"`
	Reconciler     ReconcilerDefaults      `toml:"reconciler"`
	Catalog        CatalogDefaults         `toml:"catalog"`
	CellIngress    CellIngressDefaults     `toml:"cell_ingress"`
	ServiceID      ServiceIdentityDefaults `toml:"service_identity"`
	GRPCTLS        GRPCTLSDefaults         `toml:"grpc_tls"`
	MetricsTLS     MetricsTLSDefaults      `toml:"metrics_tls"`
	ActionRegistry ActionRegistryDefaults  `toml:"action_registry"`
}

type CellDefaults struct {
	ID string `toml:"id"`
}

type APIDefaults struct {
	Host                 string               `toml:"host"`
	Port                 int                  `toml:"port"`
	LogFormat            string               `toml:"log_format"`
	RegistryAddress      string               `toml:"registry.address"`
	QueueAddress         string               `toml:"queue.address"`
	LogAddress           string               `toml:"log.address"`
	CellIngressEndpoints []string             `toml:"cell_ingress.endpoints"`
	Auth                 APIAuthDefaults      `toml:"auth"`
	Authz                APIAuthzDefaults     `toml:"authz"`
	Audit                APIAuditDefaults     `toml:"audit"`
	TLS                  APITLSDefaults       `toml:"tls"`
	HSTS                 APIHSTSDefaults      `toml:"hsts"`
	Session              APISessionDefaults   `toml:"session"`
	Cache                APICacheDefaults     `toml:"cache"`
	RateLimit            APIRateLimitDefaults `toml:"rate_limit"`
	ClientIP             APIClientIPDefaults  `toml:"client_ip"`
	HostValidation       APIHostDefaults      `toml:"host_validation"`
	CORS                 APICORSDefaults      `toml:"cors"`
}

type APIHostDefaults struct {
	AllowedHosts []string `toml:"allowed_hosts"`
}

type APIClientIPDefaults struct {
	TrustedProxyCIDRs []string `toml:"trusted_proxy_cidrs"`
}

type APICORSDefaults struct {
	AllowedOrigins []string `toml:"allowed_origins"`
}

type APICacheDefaults struct {
	Backend string `toml:"backend"`
}

type APIRateLimitDefaults struct {
	AuthRefillRate    tomlDuration `toml:"auth_refill_rate"`
	AuthBurstSize     int          `toml:"auth_burst_size"`
	TokenRefillRate   tomlDuration `toml:"token_refill_rate"`
	TokenBurstSize    int          `toml:"token_burst_size"`
	GeneralRefillRate tomlDuration `toml:"general_refill_rate"`
	GeneralBurstSize  int          `toml:"general_burst_size"`
}

type APIAuthDefaults struct {
	Enabled        bool   `toml:"enabled"`
	BootstrapToken string `toml:"bootstrap_token"`
}

type APIAuthzDefaults struct {
	Engine string `toml:"engine"`
}

type APIAuditDefaults struct {
	Enabled             bool   `toml:"enabled"`
	DurabilityOverrides string `toml:"durability_overrides"`
}

type APITLSDefaults struct {
	CertFile       string       `toml:"cert_file"`
	KeyFile        string       `toml:"key_file"`
	ReloadInterval tomlDuration `toml:"reload_interval"`
}

type APIHSTSDefaults struct {
	MaxAgeSeconds     int  `toml:"max_age_seconds"`
	IncludeSubDomains bool `toml:"include_subdomains"`
	Preload           bool `toml:"preload"`
}

type APISessionDefaults struct {
	TTL                  tomlDuration `toml:"ttl"`
	IdleTTL              tomlDuration `toml:"idle_ttl"`
	CookieSecure         bool         `toml:"cookie_secure"`
	AllowInsecureCookies bool         `toml:"allow_insecure_cookies"`
}

type QueueDefaults struct {
	Port                 int    `toml:"port"`
	MetricsHost          string `toml:"metrics_host"`
	MetricsPort          int    `toml:"metrics_port"`
	RegistryAddress      string `toml:"registry.address"`
	ResolverAddress      string `toml:"resolver.address"`
	AdvertiseAddress     string `toml:"advertise_address"`
	RegisterWithRegistry bool   `toml:"register_with_registry"`
}

type OrchestratorDefaults struct {
	Port                 int    `toml:"port"`
	MetricsHost          string `toml:"metrics_host"`
	MetricsPort          int    `toml:"metrics_port"`
	Shards               int    `toml:"shards"`
	RegistryAddress      string `toml:"registry.address"`
	AdvertiseAddress     string `toml:"advertise_address"`
	RegisterWithRegistry bool   `toml:"register_with_registry"`
}

type RegistryDefaults struct {
	Port    int                     `toml:"port"`
	Cluster RegistryClusterDefaults `toml:"cluster"`
}

type RegistryClusterDefaults struct {
	NodeID              string       `toml:"node_id"`
	AdvertiseAddress    string       `toml:"advertise_address"`
	PeerAddresses       []string     `toml:"peer_addresses"`
	GossipInterval      tomlDuration `toml:"gossip_interval"`
	AntiEntropyInterval tomlDuration `toml:"anti_entropy_interval"`
	LeaseTTL            tomlDuration `toml:"lease_ttl"`
	TombstoneTTL        tomlDuration `toml:"tombstone_ttl"`
	PeerDialTimeout     tomlDuration `toml:"peer_dial_timeout"`
}

type ActionRegistryDefaults struct {
	LocalRoots        []string `toml:"local_roots"`
	AllowedNamespaces []string `toml:"allowed_namespaces"`
	AllowedSources    []string `toml:"allowed_sources"`
	RequireDigestPins bool     `toml:"require_digest_pins"`
}

type LogDefaults struct {
	Host                        string       `toml:"host"`
	MetricsHost                 string       `toml:"metrics_host"`
	MetricsPort                 int          `toml:"metrics_port"`
	MaxRunBuffers               int          `toml:"max_run_buffers"`
	StorageReadOnlyMinFreeBytes uint64       `toml:"storage_read_only_min_free_bytes"`
	RegistryAddress             string       `toml:"registry.address"`
	GRPC                        GRPCDefaults `toml:"grpc"`
}

type ArtifactDefaults struct {
	MetricsHost                 string       `toml:"metrics_host"`
	MetricsPort                 int          `toml:"metrics_port"`
	StorageReadOnlyMinFreeBytes uint64       `toml:"storage_read_only_min_free_bytes"`
	RegistryAddress             string       `toml:"registry.address"`
	GRPC                        GRPCDefaults `toml:"grpc"`
}

type LogForwarderDefaults struct {
	MetricsHost string `toml:"metrics_host"`
	MetricsPort int    `toml:"metrics_port"`
}

type SecretsDefaults struct {
	Port            int                   `toml:"port"`
	MetricsHost     string                `toml:"metrics_host"`
	MetricsPort     int                   `toml:"metrics_port"`
	EncryptedFSRoot string                `toml:"encryptedfs.root"`
	EncryptedFSKey  string                `toml:"encryptedfs.key_file"`
	Policy          SecretsPolicyDefaults `toml:"policy"`
}

type SecretsPolicyDefaults struct {
	Allow []string `toml:"allow"`
}

type GRPCDefaults struct {
	Port                 int    `toml:"port"`
	ResolverAddress      string `toml:"resolver.address"`
	AdvertiseAddress     string `toml:"advertise_address"`
	RegisterWithRegistry bool   `toml:"register_with_registry"`
}

type DatabaseDefaults struct {
	Driver  string          `toml:"driver"`
	DSN     string          `toml:"dsn"`
	Pgx     PgxDefaults     `toml:"pgx"`
	PgxPool PgxPoolDefaults `toml:"pgx_pool"`
}

type PgxDefaults struct {
	PlanCacheMode string `toml:"plan_cache_mode"`
}

type SourceDefaults struct {
	CheckoutRoot                             string                        `toml:"checkout_root"`
	SyncConfiguredRepositoriesOnStartup      bool                          `toml:"sync_configured_repositories_on_startup"`
	SyncConfiguredRepositoriesInterval       tomlDuration                  `toml:"sync_configured_repositories_interval"`
	SyncConfiguredRepositoriesMaxConcurrency int                           `toml:"sync_configured_repositories_max_concurrency"`
	SyncConfiguredRepositoriesFailureBackoff tomlDuration                  `toml:"sync_configured_repositories_failure_backoff"`
	SyncRunningTimeout                       tomlDuration                  `toml:"sync_running_timeout"`
	Repositories                             []SourceRepositoryDeclaration `toml:"repositories"`
	Schedules                                []SourceScheduleDeclaration   `toml:"schedules"`
}

type PgxPoolDefaults struct {
	MaxOpenConns    int          `toml:"max_open_conns"`
	MaxIdleConns    int          `toml:"max_idle_conns"`
	ConnMaxLifetime tomlDuration `toml:"conn_max_lifetime"`
	ConnMaxIdleTime tomlDuration `toml:"conn_max_idle_time"`
}

type DiscoveryDefaults struct {
	RegistryAddress              string       `toml:"registry.address"`
	RegistryAddresses            []string     `toml:"registry.addresses"`
	QueueResolverAddress         string       `toml:"queue.resolver.address"`
	LogGRPCResolverAddress       string       `toml:"log.grpc.resolver.address"`
	ArtifactGRPCResolverAddress  string       `toml:"artifact.grpc.resolver.address"`
	QueueAddress                 string       `toml:"queue.address"`
	LogAddress                   string       `toml:"log.address"`
	ArtifactAddress              string       `toml:"artifact.address"`
	OrchestratorAddress          string       `toml:"orchestrator.address"`
	QueueAdvertiseAddress        string       `toml:"queue.advertise.address"`
	LogGRPCAdvertiseAddress      string       `toml:"log.grpc.advertise.address"`
	ArtifactGRPCAdvertiseAddress string       `toml:"artifact.grpc.advertise.address"`
	RegistryResolverRefresh      tomlDuration `toml:"registry_resolver_refresh"`
	RegistryResolverPollTimeout  tomlDuration `toml:"registry_resolver_poll_timeout"`
	RegistryResolverErrorRefresh tomlDuration `toml:"registry_resolver_error_refresh"`
	RegistryRegistrationRefresh  tomlDuration `toml:"registry_registration_refresh"`
}

type DispatchDefaults struct {
	StartTTL tomlDuration `toml:"start_ttl"`
}

type WorkerControlDefaults struct {
	Mode    string `toml:"mode"`
	Port    int    `toml:"port"`
	PortMin int    `toml:"port_min"`
	PortMax int    `toml:"port_max"`
}

type WorkerExecutionDefaults struct {
	Backend                        string                      `toml:"backend"`
	WorkspaceRoot                  string                      `toml:"workspace_root"`
	CheckoutCacheRoot              string                      `toml:"checkout_cache_root"`
	CheckoutCacheGenerationsToKeep int                         `toml:"checkout_cache_generations_to_keep"`
	CheckoutCacheLeaseTTL          tomlDuration                `toml:"checkout_cache_lease_ttl"`
	CheckoutCacheMaxBytes          int64                       `toml:"checkout_cache_max_bytes"`
	CheckoutCacheWarmInterval      tomlDuration                `toml:"checkout_cache_warm_interval"`
	CheckoutCacheWarmTimeout       tomlDuration                `toml:"checkout_cache_warm_timeout"`
	CheckoutCacheWarmJitterRatio   float64                     `toml:"checkout_cache_warm_jitter_ratio"`
	CheckoutCacheWarmParallelism   int                         `toml:"checkout_cache_warm_parallelism"`
	Lima                           WorkerExecutionLimaDefaults `toml:"lima"`
}

type WorkerExecutionLimaDefaults struct {
	Path               string `toml:"path"`
	Instance           string `toml:"instance"`
	GuestWorkspaceRoot string `toml:"guest_workspace_root"`
	Start              bool   `toml:"start"`
	PreserveEnv        bool   `toml:"preserve_env"`
}

type WorkerQueueDefaults struct {
	Address                       string       `toml:"address"`
	DequeuePollBaseInterval       tomlDuration `toml:"dequeue_poll_base_interval"`
	DequeuePollJitterRatio        float64      `toml:"dequeue_poll_jitter_ratio"`
	DequeuePollMaxInterval        tomlDuration `toml:"dequeue_poll_max_interval"`
	DequeueStickySuccessBudget    int          `toml:"dequeue_sticky_success_budget"`
	ContinuationInlineJobMaxBytes int64        `toml:"continuation_inline_job_max_bytes"`
}

type WorkerDefaults struct {
	RegistryAddress      string                          `toml:"registry.address"`
	Queue                WorkerQueueDefaults             `toml:"queue"`
	LogAddress           string                          `toml:"log.address"`
	OrchestratorAddress  string                          `toml:"orchestrator.address"`
	SecretsAddress       string                          `toml:"secrets.address"`
	MetricsHost          string                          `toml:"metrics_host"`
	MetricsPort          int                             `toml:"metrics_port"`
	ArtifactMaxBytes     int64                           `toml:"artifact_max_bytes"`
	ArtifactMaxRunBytes  int64                           `toml:"artifact_max_run_bytes"`
	ArtifactMaxCount     int64                           `toml:"artifact_max_count"`
	Control              WorkerControlDefaults           `toml:"control"`
	Execution            WorkerExecutionDefaults         `toml:"execution"`
	ExecutionIdentity    WorkerExecutionIdentityDefaults `toml:"execution_identity"`
	SPIFFE               WorkerSPIFFEDefaults            `toml:"spiffe"`
	RegisterWithRegistry bool                            `toml:"register_with_registry"`
}

type WorkerCoreDefaults struct {
	MetricsHost string `toml:"metrics_host"`
	MetricsPort int    `toml:"metrics_port"`
}

type WorkerExecutionIdentityDefaults struct {
	Enabled      bool   `toml:"enabled"`
	TrustDomain  string `toml:"trust_domain"`
	PathTemplate string `toml:"path_template"`
}

type WorkerSPIFFEDefaults struct {
	Enabled            bool                             `toml:"enabled"`
	WorkloadAPIAddress string                           `toml:"workload_api_address"`
	FetchTimeout       tomlDuration                     `toml:"fetch_timeout"`
	Registration       WorkerSPIFFERegistrationDefaults `toml:"registration"`
}

type WorkerSPIFFERegistrationDefaults struct {
	Enabled       bool         `toml:"enabled"`
	ServerAddress string       `toml:"server_address"`
	ParentID      string       `toml:"parent_id"`
	Selectors     []string     `toml:"selectors"`
	X509SVIDTTL   tomlDuration `toml:"x509_svid_ttl"`
	MinTTL        tomlDuration `toml:"min_ttl"`
	MaxTTL        tomlDuration `toml:"max_ttl"`
}

type CronDefaults struct {
	RegistryAddress string       `toml:"registry.address"`
	QueueAddress    string       `toml:"queue.address"`
	ClaimTTL        tomlDuration `toml:"claim_ttl"`
}

type ReconcilerDefaults struct {
	RegistryAddress string       `toml:"registry.address"`
	QueueAddress    string       `toml:"queue.address"`
	Interval        tomlDuration `toml:"interval"`
	LeaseTTL        tomlDuration `toml:"lease_ttl"`
	RedispatchLimit int          `toml:"redispatch_limit"`
	MetricsHost     string       `toml:"metrics_host"`
	MetricsPort     int          `toml:"metrics_port"`
}

type CatalogDefaults struct {
	Interval    tomlDuration `toml:"interval"`
	BatchSize   int          `toml:"batch_size"`
	MetricsHost string       `toml:"metrics_host"`
	MetricsPort int          `toml:"metrics_port"`
}

type CellIngressDefaults struct {
	Host            string       `toml:"host"`
	Port            int          `toml:"port"`
	MetricsHost     string       `toml:"metrics_host"`
	MetricsPort     int          `toml:"metrics_port"`
	RepairInterval  tomlDuration `toml:"repair_interval"`
	RegistryAddress string       `toml:"registry.address"`
	QueueAddress    string       `toml:"queue.address"`
}

type ServiceIdentityDefaults struct {
	RegistryAllowedClientIdentities      []string `toml:"registry_allowed_client_identities"`
	QueueAllowedClientIdentities         []string `toml:"queue_allowed_client_identities"`
	LogAllowedClientIdentities           []string `toml:"log_allowed_client_identities"`
	ArtifactAllowedClientIdentities      []string `toml:"artifact_allowed_client_identities"`
	OrchestratorAllowedClientIdentities  []string `toml:"orchestrator_allowed_client_identities"`
	WorkerControlAllowedClientIdentities []string `toml:"worker_control_allowed_client_identities"`
	SecretsAllowedClientIdentities       []string `toml:"secrets_allowed_client_identities"`
	CellIngressAllowedProducerIdentities []string `toml:"cell_ingress_allowed_producer_identities"`
}

type GRPCTLSDefaults struct {
	Insecure       bool         `toml:"insecure"`
	CAFile         string       `toml:"ca_file"`
	CertFile       string       `toml:"cert_file"`
	KeyFile        string       `toml:"key_file"`
	ClientCAFile   string       `toml:"client_ca_file"`
	ClientCertFile string       `toml:"client_cert_file"`
	ClientKeyFile  string       `toml:"client_key_file"`
	ServerName     string       `toml:"server_name"`
	ReloadInterval tomlDuration `toml:"reload_interval"`
}

type MetricsTLSDefaults struct {
	Insecure       bool         `toml:"insecure"`
	CertFile       string       `toml:"cert_file"`
	KeyFile        string       `toml:"key_file"`
	ReloadInterval tomlDuration `toml:"reload_interval"`
}

var (
	once     sync.Once
	cached   Defaults
	parseErr error
)

func init() {
	_ = viper.BindEnv("api.host", "VECTIS_API_SERVER_HOST")
	_ = viper.BindEnv("api.port", "VECTIS_API_SERVER_PORT")
	_ = viper.BindEnv("database.pgx.plan_cache_mode", "VECTIS_DATABASE_PGX_PLAN_CACHE_MODE")
	_ = viper.BindEnv("discovery.registry.address", "VECTIS_DISCOVERY_REGISTRY_ADDRESS")
	_ = viper.BindEnv("discovery.registry.addresses", "VECTIS_DISCOVERY_REGISTRY_ADDRESSES")
	_ = viper.BindEnv("dispatch.start_ttl", "VECTIS_DISPATCH_START_TTL")
	_ = viper.BindEnv("worker.queue.dequeue_poll_base_interval", "VECTIS_WORKER_QUEUE_DEQUEUE_POLL_BASE_INTERVAL")
	_ = viper.BindEnv("worker.queue.dequeue_poll_jitter_ratio", "VECTIS_WORKER_QUEUE_DEQUEUE_POLL_JITTER_RATIO")
	_ = viper.BindEnv("worker.queue.dequeue_poll_max_interval", "VECTIS_WORKER_QUEUE_DEQUEUE_POLL_MAX_INTERVAL")
	_ = viper.BindEnv("worker.queue.dequeue_sticky_success_budget", "VECTIS_WORKER_QUEUE_DEQUEUE_STICKY_SUCCESS_BUDGET")
	_ = viper.BindEnv("worker.queue.continuation_inline_job_max_bytes", "VECTIS_WORKER_QUEUE_CONTINUATION_INLINE_JOB_MAX_BYTES")
	_ = viper.BindEnv("worker.execution.checkout_cache_root", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_ROOT", "VECTIS_WORKER_CORE_CHECKOUT_CACHE_ROOT")
	_ = viper.BindEnv("worker.execution.checkout_cache_generations_to_keep", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_GENERATIONS_TO_KEEP", "VECTIS_WORKER_CORE_CHECKOUT_CACHE_GENERATIONS_TO_KEEP")
	_ = viper.BindEnv("worker.execution.checkout_cache_lease_ttl", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_LEASE_TTL", "VECTIS_WORKER_CORE_CHECKOUT_CACHE_LEASE_TTL")
	_ = viper.BindEnv("worker.execution.checkout_cache_max_bytes", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_MAX_BYTES", "VECTIS_WORKER_CORE_CHECKOUT_CACHE_MAX_BYTES")
	_ = viper.BindEnv("worker.execution.checkout_cache_warm_interval", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_INTERVAL")
	_ = viper.BindEnv("worker.execution.checkout_cache_warm_timeout", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_TIMEOUT")
	_ = viper.BindEnv("worker.execution.checkout_cache_warm_jitter_ratio", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_JITTER_RATIO")
	_ = viper.BindEnv("worker.execution.checkout_cache_warm_parallelism", "VECTIS_WORKER_EXECUTION_CHECKOUT_CACHE_WARM_PARALLELISM", "VECTIS_WORKER_CORE_CHECKOUT_CACHE_WARM_PARALLELISM")
}

func MustDefaults() Defaults {
	once.Do(func() {
		var d Defaults
		if err := toml.Unmarshal([]byte(defaultsToml), &d); err != nil {
			parseErr = fmt.Errorf("parse embedded defaults.toml: %w", err)
			return
		}

		validateDefaults(d)
		cached = d
	})

	if parseErr != nil {
		panic(parseErr)
	}

	return cached
}

func validateDefaults(d Defaults) {
	if strings.TrimSpace(d.Cell.ID) == "" {
		panic("config defaults: cell.id must not be empty")
	}

	if d.API.Host == "" {
		panic("config defaults: api.host must not be empty")
	}

	validatePort := func(port int, name string) {
		if port <= 0 {
			panic(fmt.Sprintf("config defaults: %s must be > 0 (got %d)", name, port))
		}
	}
	validateHost := func(host, name string) {
		if strings.TrimSpace(host) == "" {
			panic(fmt.Sprintf("config defaults: %s must not be empty", name))
		}
	}

	validatePort(d.API.Port, "api.port")
	validatePort(d.Queue.Port, "queue.port")
	validateHost(d.Queue.MetricsHost, "queue.metrics_host")
	validatePort(d.Queue.MetricsPort, "queue.metrics_port")
	if d.Queue.MetricsPort == d.Queue.Port {
		panic("config defaults: queue.metrics_port must differ from queue.port")
	}

	validatePort(d.Orchestrator.Port, "orchestrator.port")
	validateHost(d.Orchestrator.MetricsHost, "orchestrator.metrics_host")
	validatePort(d.Orchestrator.MetricsPort, "orchestrator.metrics_port")
	if d.Orchestrator.MetricsPort == d.Orchestrator.Port {
		panic("config defaults: orchestrator.metrics_port must differ from orchestrator.port")
	}

	if d.Orchestrator.Port == d.API.Port ||
		d.Orchestrator.Port == d.Queue.Port ||
		d.Orchestrator.Port == d.Registry.Port ||
		d.Orchestrator.Port == d.Log.GRPC.Port ||
		d.Orchestrator.Port == d.Artifact.GRPC.Port ||
		d.Orchestrator.Port == d.CellIngress.Port {
		panic("config defaults: orchestrator.port must differ from api/queue/registry/log/artifact/cell ingress ports")
	}

	if d.Orchestrator.MetricsPort == d.Queue.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Worker.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Log.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Artifact.MetricsPort ||
		d.Orchestrator.MetricsPort == d.LogForwarder.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Reconciler.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Catalog.MetricsPort ||
		d.Orchestrator.MetricsPort == d.CellIngress.MetricsPort ||
		d.Orchestrator.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: orchestrator.metrics_port must differ from queue/worker/log/artifact/log-forwarder/reconciler/catalog/cell-ingress metrics ports and worker control port")
	}

	if d.Orchestrator.Shards < 0 {
		panic("config defaults: orchestrator.shards must be >= 0")
	}

	validatePort(d.Registry.Port, "registry.port")
	rc := d.Registry.Cluster
	if time.Duration(rc.GossipInterval) <= 0 {
		panic("config defaults: registry.cluster.gossip_interval must be > 0")
	}

	if time.Duration(rc.AntiEntropyInterval) <= 0 {
		panic("config defaults: registry.cluster.anti_entropy_interval must be > 0")
	}

	if time.Duration(rc.LeaseTTL) <= 0 {
		panic("config defaults: registry.cluster.lease_ttl must be > 0")
	}

	if time.Duration(rc.TombstoneTTL) <= 0 {
		panic("config defaults: registry.cluster.tombstone_ttl must be > 0")
	}

	if time.Duration(rc.PeerDialTimeout) <= 0 {
		panic("config defaults: registry.cluster.peer_dial_timeout must be > 0")
	}

	if time.Duration(d.Dispatch.StartTTL) <= 0 {
		panic("config defaults: dispatch.start_ttl must be > 0")
	}

	validatePort(d.Log.GRPC.Port, "log.grpc.port")
	validateHost(d.Log.MetricsHost, "log.metrics_host")
	validatePort(d.Log.MetricsPort, "log.metrics_port")
	if d.Log.MetricsPort == d.Log.GRPC.Port {
		panic("config defaults: log.metrics_port must differ from log.grpc.port")
	}

	if d.Log.MetricsPort == d.Queue.MetricsPort || d.Log.MetricsPort == d.Worker.MetricsPort {
		panic("config defaults: log.metrics_port must differ from queue.metrics_port and worker.metrics_port")
	}

	validateHost(d.LogForwarder.MetricsHost, "log_forwarder.metrics_host")
	validatePort(d.LogForwarder.MetricsPort, "log_forwarder.metrics_port")
	if d.LogForwarder.MetricsPort == d.Queue.MetricsPort ||
		d.LogForwarder.MetricsPort == d.Worker.MetricsPort ||
		d.LogForwarder.MetricsPort == d.Log.MetricsPort ||
		d.LogForwarder.MetricsPort == d.Log.GRPC.Port {
		panic("config defaults: log_forwarder.metrics_port must differ from queue/worker/log metrics ports and log gRPC port")
	}

	validatePort(d.Secrets.Port, "secrets.port")
	if d.Secrets.Port == d.API.Port ||
		d.Secrets.Port == d.Queue.Port ||
		d.Secrets.Port == d.Registry.Port ||
		d.Secrets.Port == d.Log.GRPC.Port ||
		d.Secrets.Port == d.CellIngress.Port {
		panic("config defaults: secrets.port must differ from api/queue/registry/log/cell_ingress ports")
	}

	validateHost(d.Secrets.MetricsHost, "secrets.metrics_host")
	validatePort(d.Secrets.MetricsPort, "secrets.metrics_port")
	if d.Secrets.MetricsPort == d.Secrets.Port ||
		d.Secrets.MetricsPort == d.Queue.MetricsPort ||
		d.Secrets.MetricsPort == d.Worker.MetricsPort ||
		d.Secrets.MetricsPort == d.Log.MetricsPort ||
		d.Secrets.MetricsPort == d.LogForwarder.MetricsPort ||
		d.Secrets.MetricsPort == d.Reconciler.MetricsPort ||
		d.Secrets.MetricsPort == d.Catalog.MetricsPort ||
		d.Secrets.MetricsPort == d.CellIngress.MetricsPort ||
		d.Secrets.MetricsPort == d.Worker.Control.Port ||
		d.Secrets.MetricsPort == d.Log.GRPC.Port {
		panic("config defaults: secrets.metrics_port must differ from secrets, queue/worker/log/log-forwarder/reconciler/catalog/cell_ingress metrics ports, worker control port, and log gRPC port")
	}

	if d.Log.Host == "" {
		panic("config defaults: log.host must not be empty")
	}

	if d.Database.Driver == "" {
		panic("config defaults: database.driver must not be empty")
	}

	if d.Database.DSN == "" {
		panic("config defaults: database.dsn must not be empty")
	}

	if strings.TrimSpace(d.Source.CheckoutRoot) == "" {
		panic("config defaults: source.checkout_root must not be empty")
	}

	if time.Duration(d.Source.SyncRunningTimeout) <= 0 {
		panic("config defaults: source.sync_running_timeout must be > 0")
	}

	if time.Duration(d.Source.SyncConfiguredRepositoriesInterval) < 0 {
		panic("config defaults: source.sync_configured_repositories_interval must be >= 0")
	}

	if d.Source.SyncConfiguredRepositoriesMaxConcurrency <= 0 {
		panic("config defaults: source.sync_configured_repositories_max_concurrency must be > 0")
	}

	if time.Duration(d.Source.SyncConfiguredRepositoriesFailureBackoff) < 0 {
		panic("config defaults: source.sync_configured_repositories_failure_backoff must be >= 0")
	}

	validateHost(d.Worker.MetricsHost, "worker.metrics_host")
	validatePort(d.Worker.MetricsPort, "worker.metrics_port")
	if d.Worker.ArtifactMaxBytes < 0 {
		panic("config defaults: worker.artifact_max_bytes must be >= 0")
	}

	if d.Worker.ArtifactMaxRunBytes < 0 {
		panic("config defaults: worker.artifact_max_run_bytes must be >= 0")
	}

	if d.Worker.ArtifactMaxCount < 0 {
		panic("config defaults: worker.artifact_max_count must be >= 0")
	}

	if d.Worker.Queue.DequeuePollBaseInterval <= 0 {
		panic("config defaults: worker.queue.dequeue_poll_base_interval must be > 0")
	}

	if d.Worker.Queue.DequeuePollMaxInterval <= 0 {
		panic("config defaults: worker.queue.dequeue_poll_max_interval must be > 0")
	}

	if d.Worker.Queue.DequeuePollMaxInterval < d.Worker.Queue.DequeuePollBaseInterval {
		panic("config defaults: worker.queue.dequeue_poll_max_interval must be >= worker.queue.dequeue_poll_base_interval")
	}

	if d.Worker.Queue.DequeuePollJitterRatio < 0 || d.Worker.Queue.DequeuePollJitterRatio > 1 {
		panic("config defaults: worker.queue.dequeue_poll_jitter_ratio must be between 0 and 1")
	}

	if d.Worker.Queue.DequeueStickySuccessBudget <= 0 {
		panic("config defaults: worker.queue.dequeue_sticky_success_budget must be > 0")
	}
	if d.Worker.Queue.ContinuationInlineJobMaxBytes < 0 {
		panic("config defaults: worker.queue.continuation_inline_job_max_bytes must be >= 0")
	}

	if d.Worker.Execution.CheckoutCacheWarmInterval <= 0 {
		panic("config defaults: worker.execution.checkout_cache_warm_interval must be > 0")
	}

	if d.Worker.Execution.CheckoutCacheGenerationsToKeep <= 0 {
		panic("config defaults: worker.execution.checkout_cache_generations_to_keep must be > 0")
	}

	if d.Worker.Execution.CheckoutCacheLeaseTTL <= 0 {
		panic("config defaults: worker.execution.checkout_cache_lease_ttl must be > 0")
	}

	if d.Worker.Execution.CheckoutCacheMaxBytes < 0 {
		panic("config defaults: worker.execution.checkout_cache_max_bytes must be >= 0")
	}

	if d.Worker.Execution.CheckoutCacheWarmTimeout <= 0 {
		panic("config defaults: worker.execution.checkout_cache_warm_timeout must be > 0")
	}

	if d.Worker.Execution.CheckoutCacheWarmJitterRatio < 0 || d.Worker.Execution.CheckoutCacheWarmJitterRatio > 1 {
		panic("config defaults: worker.execution.checkout_cache_warm_jitter_ratio must be between 0 and 1")
	}

	if d.Worker.Execution.CheckoutCacheWarmParallelism <= 0 {
		panic("config defaults: worker.execution.checkout_cache_warm_parallelism must be > 0")
	}

	if d.Worker.MetricsPort == d.Queue.MetricsPort {
		panic("config defaults: worker.metrics_port must differ from queue.metrics_port")
	}

	if d.Worker.MetricsPort == d.LogForwarder.MetricsPort {
		panic("config defaults: worker.metrics_port must differ from log_forwarder.metrics_port")
	}

	validateHost(d.WorkerCore.MetricsHost, "worker_core.metrics_host")
	validatePort(d.WorkerCore.MetricsPort, "worker_core.metrics_port")
	if d.WorkerCore.MetricsPort == d.Queue.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Orchestrator.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Worker.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Log.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Artifact.MetricsPort ||
		d.WorkerCore.MetricsPort == d.LogForwarder.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Reconciler.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Catalog.MetricsPort ||
		d.WorkerCore.MetricsPort == d.CellIngress.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Secrets.MetricsPort ||
		d.WorkerCore.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: worker_core.metrics_port must differ from queue/orchestrator/worker/log/artifact/log-forwarder/reconciler/catalog/cell-ingress/secrets metrics ports and worker control port")
	}

	if strings.TrimSpace(d.Worker.ExecutionIdentity.PathTemplate) == "" {
		panic("config defaults: worker.execution_identity.path_template must not be empty")
	}

	if d.Worker.ExecutionIdentity.Enabled && strings.TrimSpace(d.Worker.ExecutionIdentity.TrustDomain) == "" {
		panic("config defaults: worker.execution_identity.trust_domain must not be empty when enabled")
	}

	defaultTrustDomain := strings.TrimSpace(d.Worker.ExecutionIdentity.TrustDomain)
	if defaultTrustDomain == "" {
		defaultTrustDomain = "example.invalid"
	}
	if _, err := workloadidentity.SPIFFEID(defaultTrustDomain, d.Worker.ExecutionIdentity.PathTemplate, workloadidentity.Execution{
		CellID:            "local",
		NamespacePath:     "/",
		JobID:             "job",
		RunID:             "run",
		RunIndex:          1,
		SegmentID:         "segment",
		ExecutionID:       "execution",
		Attempt:           1,
		DefinitionVersion: 1,
		DefinitionHash:    "sha256:sample",
	}); err != nil {
		panic("config defaults: worker.execution_identity: " + err.Error())
	}

	if d.Worker.SPIFFE.Enabled && strings.TrimSpace(d.Worker.SPIFFE.WorkloadAPIAddress) == "" {
		panic("config defaults: worker.spiffe.workload_api_address must not be empty when enabled")
	}

	if d.Worker.SPIFFE.Enabled && !d.Worker.ExecutionIdentity.Enabled {
		panic("config defaults: worker.spiffe.enabled requires worker.execution_identity.enabled")
	}

	if d.Worker.SPIFFE.FetchTimeout <= 0 {
		panic("config defaults: worker.spiffe.fetch_timeout must be > 0")
	}

	p := d.Database.PgxPool
	if p.MaxOpenConns <= 0 {
		panic("config defaults: database.pgx_pool.max_open_conns must be > 0")
	}

	if p.MaxIdleConns < 0 {
		panic("config defaults: database.pgx_pool.max_idle_conns must be >= 0")
	}

	if p.MaxIdleConns > p.MaxOpenConns {
		panic("config defaults: database.pgx_pool.max_idle_conns must be <= max_open_conns")
	}

	if time.Duration(d.Cron.ClaimTTL) <= 0 {
		panic("config defaults: cron.claim_ttl must be > 0")
	}

	if time.Duration(d.Reconciler.Interval) <= 0 {
		panic("config defaults: reconciler.interval must be > 0")
	}

	if time.Duration(d.Reconciler.LeaseTTL) <= 0 {
		panic("config defaults: reconciler.lease_ttl must be > 0")
	}

	if d.Reconciler.RedispatchLimit <= 0 {
		panic("config defaults: reconciler.redispatch_limit must be > 0")
	}

	validateHost(d.Reconciler.MetricsHost, "reconciler.metrics_host")
	validatePort(d.Reconciler.MetricsPort, "reconciler.metrics_port")
	if d.Reconciler.MetricsPort == d.Queue.MetricsPort ||
		d.Reconciler.MetricsPort == d.Worker.MetricsPort ||
		d.Reconciler.MetricsPort == d.Log.MetricsPort ||
		d.Reconciler.MetricsPort == d.LogForwarder.MetricsPort ||
		d.Reconciler.MetricsPort == d.Secrets.MetricsPort ||
		d.Reconciler.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: reconciler.metrics_port must differ from queue/worker/log/log-forwarder/secrets metrics ports and worker control port")
	}

	if time.Duration(d.Catalog.Interval) <= 0 {
		panic("config defaults: catalog.interval must be > 0")
	}

	if d.Catalog.BatchSize <= 0 {
		panic("config defaults: catalog.batch_size must be > 0")
	}

	validateHost(d.Catalog.MetricsHost, "catalog.metrics_host")
	validatePort(d.Catalog.MetricsPort, "catalog.metrics_port")
	if d.Catalog.MetricsPort == d.Queue.MetricsPort ||
		d.Catalog.MetricsPort == d.Worker.MetricsPort ||
		d.Catalog.MetricsPort == d.Log.MetricsPort ||
		d.Catalog.MetricsPort == d.LogForwarder.MetricsPort ||
		d.Catalog.MetricsPort == d.Reconciler.MetricsPort ||
		d.Catalog.MetricsPort == d.Secrets.MetricsPort ||
		d.Catalog.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: catalog.metrics_port must differ from queue/worker/log/log-forwarder/reconciler/secrets metrics ports and worker control port")
	}

	if strings.TrimSpace(d.CellIngress.Host) == "" {
		panic("config defaults: cell_ingress.host must not be empty")
	}
	validatePort(d.CellIngress.Port, "cell_ingress.port")
	if d.CellIngress.Port == d.API.Port ||
		d.CellIngress.Port == d.Queue.Port ||
		d.CellIngress.Port == d.Registry.Port ||
		d.CellIngress.Port == d.Log.GRPC.Port ||
		d.CellIngress.Port == d.Secrets.Port {
		panic("config defaults: cell_ingress.port must differ from api/queue/registry/log/secrets ports")
	}

	validateHost(d.CellIngress.MetricsHost, "cell_ingress.metrics_host")
	validatePort(d.CellIngress.MetricsPort, "cell_ingress.metrics_port")
	if d.CellIngress.MetricsPort == d.CellIngress.Port ||
		d.CellIngress.MetricsPort == d.Queue.MetricsPort ||
		d.CellIngress.MetricsPort == d.Worker.MetricsPort ||
		d.CellIngress.MetricsPort == d.Log.MetricsPort ||
		d.CellIngress.MetricsPort == d.LogForwarder.MetricsPort ||
		d.CellIngress.MetricsPort == d.Reconciler.MetricsPort ||
		d.CellIngress.MetricsPort == d.Catalog.MetricsPort ||
		d.CellIngress.MetricsPort == d.Secrets.MetricsPort ||
		d.CellIngress.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: cell_ingress.metrics_port must differ from cell ingress, queue/worker/log/log-forwarder/reconciler/catalog/secrets metrics ports and worker control port")
	}

	if time.Duration(d.CellIngress.RepairInterval) <= 0 {
		panic("config defaults: cell_ingress.repair_interval must be > 0")
	}

	if strings.TrimSpace(d.API.Auth.BootstrapToken) != "" && len(strings.TrimSpace(d.API.Auth.BootstrapToken)) < 16 {
		panic("config defaults: api.auth.bootstrap_token when set must be at least 16 characters")
	}

	e := strings.ToLower(strings.TrimSpace(d.API.Authz.Engine))
	if e == "" {
		e = "hierarchical_rbac"
	}

	if e != "authenticated_full" && e != "hierarchical_rbac" {
		panic("config defaults: api.authz.engine must be authenticated_full or hierarchical_rbac (got " + d.API.Authz.Engine + ")")
	}

	if b := normalizeAPICacheBackend(d.API.Cache.Backend); b != APICacheBackendDatabase && b != APICacheBackendMemory {
		panic("config defaults: api.cache.backend must be database or memory (got " + d.API.Cache.Backend + ")")
	}

	for _, host := range d.API.HostValidation.AllowedHosts {
		if _, err := parseAPIAllowedHost(host); err != nil {
			panic("config defaults: " + err.Error())
		}
	}

	for _, origin := range d.API.CORS.AllowedOrigins {
		if _, err := parseCORSOrigin(origin); err != nil {
			panic("config defaults: " + err.Error())
		}
	}

	if time.Duration(d.API.Session.TTL) <= 0 {
		panic("config defaults: api.session.ttl must be > 0")
	}

	if time.Duration(d.API.Session.IdleTTL) <= 0 {
		panic("config defaults: api.session.idle_ttl must be > 0")
	}

	if time.Duration(d.API.Session.IdleTTL) > time.Duration(d.API.Session.TTL) {
		panic("config defaults: api.session.idle_ttl must be <= api.session.ttl")
	}

	if d.API.HSTS.MaxAgeSeconds < 0 {
		panic("config defaults: api.hsts.max_age_seconds must be >= 0")
	}

	if d.API.HSTS.Preload && !d.API.HSTS.IncludeSubDomains {
		panic("config defaults: api.hsts.preload requires api.hsts.include_subdomains=true")
	}

	if d.API.HSTS.Preload && d.API.HSTS.MaxAgeSeconds < apiHSTSPreloadMinMaxAgeSeconds {
		panic(fmt.Sprintf("config defaults: api.hsts.preload requires api.hsts.max_age_seconds >= %d", apiHSTSPreloadMinMaxAgeSeconds))
	}

	rl := d.API.RateLimit
	if time.Duration(rl.AuthRefillRate) <= 0 {
		panic("config defaults: api.rate_limit.auth_refill_rate must be > 0")
	}

	if rl.AuthBurstSize <= 0 {
		panic("config defaults: api.rate_limit.auth_burst_size must be > 0")
	}

	if time.Duration(rl.TokenRefillRate) <= 0 {
		panic("config defaults: api.rate_limit.token_refill_rate must be > 0")
	}

	if rl.TokenBurstSize <= 0 {
		panic("config defaults: api.rate_limit.token_burst_size must be > 0")
	}

	if time.Duration(rl.GeneralRefillRate) <= 0 {
		panic("config defaults: api.rate_limit.general_refill_rate must be > 0")
	}

	if rl.GeneralBurstSize <= 0 {
		panic("config defaults: api.rate_limit.general_burst_size must be > 0")
	}

	for name, identities := range map[string][]string{
		"registry_allowed_client_identities":       d.ServiceID.RegistryAllowedClientIdentities,
		"queue_allowed_client_identities":          d.ServiceID.QueueAllowedClientIdentities,
		"log_allowed_client_identities":            d.ServiceID.LogAllowedClientIdentities,
		"artifact_allowed_client_identities":       d.ServiceID.ArtifactAllowedClientIdentities,
		"orchestrator_allowed_client_identities":   d.ServiceID.OrchestratorAllowedClientIdentities,
		"worker_control_allowed_client_identities": d.ServiceID.WorkerControlAllowedClientIdentities,
		"secrets_allowed_client_identities":        d.ServiceID.SecretsAllowedClientIdentities,
		"cell_ingress_allowed_producer_identities": d.ServiceID.CellIngressAllowedProducerIdentities,
	} {
		if _, err := validateServiceIdentityAllowlist("config defaults: service_identity."+name, identities); err != nil {
			panic(err.Error())
		}
	}
}

func coalesceNonEmpty(strs ...string) string {
	for _, s := range strs {
		if s != "" {
			return s
		}
	}
	return ""
}

func coalesceStringSlices(slices ...[]string) []string {
	for _, ss := range slices {
		cleaned := cleanStringSlice(ss)
		if len(cleaned) > 0 {
			return cleaned
		}
	}

	return nil
}

func cleanStringSlice(ss []string) []string {
	if len(ss) == 0 {
		return nil
	}

	out := make([]string, 0, len(ss))
	seen := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		for part := range strings.SplitSeq(s, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}

			if _, ok := seen[part]; ok {
				continue
			}

			seen[part] = struct{}{}
			out = append(out, part)
		}
	}

	return out
}

func stringSliceFromViper(key string) []string {
	if !viper.IsSet(key) {
		return nil
	}

	return cleanStringSlice(append(viper.GetStringSlice(key), viper.GetString(key)))
}

func PublicHost() string {
	host := APIHost()
	if isUnspecifiedAPIHost(host) {
		return MustDefaults().API.Host
	}

	return host
}

func APIHost() string {
	if host := strings.TrimSpace(viper.GetString("host")); host != "" {
		return host
	}

	if host := strings.TrimSpace(viper.GetString("api.host")); host != "" {
		return host
	}

	return MustDefaults().API.Host
}

func APIPort() int {
	return MustDefaults().API.Port
}

func metricsHost(flatKey, namespacedKey, fallback string) string {
	if host := strings.TrimSpace(viper.GetString(flatKey)); host != "" {
		return host
	}

	if host := strings.TrimSpace(viper.GetString(namespacedKey)); host != "" {
		return host
	}

	if host := strings.TrimSpace(fallback); host != "" {
		return host
	}

	return "localhost"
}

func metricsListenAddr(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func QueuePort() int {
	return MustDefaults().Queue.Port
}

func QueueMetricsHost() string {
	return metricsHost("metrics_host", "queue.metrics_host", MustDefaults().Queue.MetricsHost)
}

func OrchestratorPort() int {
	return MustDefaults().Orchestrator.Port
}

func QueueMetricsPort() int {
	return MustDefaults().Queue.MetricsPort
}

func QueueMetricsListenAddr() string {
	return metricsListenAddr(QueueMetricsHost(), QueueMetricsEffectiveListenPort())
}

func WorkerMetricsHost() string {
	return metricsHost("metrics_host", "worker.metrics_host", MustDefaults().Worker.MetricsHost)
}

func OrchestratorMetricsPort() int {
	return MustDefaults().Orchestrator.MetricsPort
}

func OrchestratorMetricsHost() string {
	return metricsHost("metrics_host", "orchestrator.metrics_host", MustDefaults().Orchestrator.MetricsHost)
}

func OrchestratorShards() int {
	if viper.IsSet("shards") {
		return viper.GetInt("shards")
	}

	if viper.IsSet("orchestrator.shards") {
		return viper.GetInt("orchestrator.shards")
	}

	return MustDefaults().Orchestrator.Shards
}

func WorkerMetricsPort() int {
	return MustDefaults().Worker.MetricsPort
}

func WorkerMetricsListenAddr() string {
	return metricsListenAddr(WorkerMetricsHost(), WorkerMetricsEffectiveListenPort())
}

func WorkerCoreMetricsHost() string {
	return metricsHost("metrics_host", "worker_core.metrics_host", MustDefaults().WorkerCore.MetricsHost)
}

func WorkerCoreMetricsPort() int {
	return MustDefaults().WorkerCore.MetricsPort
}

func WorkerCoreMetricsListenAddr() string {
	return metricsListenAddr(WorkerCoreMetricsHost(), WorkerCoreMetricsEffectiveListenPort())
}

func WorkerArtifactMaxBytes() int64 {
	if viper.IsSet("worker.artifact_max_bytes") {
		return nonNegativeInt64(viper.GetInt64("worker.artifact_max_bytes"))
	}

	return MustDefaults().Worker.ArtifactMaxBytes
}

func WorkerArtifactMaxRunBytes() int64 {
	if viper.IsSet("worker.artifact_max_run_bytes") {
		return nonNegativeInt64(viper.GetInt64("worker.artifact_max_run_bytes"))
	}

	return MustDefaults().Worker.ArtifactMaxRunBytes
}

func WorkerArtifactMaxCount() int64 {
	if viper.IsSet("worker.artifact_max_count") {
		return nonNegativeInt64(viper.GetInt64("worker.artifact_max_count"))
	}

	return MustDefaults().Worker.ArtifactMaxCount
}

func WorkerQueueDequeueStickySuccessBudget() int {
	if viper.IsSet("worker.queue.dequeue_sticky_success_budget") {
		if value := viper.GetInt("worker.queue.dequeue_sticky_success_budget"); value > 0 {
			return value
		}
	}

	return MustDefaults().Worker.Queue.DequeueStickySuccessBudget
}

func WorkerQueueContinuationInlineJobMaxBytes() int64 {
	if viper.IsSet("worker.queue.continuation_inline_job_max_bytes") {
		return nonNegativeInt64(viper.GetInt64("worker.queue.continuation_inline_job_max_bytes"))
	}

	return MustDefaults().Worker.Queue.ContinuationInlineJobMaxBytes
}

func WorkerQueueDequeuePollBaseInterval() time.Duration {
	if viper.IsSet("worker.queue.dequeue_poll_base_interval") {
		if value := viper.GetDuration("worker.queue.dequeue_poll_base_interval"); value > 0 {
			return value
		}
	}

	return time.Duration(MustDefaults().Worker.Queue.DequeuePollBaseInterval)
}

func WorkerQueueDequeuePollJitterRatio() float64 {
	if viper.IsSet("worker.queue.dequeue_poll_jitter_ratio") {
		if value := viper.GetFloat64("worker.queue.dequeue_poll_jitter_ratio"); value >= 0 && value <= 1 {
			return value
		}
	}

	return MustDefaults().Worker.Queue.DequeuePollJitterRatio
}

func WorkerQueueDequeuePollMaxInterval() time.Duration {
	base := WorkerQueueDequeuePollBaseInterval()
	if viper.IsSet("worker.queue.dequeue_poll_max_interval") {
		if value := viper.GetDuration("worker.queue.dequeue_poll_max_interval"); value >= base {
			return value
		}
	}

	value := time.Duration(MustDefaults().Worker.Queue.DequeuePollMaxInterval)
	if value < base {
		return base
	}

	return value
}

func WorkerControlMode() string {
	mode := viper.GetString("worker.control.mode")
	if mode != "" {
		return mode
	}
	return MustDefaults().Worker.Control.Mode
}

func WorkerControlPort() int {
	if p := viper.GetInt("control_port"); p > 0 {
		return p
	}
	return MustDefaults().Worker.Control.Port
}

func WorkerControlPortMin() int {
	if p := viper.GetInt("control_port_min"); p > 0 {
		return p
	}
	return MustDefaults().Worker.Control.PortMin
}

func WorkerControlPortMax() int {
	if p := viper.GetInt("control_port_max"); p > 0 {
		return p
	}
	return MustDefaults().Worker.Control.PortMax
}

func WorkerRegisterWithRegistry() bool {
	if viper.IsSet("worker.register_with_registry") {
		return viper.GetBool("worker.register_with_registry")
	}

	return MustDefaults().Worker.RegisterWithRegistry
}

func WorkerExecutionBackend() string {
	backend := strings.TrimSpace(viper.GetString("worker.execution.backend"))
	if backend == "" {
		backend = MustDefaults().Worker.Execution.Backend
	}
	return strings.ToLower(strings.TrimSpace(backend))
}

func WorkerExecutionWorkspaceRoot() string {
	if viper.IsSet("worker.execution.workspace_root") {
		return strings.TrimSpace(viper.GetString("worker.execution.workspace_root"))
	}
	return MustDefaults().Worker.Execution.WorkspaceRoot
}

func WorkerExecutionCheckoutCacheRoot() string {
	if viper.IsSet("worker.execution.checkout_cache_root") {
		return strings.TrimSpace(viper.GetString("worker.execution.checkout_cache_root"))
	}
	return MustDefaults().Worker.Execution.CheckoutCacheRoot
}

func WorkerExecutionCheckoutCacheGenerationsToKeep() int {
	if viper.IsSet("worker.execution.checkout_cache_generations_to_keep") {
		if value := viper.GetInt("worker.execution.checkout_cache_generations_to_keep"); value > 0 {
			return value
		}
	}

	return MustDefaults().Worker.Execution.CheckoutCacheGenerationsToKeep
}

func WorkerExecutionCheckoutCacheLeaseTTL() time.Duration {
	if viper.IsSet("worker.execution.checkout_cache_lease_ttl") {
		if value := viper.GetDuration("worker.execution.checkout_cache_lease_ttl"); value > 0 {
			return value
		}
	}

	return time.Duration(MustDefaults().Worker.Execution.CheckoutCacheLeaseTTL)
}

func WorkerExecutionCheckoutCacheMaxBytes() int64 {
	if viper.IsSet("worker.execution.checkout_cache_max_bytes") {
		if value := viper.GetInt64("worker.execution.checkout_cache_max_bytes"); value >= 0 {
			return value
		}
	}

	return MustDefaults().Worker.Execution.CheckoutCacheMaxBytes
}

func WorkerExecutionCheckoutCacheWarmInterval() time.Duration {
	if viper.IsSet("worker.execution.checkout_cache_warm_interval") {
		if value := viper.GetDuration("worker.execution.checkout_cache_warm_interval"); value > 0 {
			return value
		}
	}

	return time.Duration(MustDefaults().Worker.Execution.CheckoutCacheWarmInterval)
}

func WorkerExecutionCheckoutCacheWarmTimeout() time.Duration {
	if viper.IsSet("worker.execution.checkout_cache_warm_timeout") {
		if value := viper.GetDuration("worker.execution.checkout_cache_warm_timeout"); value > 0 {
			return value
		}
	}

	return time.Duration(MustDefaults().Worker.Execution.CheckoutCacheWarmTimeout)
}

func WorkerExecutionCheckoutCacheWarmJitterRatio() float64 {
	if viper.IsSet("worker.execution.checkout_cache_warm_jitter_ratio") {
		if value := viper.GetFloat64("worker.execution.checkout_cache_warm_jitter_ratio"); value >= 0 && value <= 1 {
			return value
		}
	}

	return MustDefaults().Worker.Execution.CheckoutCacheWarmJitterRatio
}

func WorkerExecutionCheckoutCacheWarmParallelism() int {
	if viper.IsSet("worker.execution.checkout_cache_warm_parallelism") {
		if value := viper.GetInt("worker.execution.checkout_cache_warm_parallelism"); value > 0 {
			return value
		}
	}

	return MustDefaults().Worker.Execution.CheckoutCacheWarmParallelism
}

func WorkerExecutionLimaPath() string {
	path := strings.TrimSpace(viper.GetString("worker.execution.lima.path"))
	if path != "" {
		return path
	}
	return MustDefaults().Worker.Execution.Lima.Path
}

func WorkerExecutionLimaInstance() string {
	if viper.IsSet("worker.execution.lima.instance") {
		return strings.TrimSpace(viper.GetString("worker.execution.lima.instance"))
	}
	return MustDefaults().Worker.Execution.Lima.Instance
}

func WorkerExecutionLimaGuestWorkspaceRoot() string {
	if viper.IsSet("worker.execution.lima.guest_workspace_root") {
		return strings.TrimSpace(viper.GetString("worker.execution.lima.guest_workspace_root"))
	}
	return MustDefaults().Worker.Execution.Lima.GuestWorkspaceRoot
}

func WorkerExecutionLimaStart() bool {
	if viper.IsSet("worker.execution.lima.start") {
		return viper.GetBool("worker.execution.lima.start")
	}
	return MustDefaults().Worker.Execution.Lima.Start
}

func WorkerExecutionLimaPreserveEnv() bool {
	if viper.IsSet("worker.execution.lima.preserve_env") {
		return viper.GetBool("worker.execution.lima.preserve_env")
	}
	return MustDefaults().Worker.Execution.Lima.PreserveEnv
}

func DispatchStartTTL() time.Duration {
	if d := viper.GetDuration("dispatch.start_ttl"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Dispatch.StartTTL)
	if d > 0 {
		return d
	}

	return 24 * time.Hour
}

func nonNegativeInt64(v int64) int64 {
	if v < 0 {
		return 0
	}

	return v
}

func RegistryPort() int {
	return MustDefaults().Registry.Port
}

func RegistryClusterNodeID() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("registry.cluster.node_id"),
		d.Registry.Cluster.NodeID,
	)
}

func RegistryClusterAdvertiseAddress(bindListenAddr string) string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("registry.cluster.advertise_address"),
		d.Registry.Cluster.AdvertiseAddress,
		bindListenAddr,
	)
}

func RegistryClusterPeerAddresses() []string {
	d := MustDefaults()
	return coalesceStringSlices(
		stringSliceFromViper("registry.cluster.peer_addresses"),
		d.Registry.Cluster.PeerAddresses,
	)
}

func RegistryClusterGossipInterval() time.Duration {
	if d := viper.GetDuration("registry.cluster.gossip_interval"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Registry.Cluster.GossipInterval)
	if d > 0 {
		return d
	}

	return 2 * time.Second
}

func RegistryClusterAntiEntropyInterval() time.Duration {
	if d := viper.GetDuration("registry.cluster.anti_entropy_interval"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Registry.Cluster.AntiEntropyInterval)
	if d > 0 {
		return d
	}

	return 30 * time.Second
}

func RegistryClusterLeaseTTL() time.Duration {
	if d := viper.GetDuration("registry.cluster.lease_ttl"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Registry.Cluster.LeaseTTL)
	if d > 0 {
		return d
	}

	return 2 * time.Minute
}

func RegistryClusterTombstoneTTL() time.Duration {
	if d := viper.GetDuration("registry.cluster.tombstone_ttl"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Registry.Cluster.TombstoneTTL)
	if d > 0 {
		return d
	}

	return 5 * time.Minute
}

func RegistryClusterPeerDialTimeout() time.Duration {
	if d := viper.GetDuration("registry.cluster.peer_dial_timeout"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Registry.Cluster.PeerDialTimeout)
	if d > 0 {
		return d
	}

	return 3 * time.Second
}

func LogGRPCPort() int {
	if p := viper.GetInt("grpc_port"); p > 0 {
		return p
	}

	if p := viper.GetInt("log.grpc.port"); p > 0 {
		return p
	}

	return MustDefaults().Log.GRPC.Port
}

func ArtifactGRPCPort() int {
	if p := viper.GetInt("grpc_port"); p > 0 {
		return p
	}

	if p := viper.GetInt("artifact.grpc.port"); p > 0 {
		return p
	}

	return MustDefaults().Artifact.GRPC.Port
}

func LogMetricsHost() string {
	return metricsHost("metrics_host", "log.metrics_host", MustDefaults().Log.MetricsHost)
}

func LogMetricsPort() int {
	return MustDefaults().Log.MetricsPort
}

func LogMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	return LogMetricsPort()
}

func LogMetricsListenAddr() string {
	return metricsListenAddr(LogMetricsHost(), LogMetricsEffectiveListenPort())
}

func ArtifactMetricsHost() string {
	return metricsHost("metrics_host", "artifact.metrics_host", MustDefaults().Artifact.MetricsHost)
}

func ArtifactMetricsPort() int {
	return MustDefaults().Artifact.MetricsPort
}

func ArtifactMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	return ArtifactMetricsPort()
}

func ArtifactMetricsListenAddr() string {
	return metricsListenAddr(ArtifactMetricsHost(), ArtifactMetricsEffectiveListenPort())
}

func LogForwarderMetricsHost() string {
	return metricsHost("metrics_host", "log_forwarder.metrics_host", MustDefaults().LogForwarder.MetricsHost)
}

func LogForwarderMetricsPort() int {
	return MustDefaults().LogForwarder.MetricsPort
}

func LogForwarderMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	return LogForwarderMetricsPort()
}

func LogForwarderMetricsListenAddr() string {
	return metricsListenAddr(LogForwarderMetricsHost(), LogForwarderMetricsEffectiveListenPort())
}

func SecretsPort() int {
	return MustDefaults().Secrets.Port
}

func SecretsEffectiveListenPort() int {
	return effectiveListenPort(SecretsPort)
}

func SecretsListenAddr() string {
	return ":" + strconv.Itoa(SecretsEffectiveListenPort())
}

func SecretsMetricsHost() string {
	return metricsHost("metrics_host", "secrets.metrics_host", MustDefaults().Secrets.MetricsHost)
}

func SecretsMetricsPort() int {
	return MustDefaults().Secrets.MetricsPort
}

func SecretsMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	if p := viper.GetInt("secrets.metrics_port"); p > 0 {
		return p
	}

	return SecretsMetricsPort()
}

func SecretsMetricsListenAddr() string {
	return metricsListenAddr(SecretsMetricsHost(), SecretsMetricsEffectiveListenPort())
}

func SecretsEncryptedFSRoot() string {
	return coalesceNonEmpty(
		viper.GetString("encryptedfs_root"),
		viper.GetString("secrets.encryptedfs.root"),
		MustDefaults().Secrets.EncryptedFSRoot,
	)
}

func SecretsEncryptedFSKeyFile() string {
	return coalesceNonEmpty(
		viper.GetString("encryptedfs_key_file"),
		viper.GetString("secrets.encryptedfs.key_file"),
		MustDefaults().Secrets.EncryptedFSKey,
	)
}

func SecretsPolicyAllowRules() []string {
	defaults := MustDefaults().Secrets.Policy.Allow
	if viper.IsSet("policy_allow") || viper.IsSet("secrets.policy.allow") {
		return coalesceStringSlices(
			append(viper.GetStringSlice("policy_allow"), viper.GetString("policy_allow")),
			append(viper.GetStringSlice("secrets.policy.allow"), viper.GetString("secrets.policy.allow")),
		)
	}

	return cleanStringSlice(defaults)
}

func LogMaxRunBuffers() int {
	if viper.IsSet("max_run_buffers") {
		if n := viper.GetInt("max_run_buffers"); n > 0 {
			return n
		}
	}

	return MustDefaults().Log.MaxRunBuffers
}

func LogStorageReadOnlyMinFreeBytes() uint64 {
	if viper.IsSet("storage_read_only_min_free_bytes") {
		return viper.GetUint64("storage_read_only_min_free_bytes")
	}

	return MustDefaults().Log.StorageReadOnlyMinFreeBytes
}

func ArtifactStorageReadOnlyMinFreeBytes() uint64 {
	if viper.IsSet("storage_read_only_min_free_bytes") {
		return viper.GetUint64("storage_read_only_min_free_bytes")
	}

	return MustDefaults().Artifact.StorageReadOnlyMinFreeBytes
}

func APIListenAddr() string {
	return net.JoinHostPort(APIHost(), strconv.Itoa(APIPort()))
}

func QueueListenAddr() string {
	return ":" + strconv.Itoa(QueuePort())
}

func RegistryListenAddr() string {
	return ":" + strconv.Itoa(RegistryPort())
}

func LogGRPCListenAddr() string {
	return ":" + strconv.Itoa(LogGRPCPort())
}

func ArtifactGRPCListenAddr() string {
	return ":" + strconv.Itoa(ArtifactGRPCPort())
}

func PublicAPIBaseURL() string {
	return fmt.Sprintf("http://%s:%d", PublicHost(), APIEffectiveListenPort())
}

func DBDriver() string {
	return MustDefaults().Database.Driver
}

func DBDSN(dataHome string) string {
	return strings.NewReplacer(
		"{{data_home}}", dataHome,
	).Replace(MustDefaults().Database.DSN)
}

func DatabasePgxPoolMaxOpenConns() int {
	if viper.IsSet("database.pgx_pool.max_open_conns") {
		return viper.GetInt("database.pgx_pool.max_open_conns")
	}

	return MustDefaults().Database.PgxPool.MaxOpenConns
}

func DatabasePgxPlanCacheMode() string {
	if viper.IsSet("database.pgx.plan_cache_mode") {
		return strings.TrimSpace(viper.GetString("database.pgx.plan_cache_mode"))
	}

	return strings.TrimSpace(MustDefaults().Database.Pgx.PlanCacheMode)
}

func DatabasePgxPoolMaxIdleConns() int {
	if viper.IsSet("database.pgx_pool.max_idle_conns") {
		return viper.GetInt("database.pgx_pool.max_idle_conns")
	}

	return MustDefaults().Database.PgxPool.MaxIdleConns
}

func DatabasePgxConnMaxLifetime() time.Duration {
	if viper.IsSet("database.pgx_pool.conn_max_lifetime") {
		return viper.GetDuration("database.pgx_pool.conn_max_lifetime")
	}

	d := time.Duration(MustDefaults().Database.PgxPool.ConnMaxLifetime)
	if d > 0 {
		return d
	}

	return time.Hour
}

func DatabasePgxConnMaxIdleTime() time.Duration {
	if viper.IsSet("database.pgx_pool.conn_max_idle_time") {
		return viper.GetDuration("database.pgx_pool.conn_max_idle_time")
	}

	d := time.Duration(MustDefaults().Database.PgxPool.ConnMaxIdleTime)
	if d > 0 {
		return d
	}

	return 15 * time.Minute
}

func RegistryResolverPollInterval() time.Duration {
	if d := viper.GetDuration("discovery.registry_resolver_refresh"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Discovery.RegistryResolverRefresh)
	if d > 0 {
		return d
	}

	return 10 * time.Second
}

func RegistryResolverPollTimeout() time.Duration {
	if d := viper.GetDuration("discovery.registry_resolver_poll_timeout"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Discovery.RegistryResolverPollTimeout)
	if d > 0 {
		return d
	}

	return 5 * time.Second
}

func RegistryResolverErrorRefresh() time.Duration {
	if d := viper.GetDuration("discovery.registry_resolver_error_refresh"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Discovery.RegistryResolverErrorRefresh)
	if d > 0 {
		return d
	}

	return 2 * time.Second
}

func RegistryRegistrationRefresh() time.Duration {
	if d := viper.GetDuration("discovery.registry_registration_refresh"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Discovery.RegistryRegistrationRefresh)
	if d > 0 {
		return d
	}

	return 45 * time.Second
}

func QueueGRPCAdvertiseAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("queue.advertise_address"),
		d.Queue.AdvertiseAddress,
		viper.GetString("discovery.queue.advertise.address"),
		d.Discovery.QueueAdvertiseAddress,
	)
}

func OrchestratorGRPCAdvertiseAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("orchestrator.advertise_address"),
		d.Orchestrator.AdvertiseAddress,
	)
}

func LogGRPCAdvertiseAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("log.grpc.advertise_address"),
		d.Log.GRPC.AdvertiseAddress,
		viper.GetString("discovery.log.grpc.advertise.address"),
		d.Discovery.LogGRPCAdvertiseAddress,
	)
}

func ArtifactGRPCAdvertiseAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("artifact.grpc.advertise_address"),
		d.Artifact.GRPC.AdvertiseAddress,
		viper.GetString("discovery.artifact.grpc.advertise.address"),
		d.Discovery.ArtifactGRPCAdvertiseAddress,
	)
}

func QueueRegistryPublishAddress(bindListenAddr string) string {
	if a := QueueGRPCAdvertiseAddress(); a != "" {
		return a
	}

	return bindListenAddr
}

func OrchestratorRegistryPublishAddress(bindListenAddr string) string {
	if a := OrchestratorGRPCAdvertiseAddress(); a != "" {
		return a
	}

	return bindListenAddr
}

func LogGRPCRegistryPublishAddress(bindListenAddr string) string {
	if a := LogGRPCAdvertiseAddress(); a != "" {
		return a
	}

	return bindListenAddr
}

func ArtifactGRPCRegistryPublishAddress(bindListenAddr string) string {
	if a := ArtifactGRPCAdvertiseAddress(); a != "" {
		return a
	}

	return bindListenAddr
}

func QueueRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("queue.registry.address"),
		d.Queue.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func OrchestratorRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("orchestrator.registry.address"),
		d.Orchestrator.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func QueueResolverAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("queue.resolver.address"),
		d.Queue.ResolverAddress,
		viper.GetString("discovery.queue.resolver.address"),
		d.Discovery.QueueResolverAddress,
	)
}

func QueueRegisterWithRegistry() bool {
	if viper.IsSet("queue.register_with_registry") {
		return viper.GetBool("queue.register_with_registry")
	}

	return MustDefaults().Queue.RegisterWithRegistry
}

func OrchestratorRegisterWithRegistry() bool {
	if viper.IsSet("orchestrator.register_with_registry") {
		return viper.GetBool("orchestrator.register_with_registry")
	}

	return MustDefaults().Orchestrator.RegisterWithRegistry
}

func LogRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("log.registry.address"),
		d.Log.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func LogResolverAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("log.grpc.resolver.address"),
		viper.GetString("log.resolver.address"),
		d.Log.GRPC.ResolverAddress,
		viper.GetString("discovery.log.grpc.resolver.address"),
		d.Discovery.LogGRPCResolverAddress,
	)
}

func ArtifactRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("artifact.registry.address"),
		d.Artifact.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func ArtifactResolverAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("artifact.grpc.resolver.address"),
		viper.GetString("artifact.resolver.address"),
		d.Artifact.GRPC.ResolverAddress,
		viper.GetString("discovery.artifact.grpc.resolver.address"),
		d.Discovery.ArtifactGRPCResolverAddress,
		viper.GetString("discovery.artifact.address"),
		d.Discovery.ArtifactAddress,
	)
}

func LogRegisterWithRegistry() bool {
	if viper.IsSet("log.grpc.register_with_registry") {
		return viper.GetBool("log.grpc.register_with_registry")
	}
	return MustDefaults().Log.GRPC.RegisterWithRegistry
}

func ArtifactRegisterWithRegistry() bool {
	if viper.IsSet("artifact.grpc.register_with_registry") {
		return viper.GetBool("artifact.grpc.register_with_registry")
	}

	return MustDefaults().Artifact.GRPC.RegisterWithRegistry
}

func WorkerRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("worker.registry.address"),
		d.Worker.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func WorkerRegistryDialAddress() string {
	return registryDialAddress(WorkerRegistryAddress)
}

func WorkerQueueAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("worker.queue.address"),
		d.Worker.Queue.Address,
		viper.GetString("discovery.queue.address"),
		d.Discovery.QueueAddress,
	)
}

func WorkerLogAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("worker.log.address"),
		d.Worker.LogAddress,
		viper.GetString("discovery.log.address"),
		d.Discovery.LogAddress,
	)
}

func WorkerOrchestratorAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("worker.orchestrator.address"),
		d.Worker.OrchestratorAddress,
		viper.GetString("discovery.orchestrator.address"),
		d.Discovery.OrchestratorAddress,
	)
}

func WorkerSecretsAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("worker.secrets.address"),
		d.Worker.SecretsAddress,
	)
}

func APIRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("api.registry.address"),
		d.API.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func APIQueueAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("api.queue.address"),
		d.API.QueueAddress,
		viper.GetString("discovery.queue.address"),
		d.Discovery.QueueAddress,
	)
}

func APILogAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("api.log.address"),
		d.API.LogAddress,
		viper.GetString("discovery.log.address"),
		d.Discovery.LogAddress,
		viper.GetString("worker.log.address"),
		d.Worker.LogAddress,
	)
}

func CellIngressEndpointSpecs() []string {
	d := MustDefaults()
	return coalesceStringSlices(
		stringSliceFromViper("cell_ingress_endpoints"),
		stringSliceFromViper("cell_ingress.endpoints"),
		d.API.CellIngressEndpoints,
	)
}

func CellIngressEndpoints() (map[string]string, error) {
	return ParseCellIngressEndpoints(CellIngressEndpointSpecs())
}

func APICellIngressEndpointSpecs() []string {
	return coalesceStringSlices(
		stringSliceFromViper("api.cell_ingress.endpoints"),
		CellIngressEndpointSpecs(),
	)
}

func APICellIngressEndpoints() (map[string]string, error) {
	return ParseCellIngressEndpoints(APICellIngressEndpointSpecs())
}

func CatalogCellDatabaseSpecs() []string {
	return stringSliceFromViper("cell_database_dsns")
}

func CatalogCellDatabaseDSNs() (map[string]string, error) {
	return ParseCellDatabaseDSNs(CatalogCellDatabaseSpecs())
}

func ParseCellDatabaseDSNs(specs []string) (map[string]string, error) {
	out := make(map[string]string)
	for _, spec := range cleanStringSlice(specs) {
		cellID, dsn, ok := strings.Cut(spec, "=")
		if !ok {
			return nil, fmt.Errorf("cell database DSN %q must be cell_id=dsn", spec)
		}

		cellID = strings.TrimSpace(cellID)
		dsn = strings.TrimSpace(dsn)
		if cellID == "" {
			return nil, fmt.Errorf("cell database DSN %q has empty cell_id", spec)
		}

		if dsn == "" {
			return nil, fmt.Errorf("cell database DSN for %q has empty dsn", cellID)
		}

		out[cellID] = dsn
	}

	return out, nil
}

func ParseCellIngressEndpoints(specs []string) (map[string]string, error) {
	out := make(map[string]string)
	for _, spec := range cleanStringSlice(specs) {
		cellID, endpoint, ok := strings.Cut(spec, "=")
		if !ok {
			return nil, fmt.Errorf("cell ingress endpoint %q must be cell_id=url", spec)
		}

		cellID = strings.TrimSpace(cellID)
		endpoint = strings.TrimSpace(endpoint)
		if cellID == "" {
			return nil, fmt.Errorf("cell ingress endpoint %q has empty cell_id", spec)
		}

		if endpoint == "" {
			return nil, fmt.Errorf("cell ingress endpoint for %q has empty url", cellID)
		}

		out[cellID] = endpoint
	}

	return out, nil
}

func CronRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("cron.registry.address"),
		d.Cron.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func CronQueueAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("cron.queue.address"),
		d.Cron.QueueAddress,
		viper.GetString("discovery.queue.address"),
		d.Discovery.QueueAddress,
	)
}

func CronClaimTTL() time.Duration {
	if d := viper.GetDuration("claim_ttl"); d > 0 {
		return d
	}

	if d := viper.GetDuration("cron.claim_ttl"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Cron.ClaimTTL)
	if d > 0 {
		return d
	}

	return 5 * time.Minute
}

func ReconcilerRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("reconciler.registry.address"),
		d.Reconciler.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func ReconcilerQueueAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("reconciler.queue.address"),
		d.Reconciler.QueueAddress,
		viper.GetString("discovery.queue.address"),
		d.Discovery.QueueAddress,
	)
}

func ReconcilerInterval() time.Duration {
	if d := viper.GetDuration("interval"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Reconciler.Interval)
	if d > 0 {
		return d
	}

	return 30 * time.Second
}

func ReconcilerLeaseTTL() time.Duration {
	if d := viper.GetDuration("lease_ttl"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Reconciler.LeaseTTL)
	if d > 0 {
		return d
	}

	return 2 * time.Minute
}

func ReconcilerRedispatchLimit() int {
	if limit := viper.GetInt("redispatch_limit"); limit > 0 {
		return limit
	}

	limit := MustDefaults().Reconciler.RedispatchLimit
	if limit > 0 {
		return limit
	}

	return 1000
}

func ReconcilerMetricsPort() int {
	return MustDefaults().Reconciler.MetricsPort
}

func ReconcilerMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	return ReconcilerMetricsPort()
}

func ReconcilerMetricsHost() string {
	return metricsHost("metrics_host", "reconciler.metrics_host", MustDefaults().Reconciler.MetricsHost)
}

func ReconcilerMetricsListenAddr() string {
	return metricsListenAddr(ReconcilerMetricsHost(), ReconcilerMetricsEffectiveListenPort())
}

func CatalogInterval() time.Duration {
	if d := viper.GetDuration("interval"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().Catalog.Interval)
	if d > 0 {
		return d
	}

	return time.Second
}

func CatalogBatchSize() int {
	if batchSize := viper.GetInt("batch_size"); batchSize > 0 {
		return batchSize
	}

	if batchSize := MustDefaults().Catalog.BatchSize; batchSize > 0 {
		return batchSize
	}

	return 100
}

func CatalogMetricsPort() int {
	return MustDefaults().Catalog.MetricsPort
}

func CatalogMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	return CatalogMetricsPort()
}

func CatalogMetricsHost() string {
	return metricsHost("metrics_host", "catalog.metrics_host", MustDefaults().Catalog.MetricsHost)
}

func CatalogMetricsListenAddr() string {
	return metricsListenAddr(CatalogMetricsHost(), CatalogMetricsEffectiveListenPort())
}

func CellIngressHost() string {
	if host := strings.TrimSpace(viper.GetString("host")); host != "" {
		return host
	}

	if host := strings.TrimSpace(viper.GetString("cell_ingress.host")); host != "" {
		return host
	}

	return MustDefaults().CellIngress.Host
}

func CellIngressPort() int {
	return MustDefaults().CellIngress.Port
}

func CellIngressEffectiveListenPort() int {
	return effectiveListenPort(CellIngressPort)
}

func CellIngressListenAddr() string {
	return net.JoinHostPort(CellIngressHost(), strconv.Itoa(CellIngressEffectiveListenPort()))
}

func CellIngressMetricsPort() int {
	return MustDefaults().CellIngress.MetricsPort
}

func CellIngressMetricsHost() string {
	return metricsHost("metrics_host", "cell_ingress.metrics_host", MustDefaults().CellIngress.MetricsHost)
}

func CellIngressMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}

	if p := viper.GetInt("cell_ingress.metrics_port"); p > 0 {
		return p
	}

	return CellIngressMetricsPort()
}

func CellIngressMetricsListenAddr() string {
	return metricsListenAddr(CellIngressMetricsHost(), CellIngressMetricsEffectiveListenPort())
}

func CellIngressRepairInterval() time.Duration {
	if d := viper.GetDuration("repair_interval"); d > 0 {
		return d
	}

	if d := viper.GetDuration("cell_ingress.repair_interval"); d > 0 {
		return d
	}

	d := time.Duration(MustDefaults().CellIngress.RepairInterval)
	if d > 0 {
		return d
	}

	return 30 * time.Second
}

func CellIngressRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("cell_ingress.registry.address"),
		d.CellIngress.RegistryAddress,
		viper.GetString("discovery.registry.address"),
		d.Discovery.RegistryAddress,
	)
}

func CellIngressQueueAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("cell_ingress.queue.address"),
		d.CellIngress.QueueAddress,
		viper.GetString("discovery.queue.address"),
		d.Discovery.QueueAddress,
	)
}

func CellIngressRegistryDialAddress() string {
	return registryDialAddress(CellIngressRegistryAddress)
}

func registryDialAddress(roleRegistry func() string) string {
	return strings.Join(registryDialAddresses(roleRegistry), ",")
}

func registryDialAddresses(roleRegistry func() string) []string {
	d := MustDefaults()
	return coalesceStringSlices(
		stringSliceFromViper("registry.addresses"),
		stringSliceFromViper("discovery.registry.addresses"),
		d.Discovery.RegistryAddresses,
		[]string{viper.GetString("registry.address")},
		[]string{roleRegistry()},
		[]string{RegistryListenAddr()},
	)
}

func QueueRegistrationRegistryAddress() string {
	return registryDialAddress(QueueRegistryAddress)
}

func OrchestratorRegistrationRegistryAddress() string {
	return registryDialAddress(OrchestratorRegistryAddress)
}

func LogRegistrationRegistryAddress() string {
	return registryDialAddress(LogRegistryAddress)
}

func ArtifactRegistrationRegistryAddress() string {
	return registryDialAddress(ArtifactRegistryAddress)
}

func WorkerRegistrationRegistryAddress() string {
	return registryDialAddress(WorkerRegistryAddress)
}

func ReconcilerRegistryDialAddress() string {
	return registryDialAddress(ReconcilerRegistryAddress)
}

func CronRegistryDialAddress() string {
	return registryDialAddress(CronRegistryAddress)
}

func APIRegistryDialAddress() string {
	return registryDialAddress(APIRegistryAddress)
}

func effectiveListenPort(defaultPort func() int) int {
	if p := viper.GetInt("port"); p > 0 {
		return p
	}
	return defaultPort()
}

func APIEffectiveListenPort() int {
	if p := effectiveListenPort(APIPort); p > 0 && p != APIPort() {
		return p
	}

	if p := viper.GetInt("api.port"); p > 0 {
		return p
	}

	return APIPort()
}

func APILogFormat() string {
	d := MustDefaults()
	s := strings.ToLower(strings.TrimSpace(viper.GetString("api.log_format")))
	if s == "" {
		s = strings.ToLower(strings.TrimSpace(d.API.LogFormat))
	}

	if s == "" {
		return "text"
	}

	return s
}

func QueueEffectiveListenPort() int {
	return effectiveListenPort(QueuePort)
}

func OrchestratorEffectiveListenPort() int {
	return effectiveListenPort(OrchestratorPort)
}

// QueueMetricsEffectiveListenPort returns the HTTP /metrics listen port for vectis-queue.
func QueueMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return QueueMetricsPort()
}

func OrchestratorMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return OrchestratorMetricsPort()
}

func OrchestratorMetricsListenAddr() string {
	return metricsListenAddr(OrchestratorMetricsHost(), OrchestratorMetricsEffectiveListenPort())
}

// WorkerMetricsEffectiveListenPort returns the HTTP /metrics listen port for vectis-worker.
func WorkerMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return WorkerMetricsPort()
}

// WorkerCoreMetricsEffectiveListenPort returns the HTTP /metrics listen port for vectis-worker-core.
func WorkerCoreMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return WorkerCoreMetricsPort()
}

func RegistryEffectiveListenPort() int {
	return effectiveListenPort(RegistryPort)
}

func ArtifactEffectiveListenPort() int {
	return effectiveListenPort(ArtifactGRPCPort)
}

func PinnedQueueAddress() string {
	return coalesceNonEmpty(
		QueueResolverAddress(),
		WorkerQueueAddress(),
		APIQueueAddress(),
	)
}

func PinnedLogAddress() string {
	return coalesceNonEmpty(
		LogResolverAddress(),
		WorkerLogAddress(),
	)
}

func PinnedArtifactAddress() string {
	return ArtifactResolverAddress()
}

func PinnedOrchestratorAddress() string {
	return WorkerOrchestratorAddress()
}

func RateLimitAuthRefillRate() time.Duration {
	if viper.IsSet("api.rate_limit.auth_refill_rate") {
		return viper.GetDuration("api.rate_limit.auth_refill_rate")
	}

	d := time.Duration(MustDefaults().API.RateLimit.AuthRefillRate)
	if d > 0 {
		return d
	}

	return 12 * time.Second
}

func RateLimitAuthBurstSize() int {
	if viper.IsSet("api.rate_limit.auth_burst_size") {
		return viper.GetInt("api.rate_limit.auth_burst_size")
	}

	if bs := MustDefaults().API.RateLimit.AuthBurstSize; bs > 0 {
		return bs
	}

	return 5
}

func RateLimitTokenRefillRate() time.Duration {
	if viper.IsSet("api.rate_limit.token_refill_rate") {
		return viper.GetDuration("api.rate_limit.token_refill_rate")
	}

	d := time.Duration(MustDefaults().API.RateLimit.TokenRefillRate)
	if d > 0 {
		return d
	}

	return 3 * time.Second
}

func RateLimitTokenBurstSize() int {
	if viper.IsSet("api.rate_limit.token_burst_size") {
		return viper.GetInt("api.rate_limit.token_burst_size")
	}

	if bs := MustDefaults().API.RateLimit.TokenBurstSize; bs > 0 {
		return bs
	}

	return 20
}

func RateLimitGeneralRefillRate() time.Duration {
	if viper.IsSet("api.rate_limit.general_refill_rate") {
		return viper.GetDuration("api.rate_limit.general_refill_rate")
	}

	d := time.Duration(MustDefaults().API.RateLimit.GeneralRefillRate)
	if d > 0 {
		return d
	}

	return 600 * time.Millisecond
}

func RateLimitGeneralBurstSize() int {
	if viper.IsSet("api.rate_limit.general_burst_size") {
		return viper.GetInt("api.rate_limit.general_burst_size")
	}

	if bs := MustDefaults().API.RateLimit.GeneralBurstSize; bs > 0 {
		return bs
	}

	return 150
}
