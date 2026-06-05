package config

import (
	_ "embed"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

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
	Cell         CellDefaults         `toml:"cell"`
	API          APIDefaults          `toml:"api"`
	Queue        QueueDefaults        `toml:"queue"`
	Registry     RegistryDefaults     `toml:"registry"`
	Log          LogDefaults          `toml:"log"`
	LogForwarder LogForwarderDefaults `toml:"log_forwarder"`
	Discovery    DiscoveryDefaults    `toml:"discovery"`
	Database     DatabaseDefaults     `toml:"database"`
	Worker       WorkerDefaults       `toml:"worker"`
	Cron         CronDefaults         `toml:"cron"`
	Reconciler   ReconcilerDefaults   `toml:"reconciler"`
	Catalog      CatalogDefaults      `toml:"catalog"`
	CellIngress  CellIngressDefaults  `toml:"cell_ingress"`
	GRPCTLS      GRPCTLSDefaults      `toml:"grpc_tls"`
	MetricsTLS   MetricsTLSDefaults   `toml:"metrics_tls"`
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

type LogDefaults struct {
	Host                        string       `toml:"host"`
	MetricsHost                 string       `toml:"metrics_host"`
	MetricsPort                 int          `toml:"metrics_port"`
	MaxRunBuffers               int          `toml:"max_run_buffers"`
	StorageReadOnlyMinFreeBytes uint64       `toml:"storage_read_only_min_free_bytes"`
	RegistryAddress             string       `toml:"registry.address"`
	GRPC                        GRPCDefaults `toml:"grpc"`
}

type LogForwarderDefaults struct {
	MetricsHost string `toml:"metrics_host"`
	MetricsPort int    `toml:"metrics_port"`
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
	PgxPool PgxPoolDefaults `toml:"pgx_pool"`
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
	QueueAddress                 string       `toml:"queue.address"`
	LogAddress                   string       `toml:"log.address"`
	QueueAdvertiseAddress        string       `toml:"queue.advertise.address"`
	LogGRPCAdvertiseAddress      string       `toml:"log.grpc.advertise.address"`
	RegistryResolverRefresh      tomlDuration `toml:"registry_resolver_refresh"`
	RegistryResolverPollTimeout  tomlDuration `toml:"registry_resolver_poll_timeout"`
	RegistryResolverErrorRefresh tomlDuration `toml:"registry_resolver_error_refresh"`
	RegistryRegistrationRefresh  tomlDuration `toml:"registry_registration_refresh"`
}

type WorkerControlDefaults struct {
	Mode    string `toml:"mode"`
	Port    int    `toml:"port"`
	PortMin int    `toml:"port_min"`
	PortMax int    `toml:"port_max"`
}

type WorkerDefaults struct {
	RegistryAddress      string                `toml:"registry.address"`
	QueueAddress         string                `toml:"queue.address"`
	LogAddress           string                `toml:"log.address"`
	MetricsHost          string                `toml:"metrics_host"`
	MetricsPort          int                   `toml:"metrics_port"`
	Control              WorkerControlDefaults `toml:"control"`
	RegisterWithRegistry bool                  `toml:"register_with_registry"`
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
	_ = viper.BindEnv("discovery.registry.address", "VECTIS_DISCOVERY_REGISTRY_ADDRESS")
	_ = viper.BindEnv("discovery.registry.addresses", "VECTIS_DISCOVERY_REGISTRY_ADDRESSES")
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

	if d.Log.Host == "" {
		panic("config defaults: log.host must not be empty")
	}

	if d.Database.Driver == "" {
		panic("config defaults: database.driver must not be empty")
	}

	if d.Database.DSN == "" {
		panic("config defaults: database.dsn must not be empty")
	}

	validateHost(d.Worker.MetricsHost, "worker.metrics_host")
	validatePort(d.Worker.MetricsPort, "worker.metrics_port")
	if d.Worker.MetricsPort == d.Queue.MetricsPort {
		panic("config defaults: worker.metrics_port must differ from queue.metrics_port")
	}

	if d.Worker.MetricsPort == d.LogForwarder.MetricsPort {
		panic("config defaults: worker.metrics_port must differ from log_forwarder.metrics_port")
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

	validateHost(d.Reconciler.MetricsHost, "reconciler.metrics_host")
	validatePort(d.Reconciler.MetricsPort, "reconciler.metrics_port")
	if d.Reconciler.MetricsPort == d.Queue.MetricsPort ||
		d.Reconciler.MetricsPort == d.Worker.MetricsPort ||
		d.Reconciler.MetricsPort == d.Log.MetricsPort ||
		d.Reconciler.MetricsPort == d.LogForwarder.MetricsPort ||
		d.Reconciler.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: reconciler.metrics_port must differ from queue/worker/log/log-forwarder metrics ports and worker control port")
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
		d.Catalog.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: catalog.metrics_port must differ from queue/worker/log/log-forwarder/reconciler metrics ports and worker control port")
	}

	if strings.TrimSpace(d.CellIngress.Host) == "" {
		panic("config defaults: cell_ingress.host must not be empty")
	}
	validatePort(d.CellIngress.Port, "cell_ingress.port")
	if d.CellIngress.Port == d.API.Port ||
		d.CellIngress.Port == d.Queue.Port ||
		d.CellIngress.Port == d.Registry.Port ||
		d.CellIngress.Port == d.Log.GRPC.Port {
		panic("config defaults: cell_ingress.port must differ from api/queue/registry/log ports")
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
		d.CellIngress.MetricsPort == d.Worker.Control.Port {
		panic("config defaults: cell_ingress.metrics_port must differ from cell ingress, queue/worker/log/log-forwarder/reconciler/catalog metrics ports and worker control port")
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
	return MustDefaults().API.Host
}

func APIHost() string {
	if host := strings.TrimSpace(viper.GetString("host")); host != "" {
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

func QueueMetricsPort() int {
	return MustDefaults().Queue.MetricsPort
}

func QueueMetricsListenAddr() string {
	return metricsListenAddr(QueueMetricsHost(), QueueMetricsEffectiveListenPort())
}

func WorkerMetricsHost() string {
	return metricsHost("metrics_host", "worker.metrics_host", MustDefaults().Worker.MetricsHost)
}

func WorkerMetricsPort() int {
	return MustDefaults().Worker.MetricsPort
}

func WorkerMetricsListenAddr() string {
	return metricsListenAddr(WorkerMetricsHost(), WorkerMetricsEffectiveListenPort())
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

func PublicAPIBaseURL() string {
	return fmt.Sprintf("http://%s:%d", PublicHost(), APIPort())
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

func LogGRPCAdvertiseAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("log.grpc.advertise_address"),
		d.Log.GRPC.AdvertiseAddress,
		viper.GetString("discovery.log.grpc.advertise.address"),
		d.Discovery.LogGRPCAdvertiseAddress,
	)
}

func QueueRegistryPublishAddress(bindListenAddr string) string {
	if a := QueueGRPCAdvertiseAddress(); a != "" {
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

func QueueRegistryAddress() string {
	d := MustDefaults()
	return coalesceNonEmpty(
		viper.GetString("queue.registry.address"),
		d.Queue.RegistryAddress,
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

func LogRegisterWithRegistry() bool {
	if viper.IsSet("log.grpc.register_with_registry") {
		return viper.GetBool("log.grpc.register_with_registry")
	}
	return MustDefaults().Log.GRPC.RegisterWithRegistry
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
		d.Worker.QueueAddress,
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

func LogRegistrationRegistryAddress() string {
	return registryDialAddress(LogRegistryAddress)
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
	return effectiveListenPort(APIPort)
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

// QueueMetricsEffectiveListenPort returns the HTTP /metrics listen port for vectis-queue.
func QueueMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return QueueMetricsPort()
}

// WorkerMetricsEffectiveListenPort returns the HTTP /metrics listen port for vectis-worker.
func WorkerMetricsEffectiveListenPort() int {
	if p := viper.GetInt("metrics_port"); p > 0 {
		return p
	}
	return WorkerMetricsPort()
}

func RegistryEffectiveListenPort() int {
	return effectiveListenPort(RegistryPort)
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
