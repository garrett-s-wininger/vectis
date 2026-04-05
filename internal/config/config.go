package config

import (
	_ "embed"
	"fmt"
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
	API        APIDefaults        `toml:"api"`
	Queue      QueueDefaults      `toml:"queue"`
	Registry   RegistryDefaults   `toml:"registry"`
	Log        LogDefaults        `toml:"log"`
	Discovery  DiscoveryDefaults  `toml:"discovery"`
	Database   DatabaseDefaults   `toml:"database"`
	Worker     WorkerDefaults     `toml:"worker"`
	Cron       CronDefaults       `toml:"cron"`
	Reconciler ReconcilerDefaults `toml:"reconciler"`
}

type APIDefaults struct {
	Host            string `toml:"host"`
	Port            int    `toml:"port"`
	LogFormat       string `toml:"log_format"`
	RegistryAddress string `toml:"registry.address"`
	QueueAddress    string `toml:"queue.address"`
}

type QueueDefaults struct {
	Port                 int    `toml:"port"`
	MetricsPort          int    `toml:"metrics_port"`
	RegistryAddress      string `toml:"registry.address"`
	ResolverAddress      string `toml:"resolver.address"`
	AdvertiseAddress     string `toml:"advertise_address"`
	RegisterWithRegistry bool   `toml:"register_with_registry"`
}

type RegistryDefaults struct {
	Port int `toml:"port"`
}

type LogDefaults struct {
	Host            string       `toml:"host"`
	RegistryAddress string       `toml:"registry.address"`
	GRPC            GRPCDefaults `toml:"grpc"`
	SSE             SSEDefaults  `toml:"sse"`
}

type GRPCDefaults struct {
	Port                 int    `toml:"port"`
	ResolverAddress      string `toml:"resolver.address"`
	AdvertiseAddress     string `toml:"advertise_address"`
	RegisterWithRegistry bool   `toml:"register_with_registry"`
}

type SSEDefaults struct {
	Port int `toml:"port"`
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

type WorkerDefaults struct {
	RegistryAddress string `toml:"registry.address"`
	QueueAddress    string `toml:"queue.address"`
	LogAddress      string `toml:"log.address"`
}

type CronDefaults struct {
	RegistryAddress string `toml:"registry.address"`
	QueueAddress    string `toml:"queue.address"`
}

type ReconcilerDefaults struct {
	RegistryAddress string       `toml:"registry.address"`
	QueueAddress    string       `toml:"queue.address"`
	Interval        tomlDuration `toml:"interval"`
}

var (
	once     sync.Once
	cached   Defaults
	parseErr error
)

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
	if d.API.Host == "" {
		panic("config defaults: api.host must not be empty")
	}

	validatePort := func(port int, name string) {
		if port <= 0 {
			panic(fmt.Sprintf("config defaults: %s must be > 0 (got %d)", name, port))
		}
	}

	validatePort(d.API.Port, "api.port")
	validatePort(d.Queue.Port, "queue.port")
	validatePort(d.Queue.MetricsPort, "queue.metrics_port")
	if d.Queue.MetricsPort == d.Queue.Port {
		panic("config defaults: queue.metrics_port must differ from queue.port")
	}

	validatePort(d.Registry.Port, "registry.port")
	validatePort(d.Log.GRPC.Port, "log.grpc.port")
	validatePort(d.Log.SSE.Port, "log.sse.port")

	if d.Log.Host == "" {
		panic("config defaults: log.host must not be empty")
	}

	if d.Database.Driver == "" {
		panic("config defaults: database.driver must not be empty")
	}

	if d.Database.DSN == "" {
		panic("config defaults: database.dsn must not be empty")
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

	if time.Duration(d.Reconciler.Interval) <= 0 {
		panic("config defaults: reconciler.interval must be > 0")
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

func PublicHost() string {
	return MustDefaults().API.Host
}

func APIPort() int {
	return MustDefaults().API.Port
}

func QueuePort() int {
	return MustDefaults().Queue.Port
}

func QueueMetricsPort() int {
	return MustDefaults().Queue.MetricsPort
}

func RegistryPort() int {
	return MustDefaults().Registry.Port
}

func LogGRPCPort() int {
	return MustDefaults().Log.GRPC.Port
}

func LogWebSocketPort() int {
	return MustDefaults().Log.SSE.Port
}

func APIListenAddr() string {
	return ":" + strconv.Itoa(APIPort())
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

func LogWebSocketListenAddr() string {
	return ":" + strconv.Itoa(LogWebSocketPort())
}

func PublicAPIBaseURL() string {
	return fmt.Sprintf("http://%s:%d", PublicHost(), APIPort())
}

func PublicLogSSEURL(runID string) string {
	return fmt.Sprintf("http://%s:%d/sse/logs/%s", MustDefaults().Log.Host, LogWebSocketPort(), runID)
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
	if a := WorkerRegistryAddress(); a != "" {
		return a
	}

	return RegistryListenAddr()
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

func registryDialAddress(roleRegistry func() string) string {
	return coalesceNonEmpty(
		viper.GetString("registry.address"),
		roleRegistry(),
		RegistryListenAddr(),
	)
}

func QueueRegistrationRegistryAddress() string {
	return registryDialAddress(QueueRegistryAddress)
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
	s := strings.ToLower(strings.TrimSpace(viper.GetString("log_format")))
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
