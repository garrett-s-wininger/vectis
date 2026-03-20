package config

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pelletier/go-toml/v2"
)

//go:embed defaults.toml
var defaultsToml string

type Defaults struct {
	API      APIDefaults      `toml:"api"`
	Queue    QueueDefaults    `toml:"queue"`
	Registry RegistryDefaults `toml:"registry"`
	Log      LogDefaults      `toml:"log"`
	Database DatabaseDefaults `toml:"database"`
}

type APIDefaults struct {
	Host string `toml:"host"`
	Port int    `toml:"port"`
}

type QueueDefaults struct {
	Port int `toml:"port"`
}

type RegistryDefaults struct {
	Port int `toml:"port"`
}

type LogDefaults struct {
	Host string       `toml:"host"`
	GRPC GRPCDefaults `toml:"grpc"`
	WS   WSDefaults   `toml:"websocket"`
}

type GRPCDefaults struct {
	Port int `toml:"port"`
}

type WSDefaults struct {
	Port int `toml:"port"`
}

type DatabaseDefaults struct {
	Driver string `toml:"driver"`
	DSN    string `toml:"dsn"`
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
	validatePort(d.Registry.Port, "registry.port")
	validatePort(d.Log.GRPC.Port, "log.grpc.port")
	validatePort(d.Log.WS.Port, "log.websocket.port")

	if d.Log.Host == "" {
		panic("config defaults: log.host must not be empty")
	}

	if d.Database.Driver == "" {
		panic("config defaults: database.driver must not be empty")
	}

	if d.Database.DSN == "" {
		panic("config defaults: database.dsn must not be empty")
	}
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

func RegistryPort() int {
	return MustDefaults().Registry.Port
}

func LogGRPCPort() int {
	return MustDefaults().Log.GRPC.Port
}

func LogWebSocketPort() int {
	return MustDefaults().Log.WS.Port
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

func PublicLogWebSocketURL(runID string) string {
	return fmt.Sprintf("ws://%s:%d/ws/logs/%s", MustDefaults().Log.Host, LogWebSocketPort(), runID)
}

func DBDriver() string {
	return MustDefaults().Database.Driver
}

func DBDSN(dataHome string) string {
	return strings.NewReplacer(
		"{{data_home}}", dataHome,
	).Replace(MustDefaults().Database.DSN)
}
