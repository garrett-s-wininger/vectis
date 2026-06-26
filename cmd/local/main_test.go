package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/viper"

	encryptedfs "vectis/extensions/secrets/encryptedfs"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/localpki"
)

func resetLocalTestConfig(t *testing.T) {
	t.Helper()
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv(database.EnvDatabaseDriver, "")
	t.Setenv(database.EnvDatabaseDSN, "")
	t.Setenv(database.EnvGlobalDatabaseDSN, "")
	t.Setenv(database.EnvCellDatabaseDSN, "")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "")
	t.Setenv("VECTIS_CELL_ID", "")
}

func TestBuildLocalTopology_DefaultCell(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("VECTIS_CELL_ID", "iad-a")

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	if topology.GlobalDB != filepath.Join(dataHome, "vectis", "global", "db.sqlite3") {
		t.Fatalf("global DB: got %q", topology.GlobalDB)
	}

	if len(topology.Cells) != 1 {
		t.Fatalf("cells: got %d, want 1", len(topology.Cells))
	}

	cell := topology.Cells[0]
	if cell.ID != "iad-a" {
		t.Fatalf("cell ID: got %q, want iad-a", cell.ID)
	}

	if cell.CellDB != filepath.Join(dataHome, "vectis", "cells", "iad-a", "db.sqlite3") {
		t.Fatalf("cell DB: got %q", cell.CellDB)
	}

	if cell.QueuePort != config.QueuePort() || cell.SecretsPort != config.SecretsPort() || cell.CellIngressPort != config.CellIngressPort() {
		t.Fatalf("default ports: queue=%d secrets=%d ingress=%d", cell.QueuePort, cell.SecretsPort, cell.CellIngressPort)
	}
}

func TestEnsureLocalSecretsKeys(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("VECTIS_CELL_ID", "iad-a")

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	if err := ensureLocalSecretsKeys(topology); err != nil {
		t.Fatalf("ensureLocalSecretsKeys: %v", err)
	}

	keyFile := topology.Cells[0].SecretsKeyFile
	info, err := os.Stat(keyFile)
	if err != nil {
		t.Fatalf("stat key file: %v", err)
	}

	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("key file permissions = %o, want no group/other bits", info.Mode().Perm())
	}

	if _, err := encryptedfs.LoadEncryptedFSKeyFile(keyFile); err != nil {
		t.Fatalf("LoadEncryptedFSKeyFile: %v", err)
	}
}

func TestBuildLocalTopology_ExtraCells(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("cells", []string{"pdx-b", "sjc-c"})

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	gotIDs := localCellIDs(topology.Cells)
	wantIDs := []string{"iad-a", "pdx-b", "sjc-c"}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("cell IDs: got %v, want %v", gotIDs, wantIDs)
	}

	if topology.Cells[1].QueuePort != config.QueuePort()+cellPortStride {
		t.Fatalf("second cell queue port: got %d", topology.Cells[1].QueuePort)
	}

	if topology.Cells[1].SecretsPort != config.SecretsPort()+cellPortStride {
		t.Fatalf("second cell secrets port: got %d", topology.Cells[1].SecretsPort)
	}

	if topology.Cells[2].CellIngressPort != config.CellIngressPort()+2*cellPortStride {
		t.Fatalf("third cell ingress port: got %d", topology.Cells[2].CellIngressPort)
	}

	if topology.Cells[2].CellDB != filepath.Join(dataHome, "vectis", "cells", "sjc-c", "db.sqlite3") {
		t.Fatalf("third cell DB: got %q", topology.Cells[2].CellDB)
	}
}

func TestBuildLocalTopology_PortEnvOverrides(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	t.Setenv("VECTIS_QUEUE_PORT", "18081")
	t.Setenv("VECTIS_QUEUE_METRICS_PORT", "19081")
	t.Setenv("VECTIS_SECRETS_PORT", "18090")
	t.Setenv("VECTIS_SECRETS_METRICS_PORT", "19091")
	t.Setenv("VECTIS_CELL_INGRESS_PORT", "18085")
	t.Setenv("VECTIS_CELL_INGRESS_METRICS_PORT", "19087")
	t.Setenv("VECTIS_WORKER_METRICS_PORT", "19082")
	viper.Set("cells", []string{"pdx-b"})

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	first := topology.Cells[0]
	if first.QueuePort != 18081 || first.QueueMetricsPort != 19081 ||
		first.SecretsPort != 18090 || first.SecretsMetricsPort != 19091 ||
		first.CellIngressPort != 18085 || first.CellIngressMetricsPort != 19087 ||
		first.WorkerMetricsPort != 19082 {
		t.Fatalf("default cell ports = %+v", first)
	}

	second := topology.Cells[1]
	if second.QueuePort != 18181 || second.QueueMetricsPort != 19181 ||
		second.SecretsPort != 18190 || second.SecretsMetricsPort != 19191 ||
		second.CellIngressPort != 18185 || second.CellIngressMetricsPort != 19187 ||
		second.WorkerMetricsPort != 19182 {
		t.Fatalf("second cell ports = %+v", second)
	}
}

func TestBuildLocalTopology_RejectsDuplicateCells(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("cells", []string{"pdx-b", "iad-a"})

	if _, err := buildLocalTopology(); err == nil {
		t.Fatal("buildLocalTopology succeeded with duplicate cell")
	}
}

func TestBuildLocalTopology_RejectsCustomDSNsForExtraCells(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv(database.EnvDatabaseDSN, filepath.Join(t.TempDir(), "shared.db"))
	viper.Set("cells", []string{"pdx-b"})

	if _, err := buildLocalTopology(); err == nil {
		t.Fatal("buildLocalTopology succeeded with custom DSN and extra cell")
	}
}

func TestLocalCellIngressEndpointSpecs(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("host", "0.0.0.0")
	cells := []localCell{
		{ID: "iad-a", CellIngressPort: 8085},
		{ID: "pdx-b", CellIngressPort: 8185},
	}

	got := localCellIngressEndpointSpecs(cells)
	want := []string{"iad-a=https://localhost:8085", "pdx-b=https://localhost:8185"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("endpoint specs: got %v, want %v", got, want)
	}

	viper.Set("grpc_insecure", true)
	got = localCellIngressEndpointSpecs(cells)
	want = []string{"iad-a=http://localhost:8085", "pdx-b=http://localhost:8185"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("insecure endpoint specs: got %v, want %v", got, want)
	}
}

func TestLocalCatalogCellDatabaseEnv(t *testing.T) {
	resetLocalTestConfig(t)
	cells := []localCell{
		{ID: "iad-a", CellDB: "/tmp/iad.db"},
		{ID: "pdx-b", CellDB: "/tmp/pdx.db"},
	}

	got := localCatalogCellDatabaseEnv(cells)
	want := []string{"VECTIS_CATALOG_CELL_DATABASE_DSNS=iad-a=/tmp/iad.db,pdx-b=/tmp/pdx.db"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("catalog env: got %v, want %v", got, want)
	}
}

func TestConfiguredLocalTLSDir(t *testing.T) {
	resetLocalTestConfig(t)

	if got := configuredLocalTLSDir("/tmp/data-home"); got != filepath.Join("/tmp/data-home", "vectis", "local-tls") {
		t.Fatalf("default TLS dir = %q", got)
	}

	viper.Set("tls_dir", "/tmp/custom-vectis-tls")
	if got := configuredLocalTLSDir("/tmp/data-home"); got != "/tmp/custom-vectis-tls" {
		t.Fatalf("custom TLS dir = %q", got)
	}
}

func TestLocalBrowserTLSOnSetsHTTPSVars(t *testing.T) {
	resetLocalTestConfig(t)
	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	cfg := localBrowserTLS(m, localHTTPSTLSOn, mocks.NopLogger{})
	if !cfg.Enabled || cfg.Scheme != "https" {
		t.Fatalf("browser TLS config = %+v, want enabled https", cfg)
	}

	for _, want := range []string{
		"VECTIS_API_TLS_CERT_FILE=" + m.ServerCert,
		"VECTIS_API_TLS_KEY_FILE=" + m.ServerKey,
		"VECTIS_API_SESSION_COOKIE_SECURE=true",
		"VECTIS_DOCS_TLS_CERT_FILE=" + m.ServerCert,
		"VECTIS_DOCS_TLS_KEY_FILE=" + m.ServerKey,
	} {
		if !hasEnv(cfg.Env, want) {
			t.Fatalf("browser TLS env missing %q: %v", want, cfg.Env)
		}
	}
}

func TestLocalBrowserTLSOffUsesHTTP(t *testing.T) {
	resetLocalTestConfig(t)
	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	cfg := localBrowserTLS(m, localHTTPSTLSOff, mocks.NopLogger{})
	if cfg.Enabled || cfg.Scheme != "http" || len(cfg.Env) != 0 {
		t.Fatalf("browser TLS off config = %+v, want plain HTTP", cfg)
	}
}

func TestLocalSPIFFEBuildsEnvAndCombinedClientCA(t *testing.T) {
	resetLocalTestConfig(t)
	tlsDir := t.TempDir()
	m, err := localpki.Ensure(tlsDir)
	if err != nil {
		t.Fatal(err)
	}

	spiffeMaterial, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	viper.Set("spiffe_enabled", true)
	viper.Set("spiffe_trust_domain", "vectis.internal")
	viper.Set("spiffe_workload_api_address", "unix:///tmp/spiffe-workload.sock")
	viper.Set("spiffe_registration_server_address", "unix:///tmp/spiffe-registration.sock")
	viper.Set("spiffe_parent_id", "spiffe://vectis.internal/vectis-spiffe/agent/local")
	viper.Set("spiffe_selectors", []string{"unix:uid:501", "unix:gid:20,unix:path:/usr/local/bin/vectis-worker"})
	viper.Set("spiffe_bundle_file", spiffeMaterial.CAFile)
	viper.Set("spiffe_fetch_timeout", "2s")
	viper.Set("spiffe_x509_svid_ttl", "5m")

	cfg, err := localSPIFFE(tlsDir, m)
	if err != nil {
		t.Fatalf("localSPIFFE: %v", err)
	}

	if !cfg.Enabled {
		t.Fatal("local SPIFFE config was not enabled")
	}

	if cfg.ClientCABundleFile != filepath.Join(tlsDir, "client-ca-bundle.pem") {
		t.Fatalf("client CA bundle path = %q", cfg.ClientCABundleFile)
	}

	b, err := os.ReadFile(cfg.ClientCABundleFile)
	if err != nil {
		t.Fatalf("read client CA bundle: %v", err)
	}

	count, err := countPEMCertificates(b)
	if err != nil {
		t.Fatalf("countPEMCertificates: %v", err)
	}

	if count != 2 {
		t.Fatalf("combined client CA certificate count = %d, want 2", count)
	}

	for _, want := range []string{
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE=" + cfg.ClientCABundleFile,
		"VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED=true",
		"VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN=vectis.internal",
		"VECTIS_WORKER_SPIFFE_ENABLED=true",
		"VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS=unix:///tmp/spiffe-workload.sock",
		"VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED=true",
		"VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS=unix:///tmp/spiffe-registration.sock",
		"VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID=spiffe://vectis.internal/vectis-spiffe/agent/local",
		"VECTIS_WORKER_SPIFFE_FETCH_TIMEOUT=2s",
		"VECTIS_WORKER_SPIFFE_REGISTRATION_X509_SVID_TTL=5m",
	} {
		if !hasEnv(cfg.Env, want) {
			t.Fatalf("local SPIFFE env missing %q: %v", want, cfg.Env)
		}
	}

	if !hasEnv(cfg.Env, "VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS=unix:uid:501,unix:gid:20,unix:path:/usr/local/bin/vectis-worker") {
		t.Fatalf("registration selectors env mismatch: %v", cfg.Env)
	}
}

func TestEmbeddedLocalSPIFFEConfigDefaults(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	runtimeHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_RUNTIME_DIR", runtimeHome)

	cfg, err := embeddedLocalSPIFFEConfig()
	if err != nil {
		t.Fatalf("embeddedLocalSPIFFEConfig: %v", err)
	}

	if !cfg.Enabled {
		t.Fatal("embedded local SPIFFE config was not enabled")
	}

	if cfg.TrustDomain != localSPIFFETrustDomainDefault {
		t.Fatalf("trust domain = %q, want %q", cfg.TrustDomain, localSPIFFETrustDomainDefault)
	}

	if cfg.ParentID != "spiffe://vectis.internal/vectis-spiffe/agent/local" {
		t.Fatalf("parent ID = %q", cfg.ParentID)
	}

	wantSelector := "unix:uid:" + strconv.Itoa(os.Getuid())
	if len(cfg.Selectors) != 1 || cfg.Selectors[0] != wantSelector {
		t.Fatalf("selectors = %v, want %q", cfg.Selectors, wantSelector)
	}

	if cfg.DataDir != filepath.Join(dataHome, "vectis", "spiffe") {
		t.Fatalf("data dir = %q", cfg.DataDir)
	}

	if cfg.RuntimeDir != filepath.Join(runtimeHome, "vectis", "spiffe") {
		t.Fatalf("runtime dir = %q", cfg.RuntimeDir)
	}

	if cfg.ServerSocket != filepath.Join(cfg.RuntimeDir, "registration.sock") || cfg.AgentSocket != filepath.Join(cfg.RuntimeDir, "workload.sock") {
		t.Fatalf("sockets = %q %q", cfg.ServerSocket, cfg.AgentSocket)
	}
}

func TestEmbeddedLocalSPIFFEConfigHonorsSPIFFEConfig(t *testing.T) {
	resetLocalTestConfig(t)
	dataDir := t.TempDir()
	runtimeDir := t.TempDir()

	viper.Set("spiffe_trust_domain", "demo.internal")
	viper.Set("spiffe_dir", dataDir)
	viper.Set("spiffe_runtime_dir", runtimeDir)
	viper.Set("spiffe_parent_id", "spiffe://demo.internal/vectis-spiffe/agent/local")
	viper.Set("spiffe_selectors", []string{"unix:uid:501", "unix:gid:20"})

	cfg, err := embeddedLocalSPIFFEConfig()
	if err != nil {
		t.Fatalf("embeddedLocalSPIFFEConfig: %v", err)
	}

	if cfg.TrustDomain != "demo.internal" {
		t.Fatalf("trust domain = %q", cfg.TrustDomain)
	}

	if cfg.DataDir != dataDir || cfg.RuntimeDir != runtimeDir {
		t.Fatalf("dirs = data:%q runtime:%q", cfg.DataDir, cfg.RuntimeDir)
	}

	if cfg.ParentID != "spiffe://demo.internal/vectis-spiffe/agent/local" {
		t.Fatalf("parent ID = %q", cfg.ParentID)
	}

	wantSelectors := []string{"unix:uid:501", "unix:gid:20"}
	if !reflect.DeepEqual(cfg.Selectors, wantSelectors) {
		t.Fatalf("selectors = %v, want %v", cfg.Selectors, wantSelectors)
	}
}

func TestEmbeddedLocalSPIFFEConfigSkipsPlaintextGRPC(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("grpc_insecure", true)

	cfg, err := embeddedLocalSPIFFEConfig()
	if err != nil {
		t.Fatalf("embeddedLocalSPIFFEConfig: %v", err)
	}

	if cfg.Enabled {
		t.Fatal("embedded local SPIFFE config enabled while plaintext gRPC is set")
	}
}

func TestStartEmbeddedLocalSPIFFEStartsAuthority(t *testing.T) {
	resetLocalTestConfig(t)
	dir, err := os.MkdirTemp(shortTempRoot(), "vectis-spiffe-*") //nolint:usetesting // Keep embedded SPIFFE runtime sockets under a short temp root.
	if err != nil {
		t.Fatalf("create short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	viper.Set("spiffe_dir", filepath.Join(dir, "data"))
	viper.Set("spiffe_runtime_dir", filepath.Join(dir, "run"))

	cfg, err := startEmbeddedLocalSPIFFE(nil)
	if err != nil {
		t.Fatalf("startEmbeddedLocalSPIFFE: %v", err)
	}
	t.Cleanup(cfg.Authority.Stop)

	if cfg.Authority == nil {
		t.Fatal("embedded local SPIFFE did not start authority")
	}

	if !viper.GetBool("spiffe_enabled") {
		t.Fatal("embedded local SPIFFE did not enable local identity mode")
	}

	for _, path := range []string{cfg.ServerSocket, cfg.AgentSocket, cfg.BundleFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected embedded local SPIFFE file %s: %v", path, err)
		}
	}

	if got := viper.GetString("spiffe_workload_api_address"); got != "unix://"+cfg.AgentSocket {
		t.Fatalf("workload API address = %q", got)
	}

	if got := viper.GetString("spiffe_registration_server_address"); got != "unix://"+cfg.ServerSocket {
		t.Fatalf("registration API address = %q", got)
	}
}

func shortTempRoot() string {
	if runtime.GOOS == "windows" {
		return ""
	}

	return "/tmp"
}

func TestLocalSPIFFERejectsPlaintextGRPC(t *testing.T) {
	resetLocalTestConfig(t)
	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	viper.Set("spiffe_enabled", true)
	viper.Set("grpc_insecure", true)

	_, err = localSPIFFE(t.TempDir(), m)
	if err == nil || !strings.Contains(err.Error(), "requires gRPC TLS") {
		t.Fatalf("localSPIFFE error = %v, want gRPC TLS requirement", err)
	}
}

func TestCleanCommaSeparatedDeduplicatesValues(t *testing.T) {
	got := cleanCommaSeparated([]string{" unix:uid:501,unix:gid:20 ", "unix:uid:501", "", "unix:path:/bin/worker"})
	want := []string{"unix:uid:501", "unix:gid:20", "unix:path:/bin/worker"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cleanCommaSeparated = %v, want %v", got, want)
	}
}

func TestValidLocalHTTPSTLSMode(t *testing.T) {
	for _, mode := range []string{localHTTPSTLSAuto, localHTTPSTLSOn, localHTTPSTLSOff} {
		if !validLocalHTTPSTLSMode(mode) {
			t.Fatalf("mode %q should be valid", mode)
		}
	}

	if validLocalHTTPSTLSMode("sometimes") {
		t.Fatal("unexpectedly accepted invalid mode")
	}
}

func TestAPIEnvConfigAsCodeRepositories(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("VECTIS_LOCAL_SOURCE_REPOSITORIES", "")
	viper.Set("host", "127.0.0.1")
	viper.Set("config_as_code", true)

	tmp := t.TempDir()
	repoDir := filepath.Join(tmp, "repo")
	if err := os.MkdirAll(repoDir, 0o755); err != nil {
		t.Fatalf("mkdir repo: %v", err)
	}

	relativeRoot := filepath.Join(tmp, "cwd")
	relativeRepo := filepath.Join(relativeRoot, "relative-repo")
	if err := os.MkdirAll(relativeRepo, 0o755); err != nil {
		t.Fatalf("mkdir relative repo: %v", err)
	}

	t.Chdir(relativeRoot)
	viper.Set("source_repositories", []string{
		"vectis-local=" + repoDir,
		"relative=relative-repo",
	})

	env, err := apiEnv()
	if err != nil {
		t.Fatalf("apiEnv: %v", err)
	}

	if !hasEnv(env, "VECTIS_API_SERVER_HOST=127.0.0.1") {
		t.Fatalf("api env missing host: %v", env)
	}

	if !hasEnv(env, "VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP=true") {
		t.Fatalf("api env missing source sync startup: %v", env)
	}

	raw, ok := envValue(env, "VECTIS_SOURCE_REPOSITORIES")
	if !ok {
		t.Fatalf("api env missing source repositories: %v", env)
	}

	var repos []config.SourceRepositoryDeclaration
	if err := json.Unmarshal([]byte(raw), &repos); err != nil {
		t.Fatalf("decode source repositories: %v", err)
	}

	if len(repos) != 2 {
		t.Fatalf("source repositories=%+v, want 2", repos)
	}

	if repos[0].RepositoryID != "vectis-local" ||
		repos[0].CheckoutPath != repoDir ||
		repos[0].SourceKind != "local_checkout" ||
		repos[0].CheckoutMode != "external" {
		t.Fatalf("first source repository mismatch: %+v", repos[0])
	}

	if repos[1].RepositoryID != "relative" || repos[1].CheckoutPath != relativeRepo {
		t.Fatalf("relative source repository mismatch: %+v", repos[1])
	}
}

func TestAPIEnvConfigAsCodeRequiresRepository(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("VECTIS_LOCAL_SOURCE_REPOSITORIES", "")
	viper.Set("config_as_code", true)

	if _, err := apiEnv(); err == nil || !strings.Contains(err.Error(), "config-as-code requires") {
		t.Fatalf("expected config-as-code repository error, got %v", err)
	}
}

func TestAPIEnvRejectsMalformedSourceRepository(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("source_repositories", []string{"not-a-mapping"})

	_, err := apiEnv()
	if err == nil || !strings.Contains(err.Error(), "repository_id=checkout_path") {
		t.Fatalf("apiEnv error = %v, want repository mapping error", err)
	}
}

func TestLocalBootstrapTokenUsesEnv(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "configured-bootstrap-token")

	token, source, err := localBootstrapToken()
	if err != nil {
		t.Fatalf("localBootstrapToken: %v", err)
	}

	if token != "configured-bootstrap-token" {
		t.Fatalf("token = %q, want env token", token)
	}

	if !strings.Contains(source, "VECTIS_API_AUTH_BOOTSTRAP_TOKEN") {
		t.Fatalf("source = %q, want env source", source)
	}
}

func TestLocalBootstrapTokenPersistsGeneratedToken(t *testing.T) {
	resetLocalTestConfig(t)
	dataHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)

	token, source, err := localBootstrapToken()
	if err != nil {
		t.Fatalf("localBootstrapToken: %v", err)
	}

	if len(token) != localBootstrapBytes*2 {
		t.Fatalf("token length = %d, want %d", len(token), localBootstrapBytes*2)
	}

	if !strings.Contains(source, "written to") {
		t.Fatalf("source = %q, want written source", source)
	}

	path := filepath.Join(dataHome, "vectis", localBootstrapFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat bootstrap token: %v", err)
	}

	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %v, want 0600", got)
	}

	next, source, err := localBootstrapToken()
	if err != nil {
		t.Fatalf("second localBootstrapToken: %v", err)
	}

	if next != token {
		t.Fatalf("second token = %q, want persisted token %q", next, token)
	}

	if !strings.Contains(source, path) {
		t.Fatalf("source = %q, want path %q", source, path)
	}
}

func TestLocalServices_HAProfileBuildsMultiInstanceCell(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("VECTIS_CELL_ID", "iad-a")
	viper.Set("profile", localProfileHA)
	viper.Set("docs_enabled", false)

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	services := localServices(mocks.NopLogger{}, topology)

	wantCounts := map[string]int{
		"vectis-registry":     3,
		"vectis-queue":        2,
		"vectis-log":          2,
		"vectis-artifact":     2,
		"vectis-secrets":      1,
		"vectis-api":          2,
		"vectis-cell-ingress": 1,
		"vectis-worker-core":  1,
		"vectis-worker":       2,
		"vectis-cron":         2,
		"vectis-reconciler":   2,
		"vectis-catalog":      1,
	}

	gotCounts := map[string]int{}
	for _, svc := range services {
		gotCounts[svc.binary]++
	}

	for binary, want := range wantCounts {
		if gotCounts[binary] != want {
			t.Fatalf("%s count = %d, want %d; services=%v", binary, gotCounts[binary], want, serviceNames(services))
		}
	}

	var foundQueue2 bool
	var foundArtifact2 bool
	var foundRegistryPeers bool
	var foundSecrets bool
	var workersUseRegistry bool
	var foundWorker1ShellSocket bool
	var foundWorker2ShellSocket bool
	var foundWorkerCoreMetricsPort bool
	var workersUseSecrets bool
	for _, svc := range services {
		if svc.name == "queue-2" &&
			hasEnv(svc.env, "VECTIS_CELL_ID=iad-a") &&
			hasEnv(svc.env, "VECTIS_QUEUE_PORT=8181") &&
			hasEnv(svc.env, "VECTIS_QUEUE_INSTANCE_ID=queue-2") {
			foundQueue2 = true
		}

		if svc.name == "artifact-2" &&
			hasEnv(svc.env, "VECTIS_ARTIFACT_GRPC_PORT=8186") &&
			hasEnv(svc.env, "VECTIS_ARTIFACT_INSTANCE_ID=artifact-2") {
			foundArtifact2 = true
		}

		if svc.name == "registry-1" &&
			envContains(svc.env, "VECTIS_REGISTRY_CLUSTER_PEER_ADDRESSES=", "localhost:8182") &&
			envContains(svc.env, "VECTIS_REGISTRY_CLUSTER_PEER_ADDRESSES=", "localhost:8282") {
			foundRegistryPeers = true
		}

		if svc.name == "vectis-secrets[iad-a]" &&
			hasEnv(svc.env, "VECTIS_SECRETS_PORT=8090") &&
			hasEnvPrefix(svc.env, "VECTIS_SECRETS_ENCRYPTEDFS_ROOT=") &&
			hasEnvPrefix(svc.env, "VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE=") &&
			hasEnv(svc.env, "VECTIS_SECRETS_POLICY_ALLOW=namespace=*;job=*;task=*;ref=encryptedfs://*") {
			foundSecrets = true
		}

		if svc.name == "worker-1" && !hasEnvPrefix(svc.env, "VECTIS_WORKER_QUEUE_ADDRESS=") {
			workersUseRegistry = true
		}

		if svc.name == "worker-1" &&
			hasEnv(svc.env, "VECTIS_WORKER_CORE_SHELL_SOCKET="+localWorkerCoreShellSocket("worker-1")) {
			foundWorker1ShellSocket = true
		}

		if svc.name == "worker-2" &&
			hasEnv(svc.env, "VECTIS_WORKER_CORE_SHELL_SOCKET="+localWorkerCoreShellSocket("worker-2")) {
			foundWorker2ShellSocket = true
		}

		if svc.name == "worker-core" && hasEnv(svc.env, "VECTIS_WORKER_CORE_METRICS_PORT=9092") {
			foundWorkerCoreMetricsPort = true
		}

		if svc.name == "worker-1" && hasEnv(svc.env, "VECTIS_WORKER_SECRETS_ADDRESS=localhost:8090") {
			workersUseSecrets = true
		}
	}

	if !foundQueue2 {
		t.Fatalf("queue-2 did not include expected HA env: %+v", services)
	}

	if !foundArtifact2 {
		t.Fatalf("artifact-2 did not include expected HA env: %+v", services)
	}

	if !foundRegistryPeers {
		t.Fatalf("registry-1 did not include expected peer env: %+v", services)
	}

	if !foundSecrets {
		t.Fatalf("vectis-secrets did not include expected env: %+v", services)
	}

	if !workersUseRegistry {
		t.Fatalf("worker-1 did not rely on registry queue discovery: %+v", services)
	}

	if !foundWorker1ShellSocket || !foundWorker2ShellSocket {
		t.Fatalf("HA workers did not include distinct core shell sockets: %+v", services)
	}

	if !foundWorkerCoreMetricsPort {
		t.Fatalf("worker-core did not include metrics port env: %+v", services)
	}

	if !workersUseSecrets {
		t.Fatalf("worker-1 did not include secrets address: %+v", services)
	}
}

func TestLocalServicesPlaintextGRPCSkipsSecrets(t *testing.T) {
	for _, profile := range []string{localProfileSimple, localProfileHA} {
		t.Run(profile, func(t *testing.T) {
			resetLocalTestConfig(t)
			t.Setenv("XDG_DATA_HOME", t.TempDir())
			t.Setenv("VECTIS_CELL_ID", "iad-a")
			viper.Set("profile", profile)
			viper.Set("grpc_insecure", true)
			viper.Set("docs_enabled", false)

			topology, err := buildLocalTopology()
			if err != nil {
				t.Fatalf("buildLocalTopology: %v", err)
			}

			services := localServices(mocks.NopLogger{}, topology)
			workers := 0
			for _, svc := range services {
				if svc.binary == "vectis-secrets" {
					t.Fatalf("plaintext local services included vectis-secrets: %v", serviceNames(services))
				}

				if svc.binary == "vectis-worker" {
					workers++
					if !hasEnv(svc.env, "VECTIS_WORKER_SECRETS_ADDRESS=disabled") {
						t.Fatalf("plaintext worker did not disable secrets address: %+v", svc.env)
					}
				}
			}

			if workers == 0 {
				t.Fatalf("plaintext local services did not include a worker: %v", serviceNames(services))
			}
		})
	}
}

func TestLocalServices_SimpleProfileHealthPortsUseEnvOverrides(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("VECTIS_REGISTRY_PORT", "18082")
	t.Setenv("VECTIS_LOG_GRPC_PORT", "18083")
	t.Setenv("VECTIS_ARTIFACT_GRPC_PORT", "18086")
	t.Setenv("VECTIS_ORCHESTRATOR_PORT", "18087")
	viper.Set("docs_enabled", false)

	topology, err := buildLocalTopology()
	if err != nil {
		t.Fatalf("buildLocalTopology: %v", err)
	}

	services := localServices(mocks.NopLogger{}, topology)
	want := map[string]int{
		"vectis-registry":     18082,
		"vectis-log":          18083,
		"vectis-artifact":     18086,
		"vectis-orchestrator": 18087,
	}

	for binary, port := range want {
		var found bool
		for _, svc := range services {
			if svc.binary != binary {
				continue
			}

			found = true
			if svc.portFn == nil {
				t.Fatalf("%s portFn is nil", binary)
			}

			if got := svc.portFn(); got != port {
				t.Fatalf("%s health port = %d, want %d", binary, got, port)
			}
		}

		if !found {
			t.Fatalf("service %s not found in %+v", binary, services)
		}
	}
}

func TestLocalDiscoveryRegistryEnv(t *testing.T) {
	resetLocalTestConfig(t)
	if got := localDiscoveryRegistryEnv(localProfileSimple); !reflect.DeepEqual(got, []string{"VECTIS_DISCOVERY_REGISTRY_ADDRESSES=localhost:8082"}) {
		t.Fatalf("default simple discovery env = %v", got)
	}

	t.Setenv("VECTIS_REGISTRY_PORT", "18082")
	if got := localDiscoveryRegistryEnv(localProfileSimple); !reflect.DeepEqual(got, []string{"VECTIS_DISCOVERY_REGISTRY_ADDRESSES=localhost:18082"}) {
		t.Fatalf("overridden simple discovery env = %v", got)
	}
}

func TestLocalDiscoveryRegistryEnvPreservesOperatorOverride(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("VECTIS_REGISTRY_PORT", "18082")
	t.Setenv("VECTIS_DISCOVERY_REGISTRY_ADDRESSES", "reg-a:8082,reg-b:8082")

	if got := localDiscoveryRegistryEnv(localProfileSimple); len(got) != 0 {
		t.Fatalf("simple discovery env with operator override = %v, want none", got)
	}

	resetLocalTestConfig(t)
	t.Setenv("VECTIS_REGISTRY_PORT", "18082")
	t.Setenv("VECTIS_DISCOVERY_REGISTRY_ADDRESSES", "")
	t.Setenv("VECTIS_WORKER_REGISTRY_ADDRESS", "worker-registry:8082")

	if got := localDiscoveryRegistryEnv(localProfileSimple); len(got) != 0 {
		t.Fatalf("simple discovery env with role override = %v, want none", got)
	}
}

func TestLocalDiscoveryRegistryEnvSkipsHAProfile(t *testing.T) {
	resetLocalTestConfig(t)
	t.Setenv("VECTIS_REGISTRY_PORT", "18082")

	if got := localDiscoveryRegistryEnv(localProfileHA); len(got) != 0 {
		t.Fatalf("HA discovery env = %v, want none", got)
	}
}

func hasEnv(env []string, want string) bool {
	return slices.Contains(env, want)
}

func hasEnvPrefix(env []string, prefix string) bool {
	for _, got := range env {
		if strings.HasPrefix(got, prefix) {
			return true
		}
	}

	return false
}

func envValue(env []string, key string) (string, bool) {
	prefix := key + "="
	for _, got := range env {
		if strings.HasPrefix(got, prefix) {
			return strings.TrimPrefix(got, prefix), true
		}
	}

	return "", false
}

func envContains(env []string, prefix, want string) bool {
	for _, got := range env {
		if strings.HasPrefix(got, prefix) && strings.Contains(got, want) {
			return true
		}
	}

	return false
}

func TestUIEnvIncludesDevAssetsURLWhenEnabled(t *testing.T) {
	withViperSettings(t, map[string]any{
		"host":                  "localhost",
		"ui_enabled":            true,
		"ui_port":               8089,
		"ui_dir":                "",
		"ui_dev_assets_enabled": true,
		"ui_dev_assets_host":    "127.0.0.1",
		"ui_dev_assets_port":    5173,
	})

	env := uiEnv()
	if !containsEnv(env, "VECTIS_UI_DEV_ASSETS_URL=http://127.0.0.1:5173") {
		t.Fatalf("uiEnv() = %v, want dev assets URL", env)
	}
}

func TestUIEnvOmitsDevAssetsURLByDefault(t *testing.T) {
	withViperSettings(t, map[string]any{
		"host":                  "localhost",
		"ui_enabled":            true,
		"ui_port":               8089,
		"ui_dir":                "",
		"ui_dev_assets_enabled": false,
	})

	env := uiEnv()
	for _, value := range env {
		if strings.HasPrefix(value, "VECTIS_UI_DEV_ASSETS_URL=") {
			t.Fatalf("uiEnv() = %v, did not expect dev assets URL", env)
		}
	}
}

func TestValidateUIDevAssetsConfigRequiresUI(t *testing.T) {
	withViperSettings(t, map[string]any{
		"ui_enabled":            false,
		"ui_dev_assets_enabled": true,
	})

	err := validateUIDevAssetsConfig()
	if err == nil || !strings.Contains(err.Error(), "--ui-dev-assets requires the local UI") {
		t.Fatalf("validateUIDevAssetsConfig() = %v, want UI required error", err)
	}
}

func TestValidateUIDevAssetsDirRequiresPackageJSON(t *testing.T) {
	dir := t.TempDir()

	err := validateUIDevAssetsDir(dir)
	if err == nil || !strings.Contains(err.Error(), "does not contain package.json") {
		t.Fatalf("validateUIDevAssetsDir() = %v, want package.json error", err)
	}
}

func TestValidateUIDevAssetsDirAcceptsUIPackage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "package.json"), []byte(`{"scripts":{"dev":"vite"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := validateUIDevAssetsDir(dir); err != nil {
		t.Fatalf("validateUIDevAssetsDir() = %v, want nil", err)
	}
}

func withViperSettings(t *testing.T, settings map[string]any) {
	t.Helper()

	previous := make(map[string]any, len(settings))
	for key := range settings {
		previous[key] = viper.Get(key)
	}

	for key, value := range settings {
		viper.Set(key, value)
	}

	t.Cleanup(func() {
		for key, value := range previous {
			viper.Set(key, value)
		}
	})
}

func containsEnv(env []string, want string) bool {
	for _, value := range env {
		if value == want {
			return true
		}
	}

	return false
}
