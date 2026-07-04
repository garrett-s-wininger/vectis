package linux

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

const (
	commonEnvFile  = "-/etc/vectis/vectis.env"
	migrationUnit  = "vectis-db-migrate.service"
	networkUnit    = "network-online.target"
	retentionUnit  = "vectis-retention-scheduled-cleanup.service"
	retentionTimer = "vectis-retention-scheduled-cleanup.timer"
	targetUnit     = "vectis.target"
)

var expectedStandaloneExecs = map[string]string{
	"vectis-api.service":               "/usr/bin/vectis-api",
	"vectis-artifact.service":          "/usr/bin/vectis-artifact",
	"vectis-catalog.service":           "/usr/bin/vectis-catalog",
	"vectis-cell-ingress.service":      "/usr/bin/vectis-cell-ingress",
	"vectis-cron.service":              "/usr/bin/vectis-cron",
	"vectis-docs.service":              "/usr/bin/vectis-docs",
	"vectis-log.service":               "/usr/bin/vectis-log",
	"vectis-log-forwarder.service":     "/usr/bin/vectis-log-forwarder",
	"vectis-orchestrator.service":      "/usr/bin/vectis-orchestrator",
	"vectis-queue.service":             "/usr/bin/vectis-queue",
	"vectis-reconciler.service":        "/usr/bin/vectis-reconciler",
	"vectis-registry.service":          "/usr/bin/vectis-registry",
	"vectis-scm-gerrit-stream.service": "/usr/bin/vectis-scm-gerrit-stream",
	"vectis-scm-poller.service":        "/usr/bin/vectis-scm-poller",
	"vectis-secrets.service":           "/usr/bin/vectis-secrets",
	"vectis-spiffe.service":            "/usr/bin/vectis-spiffe",
	"vectis-worker.service":            "/usr/bin/vectis-worker",
	"vectis-worker-core.service":       "/usr/bin/vectis-worker-core",
}

var expectedDBBackedServices = map[string]struct{}{
	"vectis-api.service":               {},
	"vectis-catalog.service":           {},
	"vectis-cell-ingress.service":      {},
	"vectis-cron.service":              {},
	"vectis-reconciler.service":        {},
	"vectis-scm-gerrit-stream.service": {},
	"vectis-scm-poller.service":        {},
	"vectis-secrets.service":           {},
	"vectis-worker.service":            {},
}

var expectedQueueProducerServices = map[string]struct{}{
	"vectis-cron.service":              {},
	"vectis-reconciler.service":        {},
	"vectis-scm-gerrit-stream.service": {},
	"vectis-scm-poller.service":        {},
}

func TestLinuxArtifactsRenderFromTOML(t *testing.T) {
	manifest := loadTestManifest(t)
	files, err := manifest.RenderFiles()
	if err != nil {
		t.Fatal(err)
	}

	got := sortedKeys(files)
	want := append(systemdPaths(t, manifest), envPaths(t, manifest)...)
	want = append(want, "sysusers.d/vectis.conf", "tmpfiles.d/vectis.conf", "install/manifest.json", "install/manifest.tsv")
	sort.Strings(want)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rendered file list mismatch\ngot:  %v\nwant: %v", got, want)
	}
}

func TestVectisTargetMembership(t *testing.T) {
	files := renderTestFiles(t)
	unit := parseUnit(t, "systemd/"+targetUnit, files["systemd/"+targetUnit])
	requireSections(t, unit, targetUnit, "Unit", "Install")

	wants := words(values(unit, "Unit", "Wants")...)
	if !reflect.DeepEqual(wants, []string{networkUnit}) {
		t.Fatalf("vectis.target Wants = %v, want only %s", wants, networkUnit)
	}

	after := words(values(unit, "Unit", "After")...)
	if !reflect.DeepEqual(after, []string{networkUnit}) {
		t.Fatalf("%s After = %v, want only %s", targetUnit, after, networkUnit)
	}

	for _, service := range append(sortedKeys(expectedStandaloneExecs), migrationUnit) {
		if contains(wants, service) {
			t.Fatalf("%s should attach through service WantedBy, not target Wants: %v", service, wants)
		}
	}
}

func TestStandaloneServiceUnitContract(t *testing.T) {
	files := renderTestFiles(t)
	for service, execStart := range expectedStandaloneExecs {
		t.Run(service, func(t *testing.T) {
			unit := parseUnit(t, "systemd/"+service, files["systemd/"+service])
			requireSections(t, unit, service, "Unit", "Service", "Install")
			requireValue(t, unit, "Service", "Type", "simple")
			requireValue(t, unit, "Service", "ExecStart", execStart)
			requireBaseServiceContract(t, unit)
			requireValue(t, unit, "Install", "WantedBy", targetUnit)
			requireWord(t, unit, "Unit", "PartOf", targetUnit)
			requireWord(t, unit, "Unit", "Wants", networkUnit)
			requireWord(t, unit, "Unit", "After", networkUnit)
			requireValue(t, unit, "Service", "Restart", "on-failure")
			requireValue(t, unit, "Service", "RestartSec", "5s")
			requireValue(t, unit, "Service", "RuntimeDirectory", "vectis")
			requireValue(t, unit, "Service", "RuntimeDirectoryMode", "0750")
			requireValue(t, unit, "Service", "RuntimeDirectoryPreserve", "yes")
			requireEnvFiles(t, unit, service, true)
			requireNoInlineEnvironment(t, unit, service)

			if _, queueProducer := expectedQueueProducerServices[service]; queueProducer {
				requireWord(t, unit, "Unit", "Wants", "vectis-registry.service")
				requireWord(t, unit, "Unit", "Wants", "vectis-queue.service")
				requireWord(t, unit, "Unit", "After", "vectis-registry.service")
				requireWord(t, unit, "Unit", "After", "vectis-queue.service")
			}

			_, dbBacked := expectedDBBackedServices[service]
			if dbBacked {
				requireWord(t, unit, "Unit", "Requires", migrationUnit)
				requireWord(t, unit, "Unit", "After", migrationUnit)
				return
			}

			if contains(words(values(unit, "Unit", "Requires")...), migrationUnit) {
				t.Fatalf("%s unexpectedly Requires %s", service, migrationUnit)
			}
		})
	}
}

func TestMigrationUnitContract(t *testing.T) {
	files := renderTestFiles(t)
	unit := parseUnit(t, "systemd/"+migrationUnit, files["systemd/"+migrationUnit])

	requireSections(t, unit, migrationUnit, "Unit", "Service", "Install")
	requireValue(t, unit, "Service", "Type", "oneshot")
	requireValue(t, unit, "Service", "ExecStart", "/usr/bin/vectis-cli database migrate")
	requireBaseServiceContract(t, unit)
	requireValue(t, unit, "Install", "WantedBy", targetUnit)
	requireEnvFiles(t, unit, migrationUnit, true)
	requireNoInlineEnvironment(t, unit, migrationUnit)

	if values(unit, "Service", "Restart") != nil {
		t.Fatalf("%s should not declare Restart", migrationUnit)
	}
}

func TestRetentionScheduledCleanupUnitAndTimerContract(t *testing.T) {
	files := renderTestFiles(t)
	unit := parseUnit(t, "systemd/"+retentionUnit, files["systemd/"+retentionUnit])

	requireSections(t, unit, retentionUnit, "Unit", "Service")
	if unit["Install"] != nil {
		t.Fatalf("%s should be static and timer-triggered, got [Install]: %v", retentionUnit, unit["Install"])
	}

	requireValue(t, unit, "Service", "Type", "oneshot")
	requireBaseServiceContract(t, unit)
	requireEnvFiles(t, unit, retentionUnit, true)
	requireNoInlineEnvironment(t, unit, retentionUnit)
	requireWord(t, unit, "Unit", "Requires", migrationUnit)
	requireWord(t, unit, "Unit", "Wants", networkUnit)
	requireWord(t, unit, "Unit", "Wants", "vectis-api.service")
	requireWord(t, unit, "Unit", "After", networkUnit)
	requireWord(t, unit, "Unit", "After", migrationUnit)
	requireWord(t, unit, "Unit", "After", "vectis-api.service")

	execStart := strings.Join(values(unit, "Service", "ExecStart"), "\n")
	for _, want := range []string{
		"/usr/bin/vectis-cli --format json retention scheduled-cleanup --yes",
		"--backup-manifest /var/lib/vectis/ops/backup-manifest.json",
		"--backup-expect /etc/vectis/expected-topology.json",
		"--audit-export-output /var/lib/vectis/ops/audit-export.json",
		"--hold-review-output /var/lib/vectis/ops/hold-review.json",
		"--require-backup-manifest",
		"--require-audit-export",
		"--require-hold-review",
		"--evidence-manifest-promote /var/lib/vectis/ops/retention-cleanup-evidence.json",
		"> /var/lib/vectis/ops/retention-scheduled-cleanup.json",
		"/usr/bin/vectis-cli retention evidence metrics",
		"--scheduled-cleanup /var/lib/vectis/ops/retention-scheduled-cleanup.json",
		"--output /var/lib/vectis/ops/retention-evidence.prom",
	} {
		if !strings.Contains(execStart, want) {
			t.Fatalf("%s ExecStart missing %q:\n%s", retentionUnit, want, execStart)
		}
	}

	if values(unit, "Service", "Restart") != nil {
		t.Fatalf("%s should not declare Restart", retentionUnit)
	}

	env := parseEnvExample(t, "env/vectis-retention-scheduled-cleanup.env.example", files["env/vectis-retention-scheduled-cleanup.env.example"])
	for _, key := range []string{"VECTIS_API_SERVER_HOST", "VECTIS_API_SERVER_PORT", "VECTIS_API_TOKEN"} {
		if _, ok := env[key]; !ok {
			t.Fatalf("retention scheduled cleanup env example missing %s", key)
		}
	}

	timer := parseUnit(t, "systemd/"+retentionTimer, files["systemd/"+retentionTimer])
	requireSections(t, timer, retentionTimer, "Unit", "Timer", "Install")
	requireValue(t, timer, "Timer", "OnCalendar", "Sun *-*-* 03:00:00")
	requireValue(t, timer, "Timer", "Persistent", "true")
	requireValue(t, timer, "Timer", "Unit", retentionUnit)
	requireValue(t, timer, "Install", "WantedBy", "timers.target")
}

func TestEnvExamples(t *testing.T) {
	files := renderTestFiles(t)
	common := parseEnvExample(t, "env/vectis.env.example", files["env/vectis.env.example"])
	if !strings.Contains(files["env/vectis.env.example"], "Example only") {
		t.Fatalf("common env example must declare that config management owns real env files")
	}
	for _, key := range []string{
		"XDG_DATA_HOME",
		"XDG_RUNTIME_DIR",
		"VECTIS_DATABASE_DRIVER",
		"VECTIS_DATABASE_DSN",
		"VECTIS_DISCOVERY_REGISTRY_ADDRESS",
		"VECTIS_GRPC_TLS_INSECURE",
		"VECTIS_METRICS_TLS_INSECURE",
		"VECTIS_LOG_DIR",
	} {
		if _, ok := common[key]; !ok {
			t.Fatalf("common env example missing %s", key)
		}
	}

	if common["VECTIS_DATABASE_DRIVER"] != "pgx" {
		t.Fatalf("common env should be Postgres-first, got driver %q", common["VECTIS_DATABASE_DRIVER"])
	}

	for path, content := range files {
		if strings.HasPrefix(path, "env/") {
			if !strings.Contains(content, "Config management") {
				t.Fatalf("%s must mention config management ownership", path)
			}
			parseEnvExample(t, path, content)
		}
	}
}

func TestInstallManifestContract(t *testing.T) {
	files := renderTestFiles(t)

	var entries []InstallEntry
	if err := json.Unmarshal([]byte(files["install/manifest.json"]), &entries); err != nil {
		t.Fatalf("install manifest JSON is invalid: %v", err)
	}

	if len(entries) == 0 {
		t.Fatalf("install manifest did not include entries")
	}

	covered := map[string]InstallEntry{}
	for _, entry := range entries {
		if entry.Source == "" || entry.Destination == "" || entry.Mode == "" || entry.Owner == "" || entry.Group == "" || entry.Kind == "" {
			t.Fatalf("install entry has empty field: %+v", entry)
		}

		if _, ok := files[entry.Source]; !ok {
			t.Fatalf("install manifest source %s was not rendered", entry.Source)
		}

		if strings.HasPrefix(entry.Source, "install/") {
			t.Fatalf("install manifest should not install itself: %+v", entry)
		}

		covered[entry.Source] = entry
	}

	for path := range files {
		if strings.HasPrefix(path, "install/") {
			continue
		}

		if _, ok := covered[path]; !ok {
			t.Fatalf("rendered artifact %s missing from install manifest", path)
		}
	}

	requireInstallDestination(t, covered, "systemd/"+targetUnit, "/etc/systemd/system/"+targetUnit, "0644", "root", "root")
	requireInstallDestination(t, covered, "systemd/"+retentionUnit, "/etc/systemd/system/"+retentionUnit, "0644", "root", "root")
	requireInstallDestination(t, covered, "systemd/"+retentionTimer, "/etc/systemd/system/"+retentionTimer, "0644", "root", "root")
	requireInstallDestination(t, covered, "env/vectis.env.example", "/etc/vectis/vectis.env", "0640", "root", "vectis")
	requireInstallDestination(t, covered, "env/vectis-retention-scheduled-cleanup.env.example", "/etc/vectis/vectis-retention-scheduled-cleanup.env", "0640", "root", "vectis")
	requireInstallDestination(t, covered, "sysusers.d/vectis.conf", "/usr/lib/sysusers.d/vectis.conf", "0644", "root", "root")
	requireInstallDestination(t, covered, "tmpfiles.d/vectis.conf", "/usr/lib/tmpfiles.d/vectis.conf", "0644", "root", "root")

	if !strings.Contains(files["install/manifest.tsv"], "env/vectis.env.example\t/etc/vectis/vectis.env\t0640\troot\tvectis\tenv") {
		t.Fatalf("install TSV missing common env entry:\n%s", files["install/manifest.tsv"])
	}
}

func TestSysusersAndTmpfiles(t *testing.T) {
	files := renderTestFiles(t)
	sysusers := files["sysusers.d/vectis.conf"]
	if !strings.Contains(sysusers, "u vectis ") {
		t.Fatalf("sysusers file does not declare vectis user:\n%s", sysusers)
	}

	tmpfiles := files["tmpfiles.d/vectis.conf"]
	for _, requiredPath := range []string{"/etc/vectis", "/var/lib/vectis", "/var/lib/vectis/ops", "/var/log/vectis", "/run/vectis"} {
		if !strings.Contains(tmpfiles, requiredPath) {
			t.Fatalf("tmpfiles missing %s:\n%s", requiredPath, tmpfiles)
		}
	}
}

func TestWriteFiles(t *testing.T) {
	files := renderTestFiles(t)
	out := t.TempDir()
	if err := WriteFiles(out, files); err != nil {
		t.Fatal(err)
	}

	for path, want := range files {
		got, err := os.ReadFile(filepath.Join(out, path))
		if err != nil {
			t.Fatal(err)
		}

		if string(got) != want {
			t.Fatalf("%s content changed while writing", path)
		}
	}
}

func TestRenderToDirUsesEmbeddedDefaultManifest(t *testing.T) {
	out := t.TempDir()
	result, err := RenderToDir(RenderOptions{OutDir: out})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "rendered" {
		t.Fatalf("status = %q, want rendered", result.Status)
	}

	if result.ManifestPath != DefaultManifestPath {
		t.Fatalf("manifest path = %q, want %q", result.ManifestPath, DefaultManifestPath)
	}

	if result.Files == 0 {
		t.Fatalf("rendered no files")
	}

	if _, err := os.Stat(filepath.Join(out, "systemd", targetUnit)); err != nil {
		t.Fatalf("embedded default render missing target unit: %v", err)
	}
}

func TestPrepareVMSmokeArtifactDirUsesTemporaryDirectory(t *testing.T) {
	opts := VMSmokeOptions{}
	cleanup, err := prepareVMSmokeArtifactDir(&opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(opts.ArtifactDir) })

	if opts.ArtifactDir == "" {
		t.Fatalf("artifact dir was not set")
	}

	if !cleanup {
		t.Fatalf("expected temporary artifact dir to be marked for cleanup")
	}

	if strings.Contains(opts.ArtifactDir, DefaultArtifactDir) {
		t.Fatalf("temporary Lima artifact dir should not default to repo artifacts path: %s", opts.ArtifactDir)
	}
}

type parsedUnit map[string]map[string][]string

func loadTestManifest(t *testing.T) Manifest {
	t.Helper()
	manifest, err := LoadManifest("services.toml")
	if err != nil {
		t.Fatal(err)
	}

	return manifest
}

func renderTestFiles(t *testing.T) map[string]string {
	t.Helper()
	files, err := loadTestManifest(t).RenderFiles()
	if err != nil {
		t.Fatal(err)
	}

	return files
}

func parseUnit(t *testing.T, name, content string) parsedUnit {
	t.Helper()
	if content == "" {
		t.Fatalf("%s was not rendered", name)
	}

	unit := parsedUnit{}
	var section string
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			if unit[section] == nil {
				unit[section] = map[string][]string{}
			}

			continue
		}

		if section == "" {
			t.Fatalf("%s: directive before section: %q", name, line)
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("%s: invalid directive %q", name, line)
		}

		key = strings.TrimSpace(key)
		if key == "" {
			t.Fatalf("%s: empty directive key in %q", name, line)
		}

		unit[section][key] = append(unit[section][key], strings.TrimSpace(value))
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	return unit
}

func requireSections(t *testing.T, unit parsedUnit, name string, requiredSections ...string) {
	t.Helper()
	for _, requiredSection := range requiredSections {
		if unit[requiredSection] == nil {
			t.Fatalf("%s missing [%s]", name, requiredSection)
		}
	}
}

func requireBaseServiceContract(t *testing.T, unit parsedUnit) {
	t.Helper()
	for key, want := range map[string]string{
		"User":             "vectis",
		"Group":            "vectis",
		"WorkingDirectory": "/var/lib/vectis",
		"StateDirectory":   "vectis",
		"LogsDirectory":    "vectis",
		"UMask":            "0027",
		"NoNewPrivileges":  "true",
		"PrivateTmp":       "true",
		"ProtectSystem":    "full",
		"ProtectHome":      "true",
	} {
		requireValue(t, unit, "Service", key, want)
	}
}

func requireEnvFiles(t *testing.T, unit parsedUnit, service string, wantsCommon bool) {
	t.Helper()
	envFiles := values(unit, "Service", "EnvironmentFile")
	serviceEnv := "-/etc/vectis/" + strings.TrimSuffix(service, ".service") + ".env"

	if wantsCommon {
		if !contains(envFiles, commonEnvFile) {
			t.Fatalf("%s missing common EnvironmentFile %s: %v", service, commonEnvFile, envFiles)
		}
	} else if contains(envFiles, commonEnvFile) {
		t.Fatalf("%s should not load standalone common env file: %v", service, envFiles)
	}

	if !contains(envFiles, serviceEnv) {
		t.Fatalf("%s missing service EnvironmentFile %s: %v", service, serviceEnv, envFiles)
	}
}

func requireNoInlineEnvironment(t *testing.T, unit parsedUnit, service string) {
	t.Helper()
	if values(unit, "Service", "Environment") != nil {
		t.Fatalf("%s must not embed config values with Environment=; use EnvironmentFile=", service)
	}
}

func requireInstallDestination(t *testing.T, entries map[string]InstallEntry, source, destination, mode, owner, group string) {
	t.Helper()
	entry, ok := entries[source]
	if !ok {
		t.Fatalf("missing install entry for %s", source)
	}

	if entry.Destination != destination || entry.Mode != mode || entry.Owner != owner || entry.Group != group {
		t.Fatalf("install entry for %s = %+v, want destination=%s mode=%s owner=%s group=%s", source, entry, destination, mode, owner, group)
	}
}

func requireValue(t *testing.T, unit parsedUnit, section, key, want string) {
	t.Helper()
	got := values(unit, section, key)
	if len(got) != 1 || got[0] != want {
		t.Fatalf("[%s] %s = %v, want exactly %q", section, key, got, want)
	}
}

func requireWord(t *testing.T, unit parsedUnit, section, key, want string) {
	t.Helper()
	got := words(values(unit, section, key)...)
	if !contains(got, want) {
		t.Fatalf("[%s] %s missing %q: %v", section, key, want, got)
	}
}

func values(unit parsedUnit, section, key string) []string {
	if unit[section] == nil {
		return nil
	}

	return unit[section][key]
}

func words(values ...string) []string {
	var out []string
	for _, value := range values {
		out = append(out, strings.Fields(value)...)
	}

	return out
}

func systemdPaths(t *testing.T, manifest Manifest) []string {
	t.Helper()
	paths := []string{filepath.ToSlash(filepath.Join("systemd", manifest.Target.Name))}
	for _, unit := range manifest.Units {
		paths = append(paths, filepath.ToSlash(filepath.Join("systemd", unit.UnitName())))
	}
	for _, timer := range manifest.Timers {
		paths = append(paths, filepath.ToSlash(filepath.Join("systemd", timer.TimerName())))
	}

	sort.Strings(paths)
	return paths
}

func envPaths(t *testing.T, manifest Manifest) []string {
	t.Helper()
	paths := []string{"env/vectis.env.example"}
	for _, unit := range manifest.Units {
		paths = append(paths, filepath.ToSlash(filepath.Join("env", strings.TrimSuffix(unit.UnitName(), ".service")+".env.example")))
	}

	sort.Strings(paths)
	return paths
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}

	return false
}

func parseEnvExample(t *testing.T, path, content string) map[string]string {
	t.Helper()

	keyRE := regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	env := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("%s: invalid env line %q", path, line)
		}

		if !keyRE.MatchString(key) {
			t.Fatalf("%s: invalid env key %q", path, key)
		}

		env[key] = value
	}

	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	return env
}
