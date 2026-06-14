package linux

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const (
	DefaultArtifactDir  = "artifacts/deploy/linux"
	DefaultManifestPath = "deploy/linux/services.toml"
)

//go:embed services.toml
var embeddedManifest []byte

type RenderOptions struct {
	ManifestPath string
	OutDir       string
}

type RenderResult struct {
	Status       string `json:"status"`
	ManifestPath string `json:"manifest_path"`
	OutputDir    string `json:"output_dir"`
	Files        int    `json:"files"`
}

type InstallEntry struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Mode        string `json:"mode"`
	Owner       string `json:"owner"`
	Group       string `json:"group"`
	Kind        string `json:"kind"`
}

type Manifest struct {
	Target           TargetConfig      `toml:"target"`
	Defaults         Defaults          `toml:"defaults"`
	CommonEnvExample map[string]string `toml:"common_env_example"`
	Tmpfiles         []TmpfileEntry    `toml:"tmpfiles"`
	Sysusers         []SysuserEntry    `toml:"sysusers"`
	Units            []Unit            `toml:"unit"`
}

type TargetConfig struct {
	Name          string `toml:"name"`
	Description   string `toml:"description"`
	Documentation string `toml:"documentation"`
}

type Defaults struct {
	NetworkUnit              string `toml:"network_unit"`
	MigrationUnit            string `toml:"migration_unit"`
	User                     string `toml:"user"`
	Group                    string `toml:"group"`
	WorkingDirectory         string `toml:"working_directory"`
	StateDirectory           string `toml:"state_directory"`
	CacheDirectory           string `toml:"cache_directory"`
	LogsDirectory            string `toml:"logs_directory"`
	RuntimeDirectory         string `toml:"runtime_directory"`
	RuntimeDirectoryMode     string `toml:"runtime_directory_mode"`
	RuntimeDirectoryPreserve string `toml:"runtime_directory_preserve"`
	CommonEnvFile            string `toml:"common_env_file"`
	Restart                  string `toml:"restart"`
	RestartSec               string `toml:"restart_sec"`
	TimeoutStopSec           string `toml:"timeout_stop_sec"`
	KillSignal               string `toml:"kill_signal"`
	UMask                    string `toml:"umask"`
	LimitNOFILE              string `toml:"limit_nofile"`
	NoNewPrivileges          string `toml:"no_new_privileges"`
	PrivateTmp               string `toml:"private_tmp"`
	ProtectSystem            string `toml:"protect_system"`
	ProtectHome              string `toml:"protect_home"`
}

type TmpfileEntry struct {
	Type  string `toml:"type"`
	Path  string `toml:"path"`
	Mode  string `toml:"mode"`
	User  string `toml:"user"`
	Group string `toml:"group"`
	Age   string `toml:"age"`
}

type SysuserEntry struct {
	Type  string `toml:"type"`
	Name  string `toml:"name"`
	ID    string `toml:"id"`
	Gecos string `toml:"gecos"`
	Home  string `toml:"home"`
	Shell string `toml:"shell"`
}

type Unit struct {
	ID              string            `toml:"id"`
	Description     string            `toml:"description"`
	ExecStart       string            `toml:"exec_start"`
	Type            string            `toml:"type"`
	TargetMember    bool              `toml:"target_member"`
	CommonEnv       bool              `toml:"common_env"`
	DBBacked        bool              `toml:"db_backed"`
	InstallWantedBy string            `toml:"install_wanted_by"`
	TimeoutStopSec  string            `toml:"timeout_stop_sec"`
	CacheDirectory  string            `toml:"cache_directory"`
	Wants           []string          `toml:"wants"`
	After           []string          `toml:"after"`
	EnvExample      map[string]string `toml:"env_example"`
}

func LoadManifest(path string) (Manifest, error) {
	if path == "" {
		path = DefaultManifestPath
	}

	var (
		b   []byte
		err error
	)

	if path == DefaultManifestPath {
		b = embeddedManifest
	} else {
		b, err = os.ReadFile(path)
		if err != nil {
			return Manifest{}, err
		}
	}

	var manifest Manifest
	if err := toml.Unmarshal(b, &manifest); err != nil {
		return Manifest{}, err
	}

	if err := manifest.validate(); err != nil {
		return Manifest{}, err
	}

	return manifest, nil
}

func RenderToDir(opts RenderOptions) (RenderResult, error) {
	if opts.ManifestPath == "" {
		opts.ManifestPath = DefaultManifestPath
	}

	if opts.OutDir == "" {
		return RenderResult{}, fmt.Errorf("output directory is required")
	}

	manifest, err := LoadManifest(opts.ManifestPath)
	if err != nil {
		return RenderResult{}, fmt.Errorf("load manifest: %w", err)
	}

	files, err := manifest.RenderFiles()
	if err != nil {
		return RenderResult{}, fmt.Errorf("render files: %w", err)
	}

	if err := WriteFiles(opts.OutDir, files); err != nil {
		return RenderResult{}, fmt.Errorf("write files: %w", err)
	}

	return RenderResult{
		Status:       "rendered",
		ManifestPath: opts.ManifestPath,
		OutputDir:    opts.OutDir,
		Files:        len(files),
	}, nil
}

func (m Manifest) RenderFiles() (map[string]string, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	files := map[string]string{}
	files[path.Join("systemd", m.Target.Name)] = m.renderTarget()
	files[path.Join("env", "vectis.env.example")] = renderEnvExample("Common Vectis environment.", m.CommonEnvExample)
	files[path.Join("sysusers.d", "vectis.conf")] = m.renderSysusers()
	files[path.Join("tmpfiles.d", "vectis.conf")] = m.renderTmpfiles()

	for _, unit := range m.Units {
		files[path.Join("systemd", unit.UnitName())] = m.renderUnit(unit)
		files[path.Join("env", strings.TrimSuffix(unit.UnitName(), ".service")+".env.example")] =
			renderEnvExample(fmt.Sprintf("Service-specific settings for %s.", unit.UnitName()), unit.EnvExample)
	}

	installPlan, err := m.InstallPlan()
	if err != nil {
		return nil, err
	}

	manifestJSON, err := renderInstallManifestJSON(installPlan)
	if err != nil {
		return nil, err
	}

	files[path.Join("install", "manifest.json")] = manifestJSON
	files[path.Join("install", "manifest.tsv")] = renderInstallManifestTSV(installPlan)

	return files, nil
}

func (m Manifest) InstallPlan() ([]InstallEntry, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}

	entries := []InstallEntry{
		installEntry("systemd/"+m.Target.Name, "/etc/systemd/system/"+m.Target.Name, "0644", "root", "root", "systemd"),
		installEntry("env/vectis.env.example", "/etc/vectis/vectis.env", "0640", "root", m.Defaults.Group, "env"),
		installEntry("sysusers.d/vectis.conf", "/usr/lib/sysusers.d/vectis.conf", "0644", "root", "root", "sysusers"),
		installEntry("tmpfiles.d/vectis.conf", "/usr/lib/tmpfiles.d/vectis.conf", "0644", "root", "root", "tmpfiles"),
	}

	for _, unit := range m.Units {
		unitName := unit.UnitName()
		envName := strings.TrimSuffix(unitName, ".service") + ".env"
		entries = append(entries,
			installEntry("systemd/"+unitName, "/etc/systemd/system/"+unitName, "0644", "root", "root", "systemd"),
			installEntry("env/"+envName+".example", "/etc/vectis/"+envName, "0640", "root", m.Defaults.Group, "env"),
		)
	}

	for _, entry := range entries {
		if err := entry.validate(); err != nil {
			return nil, err
		}
	}

	return entries, nil
}

func WriteFiles(root string, files map[string]string) error {
	paths := sortedKeys(files)
	for _, path := range paths {
		out := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}

		if err := os.WriteFile(out, []byte(files[path]), 0o644); err != nil {
			return err
		}
	}

	return nil
}

func installEntry(source, destination, mode, owner, group, kind string) InstallEntry {
	return InstallEntry{
		Source:      source,
		Destination: destination,
		Mode:        mode,
		Owner:       owner,
		Group:       group,
		Kind:        kind,
	}
}

func (e InstallEntry) validate() error {
	if e.Source == "" {
		return fmt.Errorf("install entry source is required")
	}

	if e.Destination == "" {
		return fmt.Errorf("install entry destination is required")
	}

	for _, value := range []string{e.Source, e.Destination, e.Mode, e.Owner, e.Group, e.Kind} {
		if strings.ContainsAny(value, "\t\r\n") {
			return fmt.Errorf("install entry contains unsupported control character: %q", value)
		}
	}

	return nil
}

func renderInstallManifestJSON(entries []InstallEntry) (string, error) {
	b, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return "", err
	}

	return string(append(b, '\n')), nil
}

func renderInstallManifestTSV(entries []InstallEntry) string {
	var b strings.Builder
	b.WriteString("# source\tdestination\tmode\towner\tgroup\tkind\n")
	for _, entry := range entries {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%s\n", entry.Source, entry.Destination, entry.Mode, entry.Owner, entry.Group, entry.Kind)
	}

	return b.String()
}

func (u Unit) UnitName() string {
	return "vectis-" + u.ID + ".service"
}

func (m Manifest) renderTarget() string {
	wants := []string{m.Defaults.NetworkUnit}
	after := []string{m.Defaults.NetworkUnit}

	return renderSections([]section{
		{
			Name: "Unit",
			Lines: []directive{
				{"Description", m.Target.Description},
				{"Documentation", m.Target.Documentation},
				{"Wants", joinUnits(wants)},
				{"After", joinUnits(after)},
			},
		},
		{
			Name: "Install",
			Lines: []directive{
				{"WantedBy", "multi-user.target"},
			},
		},
	})
}

func (m Manifest) renderUnit(unit Unit) string {
	unitType := unit.Type
	if unitType == "" {
		unitType = "simple"
	}

	execStart := unit.ExecStart
	if execStart == "" {
		execStart = "/usr/bin/vectis-" + unit.ID
	}

	wants := []string{m.Defaults.NetworkUnit}
	wants = append(wants, unitRefs(unit.Wants)...)

	after := []string{m.Defaults.NetworkUnit}
	if unit.DBBacked {
		after = append(after, m.Defaults.MigrationUnit)
	}

	after = append(after, unitRefs(unit.After)...)

	var requires []string
	if unit.DBBacked {
		requires = append(requires, m.Defaults.MigrationUnit)
	}

	unitLines := []directive{
		{"Description", unit.Description},
		{"Documentation", m.Target.Documentation},
	}

	if unit.TargetMember && unitType != "oneshot" {
		unitLines = append(unitLines, directive{"PartOf", m.Target.Name})
	}

	if len(requires) > 0 {
		unitLines = append(unitLines, directive{"Requires", joinUnits(requires)})
	}

	if len(wants) > 0 {
		unitLines = append(unitLines, directive{"Wants", joinUnits(wants)})
	}

	if len(after) > 0 {
		unitLines = append(unitLines, directive{"After", joinUnits(after)})
	}

	serviceLines := []directive{
		{"Type", unitType},
		{"User", m.Defaults.User},
		{"Group", m.Defaults.Group},
	}

	if unit.CommonEnv {
		serviceLines = append(serviceLines, directive{"EnvironmentFile", m.Defaults.CommonEnvFile})
	}

	serviceLines = append(serviceLines,
		directive{"EnvironmentFile", "-/etc/vectis/" + strings.TrimSuffix(unit.UnitName(), ".service") + ".env"},
		directive{"ExecStart", execStart},
	)

	if unitType != "oneshot" {
		serviceLines = append(serviceLines,
			directive{"Restart", m.Defaults.Restart},
			directive{"RestartSec", m.Defaults.RestartSec},
			directive{"TimeoutStopSec", valueOr(unit.TimeoutStopSec, m.Defaults.TimeoutStopSec)},
			directive{"KillSignal", m.Defaults.KillSignal},
		)
	}

	serviceLines = append(serviceLines,
		directive{"WorkingDirectory", m.Defaults.WorkingDirectory},
		directive{"StateDirectory", m.Defaults.StateDirectory},
	)

	if cacheDirectory := valueOr(unit.CacheDirectory, m.Defaults.CacheDirectory); cacheDirectory != "" {
		serviceLines = append(serviceLines, directive{"CacheDirectory", cacheDirectory})
	}

	serviceLines = append(serviceLines,
		directive{"LogsDirectory", m.Defaults.LogsDirectory},
	)

	if unitType != "oneshot" {
		serviceLines = append(serviceLines,
			directive{"RuntimeDirectory", m.Defaults.RuntimeDirectory},
			directive{"RuntimeDirectoryMode", m.Defaults.RuntimeDirectoryMode},
			directive{"RuntimeDirectoryPreserve", m.Defaults.RuntimeDirectoryPreserve},
		)
	}

	serviceLines = append(serviceLines,
		directive{"UMask", m.Defaults.UMask},
	)

	if unitType != "oneshot" {
		serviceLines = append(serviceLines, directive{"LimitNOFILE", m.Defaults.LimitNOFILE})
	}

	serviceLines = append(serviceLines,
		directive{"NoNewPrivileges", m.Defaults.NoNewPrivileges},
		directive{"PrivateTmp", m.Defaults.PrivateTmp},
		directive{"ProtectSystem", m.Defaults.ProtectSystem},
		directive{"ProtectHome", m.Defaults.ProtectHome},
	)

	installWantedBy := unit.InstallWantedBy
	if installWantedBy == "" {
		installWantedBy = m.Target.Name
	}

	return renderSections([]section{
		{Name: "Unit", Lines: unitLines},
		{Name: "Service", Lines: serviceLines},
		{Name: "Install", Lines: []directive{{"WantedBy", installWantedBy}}},
	})
}

func (m Manifest) renderSysusers() string {
	var b strings.Builder
	for _, entry := range m.Sysusers {
		fmt.Fprintf(&b, "%s %s %s %q %s %s\n", entry.Type, entry.Name, entry.ID, entry.Gecos, entry.Home, entry.Shell)
	}

	return b.String()
}

func (m Manifest) renderTmpfiles() string {
	var b strings.Builder
	for _, entry := range m.Tmpfiles {
		fmt.Fprintf(&b, "%s %s %s %s %s %s\n", entry.Type, entry.Path, entry.Mode, entry.User, entry.Group, entry.Age)
	}

	return b.String()
}

func renderEnvExample(header string, env map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n", header)
	b.WriteString("# Example only. Config management or package defaults should write the real /etc/vectis/*.env file for a target host.\n")
	b.WriteString("# Review secrets, DSNs, TLS settings, addresses, ports, and topology before use.\n")
	for _, key := range sortedKeys(env) {
		fmt.Fprintf(&b, "%s=%s\n", key, env[key])
	}

	return b.String()
}

type section struct {
	Name  string
	Lines []directive
}

type directive struct {
	Key   string
	Value string
}

func renderSections(sections []section) string {
	var b bytes.Buffer
	for i, section := range sections {
		if i > 0 {
			b.WriteByte('\n')
		}

		fmt.Fprintf(&b, "[%s]\n", section.Name)
		for _, line := range section.Lines {
			if line.Value == "" {
				continue
			}

			fmt.Fprintf(&b, "%s=%s\n", line.Key, line.Value)
		}
	}

	return b.String()
}

func (m Manifest) validate() error {
	if m.Target.Name == "" {
		return fmt.Errorf("target.name is required")
	}

	if m.Target.Description == "" {
		return fmt.Errorf("target.description is required")
	}

	if m.Defaults.NetworkUnit == "" {
		return fmt.Errorf("defaults.network_unit is required")
	}

	if m.Defaults.MigrationUnit == "" {
		return fmt.Errorf("defaults.migration_unit is required")
	}

	if m.Defaults.CommonEnvFile == "" {
		return fmt.Errorf("defaults.common_env_file is required")
	}

	seen := map[string]struct{}{}
	for _, unit := range m.Units {
		if unit.ID == "" {
			return fmt.Errorf("unit.id is required")
		}

		if _, ok := seen[unit.ID]; ok {
			return fmt.Errorf("duplicate unit id %q", unit.ID)
		}

		seen[unit.ID] = struct{}{}
		if unit.Description == "" {
			return fmt.Errorf("unit %q description is required", unit.ID)
		}

		for _, ref := range append(append([]string{}, unit.Wants...), unit.After...) {
			if strings.Contains(ref, ".") {
				continue
			}

			if _, ok := seen[ref]; ok {
				continue
			}

			if hasUnitID(m.Units, ref) {
				continue
			}

			return fmt.Errorf("unit %q references unknown unit %q", unit.ID, ref)
		}
	}

	return nil
}

func hasUnitID(units []Unit, id string) bool {
	for _, unit := range units {
		if unit.ID == id {
			return true
		}
	}

	return false
}

func unitRefs(refs []string) []string {
	out := make([]string, 0, len(refs))
	for _, ref := range refs {
		if strings.Contains(ref, ".") {
			out = append(out, ref)
			continue
		}

		out = append(out, "vectis-"+ref+".service")
	}

	return out
}

func joinUnits(units []string) string {
	units = unique(units)
	sort.Strings(units)
	return strings.Join(units, " ")
}

func unique(values []string) []string {
	seen := map[string]struct{}{}
	var out []string

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}

		if _, ok := seen[value]; ok {
			continue
		}

		seen[value] = struct{}{}
		out = append(out, value)
	}

	return out
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}

	return fallback
}
