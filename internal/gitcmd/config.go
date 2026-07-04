package gitcmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type configSetting struct {
	key        string
	section    string
	subsection string
	name       string
	value      string
}

// WriteWorkTreeConfigSettings writes scalar local Git config values for a worktree.
func WriteWorkTreeConfigSettings(workTree string, settings [][2]string) error {
	configPath, err := WorkTreeConfigPath(workTree)
	if err != nil {
		return err
	}

	return WriteConfigFileSettings(configPath, settings)
}

// WriteGitDirConfigSettings writes scalar local Git config values for a Git directory.
func WriteGitDirConfigSettings(gitDir string, settings [][2]string) error {
	return WriteConfigFileSettings(filepath.Join(gitDir, "config"), settings)
}

// WorkTreeConfigPath returns the local config path for a Git worktree.
func WorkTreeConfigPath(workTree string) (string, error) {
	gitDir, err := WorkTreeGitDir(workTree)
	if err != nil {
		return "", err
	}

	return filepath.Join(gitDir, "config"), nil
}

// WorkTreeRemoteURLs reads the first configured URL for each remote in a worktree.
func WorkTreeRemoteURLs(workTree string) (map[string]string, error) {
	configPath, err := WorkTreeConfigPath(workTree)
	if err != nil {
		return nil, err
	}

	return ReadConfigFileRemoteURLs(configPath)
}

// ReadConfigFileRemoteURLs reads the first configured URL for each remote section.
func ReadConfigFileRemoteURLs(configPath string) (map[string]string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}

		return nil, fmt.Errorf("read git config %s: %w", configPath, err)
	}

	remotes := map[string]string{}
	currentRemote := ""
	for _, line := range splitConfigLines(string(data)) {
		if section, subsection, ok := parseConfigSection(line); ok {
			currentRemote = ""
			if strings.EqualFold(section, "remote") && strings.TrimSpace(subsection) != "" {
				currentRemote = subsection
				if _, exists := remotes[currentRemote]; !exists {
					remotes[currentRemote] = ""
				}
			}
			continue
		}

		if currentRemote == "" {
			continue
		}

		name, value, ok := parseConfigAssignment(line)
		if !ok || !strings.EqualFold(name, "url") {
			continue
		}

		if _, exists := remotes[currentRemote]; exists {
			if remotes[currentRemote] == "" {
				remotes[currentRemote] = strings.TrimSpace(value)
			}
			continue
		}

		remotes[currentRemote] = strings.TrimSpace(value)
	}

	return remotes, nil
}

// WorkTreeGitDir returns the Git directory path for a worktree.
func WorkTreeGitDir(workTree string) (string, error) {
	dotGit := filepath.Join(workTree, ".git")
	info, err := os.Stat(dotGit)
	if err != nil {
		return "", fmt.Errorf("stat .git for %s: %w", workTree, err)
	}
	if info.IsDir() {
		return filepath.Clean(dotGit), nil
	}

	data, err := os.ReadFile(dotGit)
	if err != nil {
		return "", fmt.Errorf("read .git for %s: %w", workTree, err)
	}

	line := strings.TrimSpace(string(data))
	gitDir, ok := strings.CutPrefix(line, "gitdir:")
	if !ok {
		return "", fmt.Errorf("%s does not contain a gitdir pointer", dotGit)
	}

	gitDir = strings.TrimSpace(gitDir)
	if gitDir == "" {
		return "", fmt.Errorf("%s contains an empty gitdir pointer", dotGit)
	}
	if !filepath.IsAbs(gitDir) {
		gitDir = filepath.Join(workTree, gitDir)
	}

	return filepath.Clean(gitDir), nil
}

// GitCommonDir returns the common Git directory for gitDir.
func GitCommonDir(gitDir string) string {
	gitDir = filepath.Clean(gitDir)
	data, err := os.ReadFile(filepath.Join(gitDir, "commondir"))
	if err != nil {
		return gitDir
	}

	commonDir := strings.TrimSpace(string(data))
	if commonDir == "" {
		return gitDir
	}
	if filepath.IsAbs(commonDir) {
		return filepath.Clean(commonDir)
	}

	return filepath.Clean(filepath.Join(gitDir, commonDir))
}

// WriteConfigFileSettings updates or appends scalar Git config keys in configPath.
func WriteConfigFileSettings(configPath string, settings [][2]string) error {
	parsed, err := parseConfigSettings(settings)
	if err != nil {
		return err
	}
	if len(parsed) == 0 {
		return nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("read git config %s: %w", configPath, err)
	}

	lines := splitConfigLines(string(data))
	seen := make(map[string]bool, len(parsed))
	currentSection := ""
	currentSubsection := ""

	for i, line := range lines {
		if section, subsection, ok := parseConfigSection(line); ok {
			currentSection = section
			currentSubsection = subsection
			continue
		}

		name, ok := parseConfigAssignmentName(line)
		if !ok {
			continue
		}

		for _, setting := range parsed {
			if !sameConfigSection(currentSection, currentSubsection, setting.section, setting.subsection) {
				continue
			}
			if !strings.EqualFold(name, setting.name) {
				continue
			}

			lines[i] = "\t" + setting.name + " = " + formatConfigValue(setting.value)
			seen[setting.key] = true
			break
		}
	}

	for _, setting := range parsed {
		if seen[setting.key] {
			continue
		}

		if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) != "" {
			lines = append(lines, "")
		}
		lines = append(lines, formatConfigSection(setting.section, setting.subsection))
		lines = append(lines, "\t"+setting.name+" = "+formatConfigValue(setting.value))
		seen[setting.key] = true
	}

	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return fmt.Errorf("create git config dir %s: %w", filepath.Dir(configPath), err)
	}
	if err := os.WriteFile(configPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return fmt.Errorf("write git config %s: %w", configPath, err)
	}

	return nil
}

func parseConfigSettings(settings [][2]string) ([]configSetting, error) {
	out := make([]configSetting, 0, len(settings))
	for _, raw := range settings {
		setting, err := parseConfigSetting(raw[0], raw[1])
		if err != nil {
			return nil, err
		}
		out = append(out, setting)
	}

	return out, nil
}

func parseConfigSetting(key, value string) (configSetting, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return configSetting{}, fmt.Errorf("git config key is empty")
	}

	parts := strings.Split(key, ".")
	if len(parts) < 2 {
		return configSetting{}, fmt.Errorf("git config key %q must contain a section and name", key)
	}

	setting := configSetting{
		key:     strings.ToLower(key),
		section: parts[0],
		name:    parts[len(parts)-1],
		value:   strings.TrimSpace(value),
	}
	if len(parts) > 2 {
		setting.subsection = strings.Join(parts[1:len(parts)-1], ".")
	}

	return setting, nil
}

func splitConfigLines(in string) []string {
	in = strings.ReplaceAll(in, "\r\n", "\n")
	in = strings.TrimSuffix(in, "\n")
	if in == "" {
		return nil
	}

	return strings.Split(in, "\n")
}

func parseConfigSection(line string) (string, string, bool) {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[") || !strings.HasSuffix(trimmed, "]") {
		return "", "", false
	}

	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]"))
	if body == "" {
		return "", "", false
	}

	fields := strings.Fields(body)
	if len(fields) == 0 {
		return "", "", false
	}
	section := fields[0]
	if len(fields) == 1 {
		return section, "", true
	}

	subsection := strings.TrimSpace(strings.TrimPrefix(body, section))
	if unquoted, err := strconv.Unquote(subsection); err == nil {
		subsection = unquoted
	}

	return section, subsection, true
}

func parseConfigAssignmentName(line string) (string, bool) {
	name, _, ok := parseConfigAssignment(line)
	return name, ok
}

func parseConfigAssignment(line string) (string, string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
		return "", "", false
	}

	name, _, ok := strings.Cut(trimmed, "=")
	if !ok {
		fields := strings.Fields(trimmed)
		if len(fields) == 0 {
			return "", "", false
		}

		if len(fields) == 1 {
			return fields[0], "", true
		}

		return fields[0], strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0])), true
	}

	_, value, _ := strings.Cut(trimmed, "=")
	name = strings.TrimSpace(name)
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "\"") {
		if unquoted, err := strconv.Unquote(value); err == nil {
			value = unquoted
		}
	}

	return name, value, name != ""
}

func sameConfigSection(leftSection, leftSubsection, rightSection, rightSubsection string) bool {
	return strings.EqualFold(leftSection, rightSection) && leftSubsection == rightSubsection
}

func formatConfigSection(section, subsection string) string {
	if subsection == "" {
		return "[" + section + "]"
	}

	return "[" + section + " " + strconv.Quote(subsection) + "]"
}

func formatConfigValue(value string) string {
	if !configValueNeedsQuotes(value) {
		return value
	}

	var out strings.Builder
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\b':
			out.WriteString(`\b`)
		case '\n':
			out.WriteString(`\n`)
		case '\t':
			out.WriteString(`\t`)
		default:
			out.WriteRune(r)
		}
	}
	out.WriteByte('"')
	return out.String()
}

func configValueNeedsQuotes(value string) bool {
	return value == "" ||
		strings.ContainsAny(value, "\\\"#;\b\n\t") ||
		strings.TrimSpace(value) != value
}
