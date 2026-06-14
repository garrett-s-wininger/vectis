package packaging

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const DefaultManifestPath = "deploy/package/packages.toml"

//go:embed packages.toml
var embeddedManifest []byte

type Manifest struct {
	Defaults PackageDefaults `toml:"defaults"`
	Packages []Package       `toml:"package"`
}

type PackageDefaults struct {
	Maintainer string   `toml:"maintainer"`
	Vendor     string   `toml:"vendor"`
	Homepage   string   `toml:"homepage"`
	License    string   `toml:"license"`
	Section    string   `toml:"section"`
	Priority   string   `toml:"priority"`
	Depends    []string `toml:"depends"`
}

type Package struct {
	ID          string        `toml:"id"`
	Name        string        `toml:"name"`
	Summary     string        `toml:"summary"`
	Description string        `toml:"description"`
	Maintainer  string        `toml:"maintainer"`
	Vendor      string        `toml:"vendor"`
	Homepage    string        `toml:"homepage"`
	License     string        `toml:"license"`
	Section     string        `toml:"section"`
	Priority    string        `toml:"priority"`
	Depends     []string      `toml:"depends"`
	Files       []PackageFile `toml:"file"`
}

type PackageFile struct {
	ID          string `toml:"id"`
	Source      string `toml:"source"`
	Destination string `toml:"destination"`
	Mode        string `toml:"mode"`
	Owner       string `toml:"owner"`
	Group       string `toml:"group"`
}

type BuildOptions struct {
	ManifestPath string
	PackageID    string
	Format       string
	OutputDir    string
	Version      string
	Release      string
	Arch         string
	Inputs       map[string]string
}

type BuildResult struct {
	Status    string `json:"status"`
	PackageID string `json:"package_id"`
	Name      string `json:"name"`
	Format    string `json:"format"`
	Path      string `json:"path"`
	Version   string `json:"version"`
	Release   string `json:"release"`
	Arch      string `json:"arch"`
	Files     int    `json:"files"`
}

type resolvedPackage struct {
	ID          string
	Name        string
	Summary     string
	Description string
	Maintainer  string
	Vendor      string
	Homepage    string
	License     string
	Section     string
	Priority    string
	Depends     []string
	Version     string
	Release     string
	Arch        string
	Files       []resolvedFile
}

type resolvedFile struct {
	Source      string
	Destination string
	Mode        int64
	Owner       string
	Group       string
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

func Build(opts BuildOptions) (BuildResult, error) {
	if opts.PackageID == "" {
		return BuildResult{}, fmt.Errorf("package id is required")
	}

	if opts.OutputDir == "" {
		return BuildResult{}, fmt.Errorf("output directory is required")
	}

	format := strings.ToLower(strings.TrimSpace(opts.Format))
	if format == "" {
		format = "deb"
	}

	manifest, err := LoadManifest(opts.ManifestPath)
	if err != nil {
		return BuildResult{}, fmt.Errorf("load package manifest: %w", err)
	}

	pkg, err := manifest.resolve(opts)
	if err != nil {
		return BuildResult{}, err
	}

	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return BuildResult{}, err
	}

	var path string
	switch format {
	case "deb":
		path, err = buildDeb(pkg, opts.OutputDir)
	case "rpm":
		path, err = buildRPM(pkg, opts.OutputDir)
	default:
		err = fmt.Errorf("unsupported package format %q", format)
	}

	if err != nil {
		return BuildResult{}, err
	}

	return BuildResult{
		Status:    "packaged",
		PackageID: pkg.ID,
		Name:      pkg.Name,
		Format:    format,
		Path:      path,
		Version:   pkg.Version,
		Release:   pkg.Release,
		Arch:      pkg.Arch,
		Files:     len(pkg.Files),
	}, nil
}

func (m Manifest) validate() error {
	seen := map[string]struct{}{}
	for _, pkg := range m.Packages {
		if strings.TrimSpace(pkg.ID) == "" {
			return fmt.Errorf("package.id is required")
		}

		if _, ok := seen[pkg.ID]; ok {
			return fmt.Errorf("duplicate package id %q", pkg.ID)
		}

		seen[pkg.ID] = struct{}{}
		if strings.TrimSpace(pkg.Name) == "" {
			return fmt.Errorf("package %q name is required", pkg.ID)
		}

		if strings.TrimSpace(pkg.Summary) == "" {
			return fmt.Errorf("package %q summary is required", pkg.ID)
		}

		if len(pkg.Files) == 0 {
			return fmt.Errorf("package %q must include at least one file", pkg.ID)
		}

		for _, file := range pkg.Files {
			if strings.TrimSpace(file.Source) == "" {
				return fmt.Errorf("package %q file source is required", pkg.ID)
			}

			if strings.TrimSpace(file.Destination) == "" {
				return fmt.Errorf("package %q file destination is required", pkg.ID)
			}
		}
	}

	return nil
}

func (m Manifest) resolve(opts BuildOptions) (resolvedPackage, error) {
	var source Package
	found := false
	for _, pkg := range m.Packages {
		if pkg.ID == opts.PackageID {
			source = pkg
			found = true
			break
		}
	}

	if !found {
		return resolvedPackage{}, fmt.Errorf("package %q not found in manifest", opts.PackageID)
	}

	version := normalizePackageVersion(opts.Version)
	release := normalizePackageRelease(opts.Release)
	arch := normalizePackageArch(opts.Arch)

	pkg := resolvedPackage{
		ID:          source.ID,
		Name:        source.Name,
		Summary:     source.Summary,
		Description: strings.TrimSpace(source.Description),
		Maintainer:  valueOr(source.Maintainer, m.Defaults.Maintainer),
		Vendor:      valueOr(source.Vendor, m.Defaults.Vendor),
		Homepage:    valueOr(source.Homepage, m.Defaults.Homepage),
		License:     valueOr(source.License, m.Defaults.License),
		Section:     valueOr(source.Section, m.Defaults.Section),
		Priority:    valueOr(source.Priority, m.Defaults.Priority),
		Depends:     append(append([]string{}, m.Defaults.Depends...), source.Depends...),
		Version:     version,
		Release:     release,
		Arch:        arch,
	}

	for _, file := range source.Files {
		resolved, err := resolvePackageFile(file, opts.Inputs)
		if err != nil {
			return resolvedPackage{}, fmt.Errorf("package %q: %w", source.ID, err)
		}

		pkg.Files = append(pkg.Files, resolved)
	}

	return pkg, nil
}

func resolvePackageFile(file PackageFile, inputs map[string]string) (resolvedFile, error) {
	source := strings.TrimSpace(file.Source)
	for _, key := range []string{file.ID, file.Source} {
		if key == "" {
			continue
		}

		if input := strings.TrimSpace(inputs[key]); input != "" {
			source = input
			break
		}
	}

	if source == "" {
		return resolvedFile{}, fmt.Errorf("file %q source is required", file.ID)
	}

	destination := filepath.ToSlash(strings.TrimSpace(file.Destination))
	if !strings.HasPrefix(destination, "/") {
		return resolvedFile{}, fmt.Errorf("file %q destination must be absolute: %s", file.ID, file.Destination)
	}

	if strings.Contains(destination, "\x00") || strings.Contains(destination, "\n") || strings.Contains(destination, "\t") {
		return resolvedFile{}, fmt.Errorf("file %q destination contains an unsupported control character", file.ID)
	}

	if strings.Contains(destination, "/../") || strings.HasSuffix(destination, "/..") {
		return resolvedFile{}, fmt.Errorf("file %q destination must not contain parent traversal: %s", file.ID, file.Destination)
	}

	mode, err := parseFileMode(valueOr(file.Mode, "0644"))
	if err != nil {
		return resolvedFile{}, fmt.Errorf("file %q mode: %w", file.ID, err)
	}

	return resolvedFile{
		Source:      source,
		Destination: destination,
		Mode:        mode,
		Owner:       valueOr(file.Owner, "root"),
		Group:       valueOr(file.Group, "root"),
	}, nil
}

func parseFileMode(raw string) (int64, error) {
	mode, err := strconv.ParseInt(strings.TrimSpace(raw), 8, 64)
	if err != nil {
		return 0, err
	}

	if mode <= 0 || mode > 0o7777 {
		return 0, fmt.Errorf("mode %q is out of range", raw)
	}

	return mode, nil
}

func normalizePackageVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return "0.0.0-dev"
	}

	version = strings.TrimPrefix(version, "v")
	version = sanitizePackageVersion(version)
	if version == "" {
		return "0.0.0-dev"
	}

	if version[0] < '0' || version[0] > '9' {
		version = "0.0.0+" + version
	}

	return version
}

func normalizePackageRelease(release string) string {
	release = strings.TrimSpace(release)
	if release == "" {
		return "1"
	}

	release = sanitizePackageVersion(release)
	if release == "" {
		return "1"
	}

	return strings.ReplaceAll(release, "-", ".")
}

func normalizePackageArch(arch string) string {
	arch = strings.TrimSpace(arch)
	if arch == "" {
		return runtime.GOARCH
	}

	return arch
}

var packageVersionInvalidRE = regexp.MustCompile(`[^A-Za-z0-9.+:~_-]+`)

func sanitizePackageVersion(version string) string {
	version = strings.ReplaceAll(version, "/", ".")
	version = packageVersionInvalidRE.ReplaceAllString(version, ".")
	return strings.Trim(version, ".-_+~:")
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}

	return fallback
}
