package packaging

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func buildDeb(pkg resolvedPackage, outDir string) (string, error) {
	debArch := debianArch(pkg.Arch)
	debVersion := debianVersion(pkg.Version, pkg.Release)
	outPath := filepath.Join(outDir, fmt.Sprintf("%s_%s_%s.deb", pkg.Name, debVersion, debArch))

	data, md5sums, err := buildDebDataArchive(pkg.Files)
	if err != nil {
		return "", err
	}

	control, err := buildDebControlArchive(pkg, debArch, debVersion, md5sums)
	if err != nil {
		return "", err
	}

	var out bytes.Buffer
	out.WriteString("!<arch>\n")
	if err := writeArMember(&out, "debian-binary", []byte("2.0\n")); err != nil {
		return "", err
	}

	if err := writeArMember(&out, "control.tar.gz", control); err != nil {
		return "", err
	}

	if err := writeArMember(&out, "data.tar.gz", data); err != nil {
		return "", err
	}

	if err := os.WriteFile(outPath, out.Bytes(), 0o644); err != nil {
		return "", err
	}

	return outPath, nil
}

func buildDebDataArchive(files []resolvedFile) ([]byte, string, error) {
	var (
		buf    bytes.Buffer
		md5sum strings.Builder
	)

	gz := gzip.NewWriter(&buf)
	gz.Header.ModTime = time.Unix(0, 0)
	tw := tar.NewWriter(gz)

	sorted := append([]resolvedFile{}, files...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Destination < sorted[j].Destination
	})

	for _, dir := range packageParentDirs(sorted) {
		header := &tar.Header{
			Name:     "." + dir + "/",
			Typeflag: tar.TypeDir,
			Mode:     0o755,
			ModTime:  time.Unix(0, 0),
			Uid:      0,
			Gid:      0,
			Uname:    "root",
			Gname:    "root",
		}

		if err := tw.WriteHeader(header); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, "", err
		}
	}

	for _, file := range sorted {
		content, err := os.ReadFile(file.Source)
		if err != nil {
			_ = tw.Close()
			_ = gz.Close()

			return nil, "", fmt.Errorf("read package file %s: %w", file.Source, err)
		}

		digest := md5.Sum(content)
		fmt.Fprintf(&md5sum, "%s  %s\n", hex.EncodeToString(digest[:]), strings.TrimPrefix(file.Destination, "/"))

		header := &tar.Header{
			Name:    "." + file.Destination,
			Mode:    file.Mode,
			Size:    int64(len(content)),
			ModTime: time.Unix(0, 0),
			Uid:     0,
			Gid:     0,
			Uname:   file.Owner,
			Gname:   file.Group,
		}

		if err := tw.WriteHeader(header); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, "", err
		}

		if _, err := tw.Write(content); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, "", err
		}
	}

	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return nil, "", err
	}
	if err := gz.Close(); err != nil {
		return nil, "", err
	}

	return buf.Bytes(), md5sum.String(), nil
}

func buildDebControlArchive(pkg resolvedPackage, arch, version, md5sums string) ([]byte, error) {
	files := map[string][]byte{
		"./control": []byte(renderDebControl(pkg, arch, version)),
	}

	if md5sums != "" {
		files["./md5sums"] = []byte(md5sums)
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Header.ModTime = time.Unix(0, 0)
	tw := tar.NewWriter(gz)

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}

	sort.Strings(names)

	for _, name := range names {
		content := files[name]
		header := &tar.Header{
			Name:    name,
			Mode:    0o644,
			Size:    int64(len(content)),
			ModTime: time.Unix(0, 0),
			Uid:     0,
			Gid:     0,
			Uname:   "root",
			Gname:   "root",
		}

		if err := tw.WriteHeader(header); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}

		if _, err := tw.Write(content); err != nil {
			_ = tw.Close()
			_ = gz.Close()
			return nil, err
		}
	}

	if err := tw.Close(); err != nil {
		_ = gz.Close()
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func renderDebControl(pkg resolvedPackage, arch, version string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Package: %s\n", pkg.Name)
	fmt.Fprintf(&b, "Version: %s\n", version)
	fmt.Fprintf(&b, "Section: %s\n", valueOr(pkg.Section, "devel"))
	fmt.Fprintf(&b, "Priority: %s\n", valueOr(pkg.Priority, "optional"))
	fmt.Fprintf(&b, "Architecture: %s\n", arch)
	fmt.Fprintf(&b, "Maintainer: %s\n", pkg.Maintainer)

	if len(pkg.Depends) > 0 {
		fmt.Fprintf(&b, "Depends: %s\n", strings.Join(pkg.Depends, ", "))
	}

	if pkg.Homepage != "" {
		fmt.Fprintf(&b, "Homepage: %s\n", pkg.Homepage)
	}

	if pkg.Vendor != "" {
		fmt.Fprintf(&b, "Vendor: %s\n", pkg.Vendor)
	}

	fmt.Fprintf(&b, "Description: %s\n", pkg.Summary)
	if pkg.Description != "" {
		for _, line := range strings.Split(pkg.Description, "\n") {
			line = strings.TrimRight(line, " \t")
			if line == "" {
				b.WriteString(" .\n")
				continue
			}

			fmt.Fprintf(&b, " %s\n", line)
		}
	}

	return b.String()
}

func writeArMember(w io.Writer, name string, content []byte) error {
	if len(name) > 15 {
		return fmt.Errorf("ar member name %q is too long", name)
	}

	header := fmt.Sprintf("%-16s%-12d%-6d%-6d%-8o%-10d`\n", name, 0, 0, 0, 0o100644, len(content))
	if len(header) != 60 {
		return fmt.Errorf("internal ar header size for %q = %d, want 60", name, len(header))
	}

	if _, err := io.WriteString(w, header); err != nil {
		return err
	}

	if _, err := w.Write(content); err != nil {
		return err
	}

	if len(content)%2 != 0 {
		_, err := w.Write([]byte{'\n'})
		return err
	}

	return nil
}

func debianVersion(version, release string) string {
	version = normalizePackageVersion(version)
	release = normalizePackageRelease(release)
	if release == "" {
		return version
	}

	return version + "-" + release
}

func debianArch(arch string) string {
	switch strings.TrimSpace(arch) {
	case "amd64", "x86_64":
		return "amd64"
	case "386", "i386":
		return "i386"
	case "arm64", "aarch64":
		return "arm64"
	case "arm":
		return "armhf"
	default:
		return strings.TrimSpace(arch)
	}
}
