package packaging

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestDebArchiveContents(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "vectis-cli")
	content := []byte("#!/bin/sh\necho vectis\n")
	if err := os.WriteFile(bin, content, 0o755); err != nil {
		t.Fatal(err)
	}

	path, err := buildDeb(resolvedPackage{
		ID:          "vectis-cli",
		Name:        "vectis-cli",
		Summary:     "Command line client for Vectis",
		Description: "A small client.",
		Maintainer:  "Garrett Wininger <garrett.s.wininger@outlook.com>",
		Homepage:    "https://github.com/garrett-s-wininger/vectis",
		Vendor:      "Vectis",
		Section:     "devel",
		Priority:    "optional",
		Depends:     []string{"ca-certificates"},
		Version:     "1.2.3",
		Release:     "1",
		Arch:        "amd64",
		Files: []resolvedFile{{
			Source:      bin,
			Destination: "/usr/bin/vectis-cli",
			Mode:        0o755,
			Owner:       "root",
			Group:       "root",
		}},
	}, t.TempDir())

	if err != nil {
		t.Fatal(err)
	}

	members := readArMembers(t, path)
	if got, want := sortedMemberNames(members), []string{"control.tar.gz", "data.tar.gz", "debian-binary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ar members = %v, want %v", got, want)
	}

	controlFiles := readGzipTar(t, members["control.tar.gz"])
	control := string(controlFiles["./control"])
	for _, want := range []string{
		"Package: vectis-cli\n",
		"Version: 1.2.3-1\n",
		"Architecture: amd64\n",
		"Depends: ca-certificates\n",
		"Description: Command line client for Vectis\n",
	} {
		if !strings.Contains(control, want) {
			t.Fatalf("control missing %q:\n%s", want, control)
		}
	}

	if !strings.Contains(string(controlFiles["./md5sums"]), "  usr/bin/vectis-cli\n") {
		t.Fatalf("md5sums missing binary entry: %s", controlFiles["./md5sums"])
	}

	dataFiles := readGzipTar(t, members["data.tar.gz"])
	if got := dataFiles["./usr/bin/vectis-cli"]; !bytes.Equal(got, content) {
		t.Fatalf("packaged binary = %q, want %q", got, content)
	}
}

func readArMembers(t *testing.T, path string) map[string][]byte {
	t.Helper()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.HasPrefix(b, []byte("!<arch>\n")) {
		t.Fatalf("%s is not an ar archive", path)
	}

	members := map[string][]byte{}
	rest := b[len("!<arch>\n"):]
	for len(rest) > 0 {
		if len(rest) < 60 {
			t.Fatalf("truncated ar header")
		}

		header := rest[:60]
		name := strings.TrimSpace(string(header[:16]))
		sizeRaw := strings.TrimSpace(string(header[48:58]))
		var size int
		if _, err := fmt.Sscanf(sizeRaw, "%d", &size); err != nil {
			t.Fatalf("invalid ar size %q: %v", sizeRaw, err)
		}

		rest = rest[60:]
		if len(rest) < size {
			t.Fatalf("truncated ar member %s", name)
		}

		members[name] = append([]byte{}, rest[:size]...)
		rest = rest[size:]
		if size%2 != 0 {
			if len(rest) == 0 {
				t.Fatalf("missing ar padding for %s", name)
			}

			rest = rest[1:]
		}
	}

	return members
}

func readGzipTar(t *testing.T, b []byte) map[string][]byte {
	t.Helper()

	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	files := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatal(err)
		}

		content, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}

		files[header.Name] = content
	}

	return files
}

func sortedMemberNames(m map[string][]byte) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}

	sortStrings(names)
	return names
}

func sortStrings(values []string) {
	sort.Strings(values)
}
