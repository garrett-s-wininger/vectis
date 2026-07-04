package proto_test

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"testing"
)

var (
	protoImportPattern    = regexp.MustCompile(`(?m)^\s*import\s+"([^"]+)"\s*;`)
	protoGoPackagePattern = regexp.MustCompile(`(?m)^\s*option\s+go_package\s*=\s*"([^"]+)"\s*;`)
)

func TestProtoGoPackageOptionsStayStable(t *testing.T) {
	files, err := filepath.Glob("*.proto")
	if err != nil {
		t.Fatalf("glob protos: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("no proto files found")
	}

	for _, path := range files {
		path := path
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}

			match := protoGoPackagePattern.FindSubmatch(data)
			if match == nil {
				t.Fatalf("%s missing go_package option", path)
			}

			if got := string(match[1]); got != "vectis/api/gen/go;api" {
				t.Fatalf("%s go_package = %q, want vectis/api/gen/go;api", path, got)
			}
		})
	}
}

func TestWorkerCoreProtoImportClosureIsExtensionFriendly(t *testing.T) {
	data, err := os.ReadFile("worker_core.proto")
	if err != nil {
		t.Fatalf("read worker_core.proto: %v", err)
	}

	var got []string
	for _, match := range protoImportPattern.FindAllSubmatch(data, -1) {
		got = append(got, string(match[1]))
	}

	sort.Strings(got)
	want := []string{"common.proto", "secrets.proto"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("worker_core.proto imports = %v, want %v", got, want)
	}
}
