package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	packaging "vectis/deploy/package"
)

type inputFlags map[string]string

func (f inputFlags) String() string {
	var parts []string
	for key, value := range f {
		parts = append(parts, key+"="+value)
	}

	return strings.Join(parts, ",")
}

func (f inputFlags) Set(raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
		return fmt.Errorf("input must be name=path")
	}

	f[strings.TrimSpace(key)] = strings.TrimSpace(value)
	return nil
}

func main() {
	inputs := inputFlags{}
	opts := packaging.BuildOptions{Inputs: inputs}
	jsonOutput := flag.Bool("json", false, "Print build result as JSON")
	flag.StringVar(&opts.ManifestPath, "manifest", packaging.DefaultManifestPath, "Path to package manifest TOML")
	flag.StringVar(&opts.PackageID, "package", "vectis-cli", "Package id to build")
	flag.StringVar(&opts.Format, "format", "deb", "Package format to build")
	flag.StringVar(&opts.OutputDir, "out", "artifacts/packages", "Directory for package outputs")
	flag.StringVar(&opts.Version, "version", "0.0.0-dev", "Package version")
	flag.StringVar(&opts.Release, "release", "1", "Package release")
	flag.StringVar(&opts.Arch, "arch", "", "Target architecture, using Go arch names")
	flag.Var(inputs, "input", "Logical package input as name=path; may be repeated")
	flag.Parse()

	result, err := packaging.Build(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "package build failed: %v\n", err)
		os.Exit(1)
	}

	if *jsonOutput {
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fmt.Fprintf(os.Stderr, "encode result: %v\n", err)
			os.Exit(1)
		}

		return
	}

	fmt.Printf("%s\n", result.Path)
}
