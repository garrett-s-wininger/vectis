package main

import (
	"flag"
	"fmt"
	"os"

	"vectis/deploy/linux"
)

func main() {
	manifestPath := flag.String("manifest", linux.DefaultManifestPath, "Path to the Linux deploy services TOML manifest")
	outDir := flag.String("out", "", "Directory where rendered Linux artifacts are written")
	flag.Parse()

	if *outDir == "" {
		fmt.Fprintln(os.Stderr, "-out is required")
		os.Exit(2)
	}

	manifest, err := linux.LoadManifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load manifest: %v\n", err)
		os.Exit(1)
	}

	files, err := manifest.RenderFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "render files: %v\n", err)
		os.Exit(1)
	}

	if err := linux.WriteFiles(*outDir, files); err != nil {
		fmt.Fprintf(os.Stderr, "write files: %v\n", err)
		os.Exit(1)
	}
}
