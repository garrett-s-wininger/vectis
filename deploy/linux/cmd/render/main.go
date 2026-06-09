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

	if _, err := linux.RenderToDir(linux.RenderOptions{ManifestPath: *manifestPath, OutDir: *outDir}); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
