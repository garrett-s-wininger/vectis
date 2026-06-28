//go:build mage

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

var imageComponentNames = []string{
	"cli",
	"api",
	"artifact",
	"catalog",
	"cell-ingress",
	"cron",
	"log",
	"log-forwarder",
	"orchestrator",
	"queue",
	"reconciler",
	"registry",
	"secrets",
	"spiffe",
	"worker",
	"worker-core",
	"docs",
}

// Image builds one component container image. Usage: mage image <component>.
func Image(component string) error {
	if !validImageComponent(component) {
		return fmt.Errorf("unknown image component %q", component)
	}

	return podmanBuild("vectis-"+component+":latest", component)
}

// ImageFull builds the all-in-one container image.
func ImageFull() error {
	return podmanBuild("vectis:latest", "all-in-one")
}

// ImagesAll builds the all-in-one image and all component images.
func ImagesAll() error {
	if err := ImageFull(); err != nil {
		return err
	}

	return ImagesComponents()
}

// ImagesComponents builds the component container images.
func ImagesComponents() error {
	for _, component := range imageComponentNames {
		if component == "docs" && truthy(os.Getenv("SKIP_WEB_BUILD")) {
			continue
		}

		if err := Image(component); err != nil {
			return err
		}
	}

	return nil
}

func podmanBuild(tag, target string) error {
	return run("", nil, envDefault("PODMAN", "podman"),
		"build",
		"-t", tag,
		"-f", filepath.Join("build", "Containerfile"),
		"--target", target,
		".",
	)
}

func validImageComponent(component string) bool {
	for _, valid := range imageComponentNames {
		if component == valid {
			return true
		}
	}

	return false
}
