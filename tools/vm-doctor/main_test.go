package main

import (
	"io"
	"strings"
	"testing"
)

func TestParseLimaStatus(t *testing.T) {
	got, err := parseLimaStatus("vectis-deploy-smoke\tStopped\n", "vectis-deploy-smoke")
	if err != nil {
		t.Fatal(err)
	}

	if got != "Stopped" {
		t.Fatalf("status = %q, want Stopped", got)
	}
}

func TestParseLimaStatusRejectsMissingInstance(t *testing.T) {
	_, err := parseLimaStatus("other\tRunning\n", "vectis-deploy-smoke")
	if err == nil || !strings.Contains(err.Error(), "not present") {
		t.Fatalf("expected missing instance error, got %v", err)
	}
}

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if opts.mode != "status" {
		t.Fatalf("mode = %q, want status", opts.mode)
	}

	if opts.prepVersion != defaultPrepVersion {
		t.Fatalf("prep version = %q, want %q", opts.prepVersion, defaultPrepVersion)
	}
}

func TestLanesIncludeRepairTargets(t *testing.T) {
	opts, err := parseOptions([]string{"--prep-version", "7"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	lanes := lanes(opts)
	if len(lanes) != 4 {
		t.Fatalf("lanes = %d, want 4", len(lanes))
	}

	for _, lane := range lanes {
		if lane.prepareTarget == "" || lane.checkTarget == "" {
			t.Fatalf("lane %s missing repair/check target: %+v", lane.name, lane)
		}
	}
}
