package main

import (
	"io"
	"strings"
	"testing"
)

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

	if opts.lane != "all" {
		t.Fatalf("lane = %q, want all", opts.lane)
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

func TestSelectedLanesFiltersByName(t *testing.T) {
	opts, err := parseOptions([]string{"--lane", "package-builder"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	lanes, err := selectedLanes(opts)
	if err != nil {
		t.Fatal(err)
	}

	if len(lanes) != 1 || lanes[0].name != "package-builder" {
		t.Fatalf("selected lanes = %+v, want package-builder only", lanes)
	}
}

func TestSelectedLanesRejectsUnknownLane(t *testing.T) {
	opts, err := parseOptions([]string{"--lane", "not-real"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	_, err = selectedLanes(opts)
	if err == nil || !strings.Contains(err.Error(), "unknown VM lane") {
		t.Fatalf("expected unknown lane error, got %v", err)
	}
}
