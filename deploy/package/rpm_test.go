package packaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestRPMArchiveShape(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "vectis-cli")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\necho vectis\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	path, err := buildRPM(resolvedPackage{
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
		Arch:        "arm64",
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

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(b[:4], []byte{0xed, 0xab, 0xee, 0xdb}) {
		t.Fatalf("missing RPM lead magic: %x", b[:4])
	}

	if !bytes.Equal(b[96:100], []byte(rpmHeaderMagic)) {
		t.Fatalf("missing RPM signature header magic: %x", b[96:100])
	}

	signatureSize := rpmHeaderTotalSize(t, b[96:], true)
	mainOffset := 96 + signatureSize
	if mainOffset%8 != 0 {
		t.Fatalf("main header offset = %d, want 8-byte aligned", mainOffset)
	}

	if !bytes.Equal(b[mainOffset:mainOffset+4], []byte(rpmHeaderMagic)) {
		t.Fatalf("missing RPM main header magic at %d: %x", mainOffset, b[mainOffset:mainOffset+4])
	}

	payloadOffset := mainOffset + rpmHeaderTotalSize(t, b[mainOffset:], false)
	if !bytes.Equal(b[payloadOffset:payloadOffset+2], []byte{0x1f, 0x8b}) {
		t.Fatalf("missing gzip payload at %d: %x", payloadOffset, b[payloadOffset:payloadOffset+2])
	}

	signatureEntries := rpmHeaderEntries(t, b[96:])
	if signatureEntries[0].tag != rpmTagSigHeaderImmutable {
		t.Fatalf("first signature tag = %d, want %d", signatureEntries[0].tag, rpmTagSigHeaderImmutable)
	}

	mainEntries := rpmHeaderEntries(t, b[mainOffset:])
	if mainEntries[0].tag != rpmTagHeaderImmutable {
		t.Fatalf("first main header tag = %d, want %d", mainEntries[0].tag, rpmTagHeaderImmutable)
	}

	headerSHA256 := sha256.Sum256(b[mainOffset:payloadOffset])
	if got, want := rpmHeaderString(t, b[96:], signatureEntries, rpmTagSigSHA256), hex.EncodeToString(headerSHA256[:]); got != want {
		t.Fatalf("signature SHA256 header = %q, want %q", got, want)
	}
}

func rpmHeaderTotalSize(t *testing.T, b []byte, padded bool) int {
	t.Helper()
	if !bytes.Equal(b[:4], []byte(rpmHeaderMagic)) {
		t.Fatalf("missing header magic")
	}

	indexCount := int(binary.BigEndian.Uint32(b[8:12]))
	storeSize := int(binary.BigEndian.Uint32(b[12:16]))
	size := 16 + indexCount*16 + storeSize
	if padded {
		for size%8 != 0 {
			size++
		}
	}

	return size
}

func rpmHeaderEntries(t *testing.T, b []byte) []rpmHeaderIndex {
	t.Helper()
	if !bytes.Equal(b[:4], []byte(rpmHeaderMagic)) {
		t.Fatalf("missing header magic")
	}

	indexCount := int(binary.BigEndian.Uint32(b[8:12]))
	entries := make([]rpmHeaderIndex, 0, indexCount)
	for i := 0; i < indexCount; i++ {
		offset := 16 + i*16
		entries = append(entries, rpmHeaderIndex{
			tag:    int32(binary.BigEndian.Uint32(b[offset : offset+4])),
			typ:    int32(binary.BigEndian.Uint32(b[offset+4 : offset+8])),
			offset: int32(binary.BigEndian.Uint32(b[offset+8 : offset+12])),
			count:  int32(binary.BigEndian.Uint32(b[offset+12 : offset+16])),
		})
	}

	return entries
}

func rpmHeaderString(t *testing.T, b []byte, entries []rpmHeaderIndex, tag int) string {
	t.Helper()

	for _, entry := range entries {
		if entry.tag != int32(tag) {
			continue
		}

		if entry.typ != rpmTypeString {
			t.Fatalf("tag %d type = %d, want string", tag, entry.typ)
		}

		storeOffset := 16 + len(entries)*16
		start := storeOffset + int(entry.offset)
		end := bytes.IndexByte(b[start:], 0)
		if end < 0 {
			t.Fatalf("tag %d string is not NUL-terminated", tag)
		}

		return string(b[start : start+end])
	}

	t.Fatalf("tag %d not found", tag)
	return ""
}
