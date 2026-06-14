package packaging

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	rpmHeaderMagic = "\x8e\xad\xe8\x01"

	rpmTypeInt16       = 3
	rpmTypeInt32       = 4
	rpmTypeString      = 6
	rpmTypeBin         = 7
	rpmTypeStringArray = 8
	rpmTypeI18NString  = 9

	rpmTagSigHeaderImmutable = 62
	rpmTagHeaderImmutable    = 63
	rpmTagSigSHA1            = 269
	rpmTagSigSHA256          = 273
	rpmTagSigSize            = 1000
	rpmTagSigMD5             = 1004
	rpmTagSigPayloadSize     = 1007

	rpmTagName              = 1000
	rpmTagVersion           = 1001
	rpmTagRelease           = 1002
	rpmTagSummary           = 1004
	rpmTagDescription       = 1005
	rpmTagBuildTime         = 1006
	rpmTagBuildHost         = 1007
	rpmTagSize              = 1009
	rpmTagVendor            = 1011
	rpmTagLicense           = 1014
	rpmTagPackager          = 1015
	rpmTagGroup             = 1016
	rpmTagURL               = 1020
	rpmTagOS                = 1021
	rpmTagArch              = 1022
	rpmTagFileSizes         = 1028
	rpmTagFileModes         = 1030
	rpmTagFileRDevs         = 1033
	rpmTagFileMTimes        = 1034
	rpmTagFileDigests       = 1035
	rpmTagFileLinkTos       = 1036
	rpmTagFileFlags         = 1037
	rpmTagFileUserName      = 1039
	rpmTagFileGroupName     = 1040
	rpmTagProvideName       = 1047
	rpmTagRequireFlags      = 1048
	rpmTagRequireName       = 1049
	rpmTagRequireVersion    = 1050
	rpmTagRPMVersion        = 1064
	rpmTagFileDevices       = 1095
	rpmTagFileInodes        = 1096
	rpmTagFileLangs         = 1097
	rpmTagProvideFlags      = 1112
	rpmTagProvideVersion    = 1113
	rpmTagDirIndexes        = 1116
	rpmTagBaseNames         = 1117
	rpmTagDirNames          = 1118
	rpmTagPayloadFormat     = 1124
	rpmTagPayloadCompressor = 1125
	rpmTagPayloadFlags      = 1126
	rpmTagFileDigestAlgo    = 5011
	rpmTagPayloadDigest     = 5092
	rpmTagPayloadDigestAlgo = 5093

	rpmSenseLess    = 2
	rpmSenseEqual   = 8
	rpmRegularMode  = 0o100000
	rpmDigestSHA256 = 8
)

type rpmHeaderIndex struct {
	tag    int32
	typ    int32
	offset int32
	count  int32
}

type rpmHeaderBuilder struct {
	indexes []rpmHeaderIndex
	store   []byte
}

type rpmPayloadFile struct {
	resolvedFile
	content      []byte
	payloadName  string
	baseName     string
	dirName      string
	size         int32
	mode         int16
	mtime        int32
	device       int32
	inode        int32
	rdev         int16
	digestSHA256 string
}

func buildRPM(pkg resolvedPackage, outDir string) (string, error) {
	rpmArch := rpmArch(pkg.Arch)
	rpmVersion := rpmVersion(pkg.Version)
	rpmRelease := rpmRelease(pkg.Release)
	outPath := filepath.Join(outDir, fmt.Sprintf("%s-%s-%s.%s.rpm", pkg.Name, rpmVersion, rpmRelease, rpmArch))

	files, installedSize, err := rpmPayloadFiles(pkg.Files)
	if err != nil {
		return "", err
	}

	payload, payloadDigest, uncompressedPayloadSize, err := buildRPMPayload(files)
	if err != nil {
		return "", err
	}

	header, err := buildRPMMainHeader(pkg, files, rpmArch, rpmVersion, rpmRelease, installedSize, payloadDigest)
	if err != nil {
		return "", err
	}

	headerSHA1 := sha1Hex(header)
	headerSHA256 := sha256Hex(header)
	headerAndPayloadMD5 := md5.Sum(append(append([]byte{}, header...), payload...))
	signature := buildRPMSignatureHeader(int32(len(header)+len(payload)), uncompressedPayloadSize, headerSHA1, headerSHA256, headerAndPayloadMD5[:])
	lead := buildRPMLead(pkg.Name)

	var out bytes.Buffer
	out.Write(lead)
	out.Write(signature)
	out.Write(header)
	out.Write(payload)

	if err := os.WriteFile(outPath, out.Bytes(), 0o644); err != nil {
		return "", err
	}

	return outPath, nil
}

func rpmPayloadFiles(files []resolvedFile) ([]rpmPayloadFile, int32, error) {
	sorted := append([]resolvedFile{}, files...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Destination < sorted[j].Destination
	})

	out := make([]rpmPayloadFile, 0, len(sorted))
	var installedSize int64
	for i, file := range sorted {
		content, err := os.ReadFile(file.Source)
		if err != nil {
			return nil, 0, fmt.Errorf("read package file %s: %w", file.Source, err)
		}

		digest := sha256.Sum256(content)
		dirName, baseName := rpmSplitPath(file.Destination)
		mode := int64(rpmRegularMode) | file.Mode
		payloadName := "." + file.Destination
		installedSize += int64(len(content))
		out = append(out, rpmPayloadFile{
			resolvedFile: file,
			content:      content,
			payloadName:  payloadName,
			baseName:     baseName,
			dirName:      dirName,
			size:         int32(len(content)),
			mode:         int16(mode),
			mtime:        0,
			device:       1,
			inode:        int32(i + 1),
			rdev:         0,
			digestSHA256: hex.EncodeToString(digest[:]),
		})
	}

	if installedSize > int64(^uint32(0)>>1) {
		return nil, 0, fmt.Errorf("installed package size exceeds RPM int32 metadata limit")
	}

	return out, int32(installedSize), nil
}

func buildRPMPayload(files []rpmPayloadFile) ([]byte, string, int32, error) {
	var cpio bytes.Buffer
	for _, file := range files {
		if err := writeNewCEntry(&cpio, file.payloadName, int64(file.mode), file.content); err != nil {
			return nil, "", 0, err
		}
	}

	if err := writeNewCEntry(&cpio, "TRAILER!!!", 0, nil); err != nil {
		return nil, "", 0, err
	}

	if cpio.Len() > int(^uint32(0)>>1) {
		return nil, "", 0, fmt.Errorf("rpm payload exceeds int32 metadata limit")
	}

	var compressed bytes.Buffer
	gz := gzip.NewWriter(&compressed)
	gz.Header.ModTime = time.Unix(0, 0)
	if _, err := gz.Write(cpio.Bytes()); err != nil {
		_ = gz.Close()
		return nil, "", 0, err
	}

	if err := gz.Close(); err != nil {
		return nil, "", 0, err
	}

	digest := sha256.Sum256(compressed.Bytes())
	return compressed.Bytes(), hex.EncodeToString(digest[:]), int32(cpio.Len()), nil
}

func writeNewCEntry(buf *bytes.Buffer, name string, mode int64, content []byte) error {
	namesize := len(name) + 1
	header := fmt.Sprintf("070701%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x",
		0,
		uint32(mode),
		0,
		0,
		1,
		0,
		len(content),
		0,
		0,
		0,
		0,
		namesize,
		0,
	)

	if len(header) != 110 {
		return fmt.Errorf("internal cpio header size = %d, want 110", len(header))
	}

	buf.WriteString(header)
	buf.WriteString(name)
	buf.WriteByte(0)
	padBuffer(buf, 4)
	buf.Write(content)
	padBuffer(buf, 4)

	return nil
}

func buildRPMMainHeader(pkg resolvedPackage, files []rpmPayloadFile, arch, version, release string, installedSize int32, payloadDigest string) ([]byte, error) {
	var header rpmHeaderBuilder
	header.addString(rpmTagName, pkg.Name)
	header.addString(rpmTagVersion, version)
	header.addString(rpmTagRelease, release)
	header.addI18NString(rpmTagSummary, pkg.Summary)
	header.addI18NString(rpmTagDescription, rpmDescription(pkg))
	header.addInt32(rpmTagBuildTime, []int32{0})
	header.addString(rpmTagBuildHost, "vectis-package-builder")
	header.addInt32(rpmTagSize, []int32{installedSize})
	header.addString(rpmTagVendor, pkg.Vendor)
	header.addString(rpmTagLicense, pkg.License)
	header.addString(rpmTagPackager, pkg.Maintainer)
	header.addString(rpmTagGroup, rpmGroup(pkg.Section))
	header.addString(rpmTagURL, pkg.Homepage)
	header.addString(rpmTagOS, "linux")
	header.addString(rpmTagArch, arch)
	header.addString(rpmTagRPMVersion, "4.18.0")

	if len(files) > 0 {
		header.addStringArray(rpmTagBaseNames, rpmBaseNames(files))
		header.addStringArray(rpmTagDirNames, rpmDirNames(files))
		header.addInt32(rpmTagDirIndexes, rpmDirIndexes(files))
		header.addInt32(rpmTagFileSizes, rpmFileSizes(files))
		header.addInt16(rpmTagFileModes, rpmFileModes(files))
		header.addInt16(rpmTagFileRDevs, rpmFileRDevs(files))
		header.addInt32(rpmTagFileMTimes, rpmFileMTimes(files))
		header.addStringArray(rpmTagFileDigests, rpmFileDigests(files))
		header.addStringArray(rpmTagFileLinkTos, repeatString("", len(files)))
		header.addInt32(rpmTagFileFlags, repeatInt32(0, len(files)))
		header.addStringArray(rpmTagFileUserName, rpmFileOwners(files))
		header.addStringArray(rpmTagFileGroupName, rpmFileGroups(files))
		header.addInt32(rpmTagFileDevices, rpmFileDevices(files))
		header.addInt32(rpmTagFileInodes, rpmFileInodes(files))
		header.addStringArray(rpmTagFileLangs, repeatString("", len(files)))
		header.addInt32(rpmTagFileDigestAlgo, []int32{rpmDigestSHA256})
	}

	provideVersion := fmt.Sprintf("%s-%s", version, release)
	header.addStringArray(rpmTagProvideName, []string{pkg.Name})
	header.addStringArray(rpmTagProvideVersion, []string{provideVersion})
	header.addInt32(rpmTagProvideFlags, []int32{rpmSenseEqual})

	requireNames, requireFlags, requireVersions := rpmRequires(pkg.Depends)
	if len(requireNames) > 0 {
		header.addStringArray(rpmTagRequireName, requireNames)
		header.addStringArray(rpmTagRequireVersion, requireVersions)
		header.addInt32(rpmTagRequireFlags, requireFlags)
	}

	header.addString(rpmTagPayloadFormat, "cpio")
	header.addString(rpmTagPayloadCompressor, "gzip")
	header.addString(rpmTagPayloadFlags, "9")
	header.addStringArray(rpmTagPayloadDigest, []string{payloadDigest})
	header.addInt32(rpmTagPayloadDigestAlgo, []int32{rpmDigestSHA256})

	return header.render(rpmTagHeaderImmutable, false), nil
}

func buildRPMSignatureHeader(size, payloadSize int32, headerSHA1, headerSHA256 string, digest []byte) []byte {
	var header rpmHeaderBuilder
	header.addString(rpmTagSigSHA1, headerSHA1)
	header.addString(rpmTagSigSHA256, headerSHA256)
	header.addInt32(rpmTagSigSize, []int32{size})
	header.addBin(rpmTagSigMD5, digest)
	header.addInt32(rpmTagSigPayloadSize, []int32{payloadSize})

	return header.render(rpmTagSigHeaderImmutable, true)
}

func buildRPMLead(name string) []byte {
	lead := make([]byte, 96)
	copy(lead[0:4], []byte{0xed, 0xab, 0xee, 0xdb})
	lead[4] = 3
	lead[5] = 0
	binary.BigEndian.PutUint16(lead[6:8], 0)
	binary.BigEndian.PutUint16(lead[8:10], 0)
	copy(lead[10:76], []byte(name))
	binary.BigEndian.PutUint16(lead[76:78], 0)
	binary.BigEndian.PutUint16(lead[78:80], 5)

	return lead
}

func (h *rpmHeaderBuilder) addString(tag int, value string) {
	h.add(tag, rpmTypeString, []byte(value+"\x00"), 1)
}

func (h *rpmHeaderBuilder) addI18NString(tag int, value string) {
	h.add(tag, rpmTypeI18NString, []byte(value+"\x00"), 1)
}

func (h *rpmHeaderBuilder) addStringArray(tag int, values []string) {
	var b bytes.Buffer
	for _, value := range values {
		b.WriteString(value)
		b.WriteByte(0)
	}

	h.add(tag, rpmTypeStringArray, b.Bytes(), len(values))
}

func (h *rpmHeaderBuilder) addInt16(tag int, values []int16) {
	h.align(2)
	var b bytes.Buffer
	for _, value := range values {
		_ = binary.Write(&b, binary.BigEndian, value)
	}

	h.add(tag, rpmTypeInt16, b.Bytes(), len(values))
}

func (h *rpmHeaderBuilder) addInt32(tag int, values []int32) {
	h.align(4)
	var b bytes.Buffer
	for _, value := range values {
		_ = binary.Write(&b, binary.BigEndian, value)
	}

	h.add(tag, rpmTypeInt32, b.Bytes(), len(values))
}

func (h *rpmHeaderBuilder) addBin(tag int, value []byte) {
	h.add(tag, rpmTypeBin, value, len(value))
}

func (h *rpmHeaderBuilder) add(tag, typ int, data []byte, count int) {
	offset := len(h.store)
	h.store = append(h.store, data...)
	h.indexes = append(h.indexes, rpmHeaderIndex{
		tag:    int32(tag),
		typ:    int32(typ),
		offset: int32(offset),
		count:  int32(count),
	})
}

func (h *rpmHeaderBuilder) align(n int) {
	for len(h.store)%n != 0 {
		h.store = append(h.store, 0)
	}
}

func (h rpmHeaderBuilder) render(regionTag int, padToEight bool) []byte {
	indexes := append([]rpmHeaderIndex{}, h.indexes...)
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].offset < indexes[j].offset
	})

	store := append([]byte{}, h.store...)
	if regionTag != 0 {
		indexCount := len(indexes) + 1
		regionOffset := len(store)

		var region bytes.Buffer
		_ = binary.Write(&region, binary.BigEndian, int32(regionTag))
		_ = binary.Write(&region, binary.BigEndian, int32(rpmTypeBin))
		_ = binary.Write(&region, binary.BigEndian, int32(-indexCount*16))
		_ = binary.Write(&region, binary.BigEndian, int32(16))
		store = append(store, region.Bytes()...)

		regionIndex := rpmHeaderIndex{
			tag:    int32(regionTag),
			typ:    rpmTypeBin,
			offset: int32(regionOffset),
			count:  16,
		}

		indexes = append([]rpmHeaderIndex{regionIndex}, indexes...)
	}

	var b bytes.Buffer
	b.WriteString(rpmHeaderMagic)
	b.Write([]byte{0, 0, 0, 0})
	_ = binary.Write(&b, binary.BigEndian, int32(len(indexes)))
	_ = binary.Write(&b, binary.BigEndian, int32(len(store)))
	for _, index := range indexes {
		_ = binary.Write(&b, binary.BigEndian, index.tag)
		_ = binary.Write(&b, binary.BigEndian, index.typ)
		_ = binary.Write(&b, binary.BigEndian, index.offset)
		_ = binary.Write(&b, binary.BigEndian, index.count)
	}

	b.Write(store)
	if padToEight {
		padBuffer(&b, 8)
	}

	return b.Bytes()
}

func rpmSplitPath(destination string) (dir, base string) {
	clean := path.Clean(filepath.ToSlash(destination))
	base = path.Base(clean)
	dir = path.Dir(clean)
	if dir == "." {
		dir = "/"
	}

	if !strings.HasSuffix(dir, "/") {
		dir += "/"
	}

	return dir, base
}

func rpmDescription(pkg resolvedPackage) string {
	if pkg.Description != "" {
		return pkg.Description
	}

	return pkg.Summary
}

func rpmGroup(section string) string {
	switch strings.TrimSpace(section) {
	case "", "devel":
		return "Development/Tools"
	default:
		return section
	}
}

func rpmRequires(depends []string) ([]string, []int32, []string) {
	names := []string{
		"rpmlib(CompressedFileNames)",
		"rpmlib(FileDigests)",
		"rpmlib(PayloadFilesHavePrefix)",
	}

	flags := []int32{
		rpmSenseLess | rpmSenseEqual,
		rpmSenseLess | rpmSenseEqual,
		rpmSenseLess | rpmSenseEqual,
	}

	versions := []string{
		"3.0.4-1",
		"4.6.0-1",
		"4.0-1",
	}

	for _, depend := range depends {
		depend = strings.TrimSpace(depend)
		if depend == "" {
			continue
		}

		names = append(names, depend)
		flags = append(flags, 0)
		versions = append(versions, "")
	}

	return names, flags, versions
}

func rpmBaseNames(files []rpmPayloadFile) []string {
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, file.baseName)
	}

	return out
}

func rpmDirNames(files []rpmPayloadFile) []string {
	var dirs []string
	seen := map[string]bool{}
	for _, file := range files {
		if seen[file.dirName] {
			continue
		}

		seen[file.dirName] = true
		dirs = append(dirs, file.dirName)
	}

	return dirs
}

func rpmDirIndexes(files []rpmPayloadFile) []int32 {
	dirs := rpmDirNames(files)
	indexes := map[string]int32{}
	for i, dir := range dirs {
		indexes[dir] = int32(i)
	}

	out := make([]int32, 0, len(files))
	for _, file := range files {
		out = append(out, indexes[file.dirName])
	}

	return out
}

func rpmFileSizes(files []rpmPayloadFile) []int32 {
	out := make([]int32, 0, len(files))
	for _, file := range files {
		out = append(out, file.size)
	}

	return out
}

func rpmFileModes(files []rpmPayloadFile) []int16 {
	out := make([]int16, 0, len(files))
	for _, file := range files {
		out = append(out, file.mode)
	}

	return out
}

func rpmFileRDevs(files []rpmPayloadFile) []int16 {
	out := make([]int16, 0, len(files))
	for _, file := range files {
		out = append(out, file.rdev)
	}

	return out
}

func rpmFileMTimes(files []rpmPayloadFile) []int32 {
	out := make([]int32, 0, len(files))
	for _, file := range files {
		out = append(out, file.mtime)
	}

	return out
}

func rpmFileDigests(files []rpmPayloadFile) []string {
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, file.digestSHA256)
	}

	return out
}

func rpmFileOwners(files []rpmPayloadFile) []string {
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, file.Owner)
	}

	return out
}

func rpmFileGroups(files []rpmPayloadFile) []string {
	out := make([]string, 0, len(files))
	for _, file := range files {
		out = append(out, file.Group)
	}

	return out
}

func rpmFileDevices(files []rpmPayloadFile) []int32 {
	out := make([]int32, 0, len(files))
	for _, file := range files {
		out = append(out, file.device)
	}

	return out
}

func rpmFileInodes(files []rpmPayloadFile) []int32 {
	out := make([]int32, 0, len(files))
	for _, file := range files {
		out = append(out, file.inode)
	}

	return out
}

func repeatString(value string, count int) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = value
	}

	return out
}

func repeatInt32(value int32, count int) []int32 {
	out := make([]int32, count)
	for i := range out {
		out[i] = value
	}

	return out
}

func padBuffer(buf *bytes.Buffer, alignment int) {
	for buf.Len()%alignment != 0 {
		buf.WriteByte(0)
	}
}

func sha1Hex(b []byte) string {
	digest := sha1.Sum(b)
	return hex.EncodeToString(digest[:])
}

func sha256Hex(b []byte) string {
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func rpmVersion(version string) string {
	return strings.ReplaceAll(normalizePackageVersion(version), "-", "_")
}

func rpmRelease(release string) string {
	return strings.ReplaceAll(normalizePackageRelease(release), "-", "_")
}

func rpmArch(arch string) string {
	switch strings.TrimSpace(arch) {
	case "amd64", "x86_64":
		return "x86_64"
	case "386", "i386":
		return "i386"
	case "arm64", "aarch64":
		return "aarch64"
	case "arm":
		return "armv7hl"
	default:
		return strings.TrimSpace(arch)
	}
}
