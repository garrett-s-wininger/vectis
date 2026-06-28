//go:build windows

package platform

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// FileSystemStats is the portable subset of filesystem capacity details used by Vectis.
type FileSystemStats struct {
	FreeBytes       uint64
	TotalBytes      uint64
	FreeInodes      uint64
	FreeInodesKnown bool
}

// StatFileSystem returns available capacity for the filesystem containing path.
func StatFileSystem(path string) (FileSystemStats, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return FileSystemStats{}, err
	}

	pathPtr, err := windows.UTF16PtrFromString(abs)
	if err != nil {
		return FileSystemStats{}, err
	}

	var freeBytesAvailableToCaller uint64
	var totalBytes uint64
	var totalFreeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(pathPtr, &freeBytesAvailableToCaller, &totalBytes, &totalFreeBytes); err != nil {
		return FileSystemStats{}, err
	}

	return FileSystemStats{
		FreeBytes:  freeBytesAvailableToCaller,
		TotalBytes: totalBytes,
	}, nil
}
