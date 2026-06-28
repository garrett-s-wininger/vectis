//go:build !windows

package platform

import "syscall"

// FileSystemStats is the portable subset of filesystem capacity details used by Vectis.
type FileSystemStats struct {
	FreeBytes       uint64
	TotalBytes      uint64
	FreeInodes      uint64
	FreeInodesKnown bool
}

// StatFileSystem returns available capacity for the filesystem containing path.
func StatFileSystem(path string) (FileSystemStats, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return FileSystemStats{}, err
	}

	return FileSystemStats{
		FreeBytes:       uint64(st.Bavail) * uint64(st.Bsize),
		TotalBytes:      uint64(st.Blocks) * uint64(st.Bsize),
		FreeInodes:      st.Ffree,
		FreeInodesKnown: true,
	}, nil
}
