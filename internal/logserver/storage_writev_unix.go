//go:build darwin || linux

package logserver

import (
	"errors"
	"io"
	"syscall"
	"unsafe"
)

const platformSupportsVectoredWrite = true

type platformWriteBatch struct {
	iovs []syscall.Iovec
}

func (b *platformWriteBatch) ensureCapacity(size int) {
	if cap(b.iovs) < size {
		b.iovs = make([]syscall.Iovec, 0, size)
	}
}

func (b *platformWriteBatch) reset() {
	b.iovs = b.iovs[:0]
}

func (b *platformWriteBatch) release(maxCapacity int) {
	for i := range b.iovs {
		b.iovs[i] = syscall.Iovec{}
	}

	if cap(b.iovs) > maxCapacity {
		b.iovs = nil
		return
	}

	b.iovs = b.iovs[:0]
}

func (b *platformWriteBatch) appendBytes(data []byte) {
	iov := syscall.Iovec{Base: &data[0]}
	iov.SetLen(len(data))
	b.iovs = append(b.iovs, iov)
}

func (b *platformWriteBatch) writeAll(fd int) error {
	iovs := b.iovs
	for len(iovs) > 0 {
		n, err := platformWritev(fd, iovs)
		if n > 0 {
			iovs = advancePlatformWritev(iovs, n)
		}

		if err != nil {
			if errors.Is(err, syscall.EINTR) && n == 0 {
				continue
			}

			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}
	}

	return nil
}

func platformWritev(fd int, iovs []syscall.Iovec) (int, error) {
	// #nosec G103 -- writev requires passing the kernel a pointer to the first iovec.
	r0, _, errno := syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&iovs[0])), uintptr(len(iovs)))
	if errno != 0 {
		return int(r0), errno
	}

	return int(r0), nil
}

func advancePlatformWritev(iovs []syscall.Iovec, n int) []syscall.Iovec {
	for n > 0 && len(iovs) > 0 {
		iovLen := int(iovs[0].Len)
		if n < iovLen {
			// #nosec G103 -- adjust the iovec base by the number of bytes the kernel accepted.
			iovs[0].Base = (*byte)(unsafe.Add(unsafe.Pointer(iovs[0].Base), n))
			iovs[0].SetLen(iovLen - n)
			return iovs
		}

		n -= iovLen
		iovs[0] = syscall.Iovec{}
		iovs = iovs[1:]
	}

	return iovs
}
