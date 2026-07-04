//go:build !darwin && !linux

package logserver

import "errors"

const platformSupportsVectoredWrite = false

var errPlatformVectoredWriteUnsupported = errors.New("vectored writes are not supported on this platform")

type platformWriteBatch struct{}

func (b *platformWriteBatch) ensureCapacity(int) {}

func (b *platformWriteBatch) reset() {}

func (b *platformWriteBatch) release(int) {}

func (b *platformWriteBatch) appendBytes([]byte) {}

func (b *platformWriteBatch) writeAll(int) error {
	return errPlatformVectoredWriteUnsupported
}
