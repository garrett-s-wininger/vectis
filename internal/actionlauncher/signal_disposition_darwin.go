//go:build darwin

package actionlauncher

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
)

type darwinSigaction struct {
	handler [8]byte
	mask    uint32
	flags   int32
}

var (
	loadDarwinSigactionOnce sync.Once
	loadDarwinSigactionErr  error
	darwinLibcSigaction     func(int32, *darwinSigaction, unsafe.Pointer) int32
)

func resetSignalDisposition(sig syscall.Signal) error {
	loadDarwinSigactionOnce.Do(func() {
		loadDarwinSigactionErr = loadDarwinSigaction()
	})

	if loadDarwinSigactionErr != nil {
		return loadDarwinSigactionErr
	}

	var action darwinSigaction
	if rc := darwinLibcSigaction(int32(sig), &action, nil); rc != 0 {
		return fmt.Errorf("sigaction(%d) failed with status %d", sig, rc)
	}

	return nil
}

func loadDarwinSigaction() (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("load libc sigaction: %v", recovered)
		}
	}()

	libc, err := purego.Dlopen("/usr/lib/libSystem.B.dylib", purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("load libSystem: %w", err)
	}

	purego.RegisterLibFunc(&darwinLibcSigaction, libc, "sigaction")
	return nil
}
