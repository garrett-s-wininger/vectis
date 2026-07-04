//go:build !darwin && !linux && !windows

package platform

import (
	"context"
	"fmt"
	"runtime"
)

func installCertificateAuthority(ctx context.Context, caFile string) error {
	return fmt.Errorf("platform: installing into system trust is unsupported on %s", runtime.GOOS)
}
