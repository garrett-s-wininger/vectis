package linux

import (
	"context"

	"vectis/internal/platform"
)

const (
	DefaultLimaInstance = "vectis-deploy-smoke"
	DefaultLimaTemplate = "ubuntu-lts"
)

type LimaOptions = VMSmokeOptions
type LimaResult = VMSmokeResult

func RunLimaVerify(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts.Provider = platform.VirtualMachineProviderLima
	return RunVMSmokeVerify(ctx, opts)
}

func RunLimaClean(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts.Provider = platform.VirtualMachineProviderLima
	return RunVMSmokeClean(ctx, opts)
}

func RunLimaDown(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts.Provider = platform.VirtualMachineProviderLima
	return RunVMSmokeDown(ctx, opts)
}

func RunLimaDelete(ctx context.Context, opts LimaOptions) (LimaResult, error) {
	opts.Provider = platform.VirtualMachineProviderLima
	return RunVMSmokeDelete(ctx, opts)
}
