package platform

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/interfaces"
)

const VirtualMachineProviderLima = "lima"

// VirtualMachineCommand describes a process that should run inside a VM.
type VirtualMachineCommand struct {
	Path    string
	Args    []string
	WorkDir string
	Env     []string
}

// VirtualMachine owns VM-specific command execution.
type VirtualMachine interface {
	StartCommand(ctx context.Context, command VirtualMachineCommand) (interfaces.Process, error)
}

// VirtualMachineConfig selects a VM provider and carries common provider
// settings. Provider implementation details stay behind NewVirtualMachine.
type VirtualMachineConfig struct {
	Provider           string
	Instance           string
	ProviderPath       string
	GuestWorkspaceRoot string
	Start              bool
	PreserveEnv        bool
}

// VirtualMachineCommandExecutor adapts a VM provider to the process executor
// interface used by action implementations.
type VirtualMachineCommandExecutor struct {
	vm VirtualMachine
}

func NewVirtualMachine(config VirtualMachineConfig) (VirtualMachine, error) {
	switch provider := strings.ToLower(strings.TrimSpace(config.Provider)); provider {
	case VirtualMachineProviderLima:
		return newLimaVirtualMachine(limaVirtualMachineConfig{
			instance:           config.Instance,
			limactlPath:        config.ProviderPath,
			guestWorkspaceRoot: config.GuestWorkspaceRoot,
			start:              config.Start,
			preserveEnv:        config.PreserveEnv,
		})
	default:
		return nil, fmt.Errorf("unknown virtual machine provider %q", config.Provider)
	}
}

func NewVirtualMachineCommandExecutor(config VirtualMachineConfig) (*VirtualMachineCommandExecutor, error) {
	vm, err := NewVirtualMachine(config)
	if err != nil {
		return nil, err
	}

	return &VirtualMachineCommandExecutor{vm: vm}, nil
}

func (e *VirtualMachineCommandExecutor) Start(ctx context.Context, path string, args []string, workDir string, env []string) (interfaces.Process, error) {
	if e == nil || e.vm == nil {
		return nil, fmt.Errorf("virtual machine executor is not configured")
	}

	return e.vm.StartCommand(ctx, VirtualMachineCommand{
		Path:    path,
		Args:    args,
		WorkDir: workDir,
		Env:     env,
	})
}
