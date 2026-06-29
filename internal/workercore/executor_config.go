package workercore

import (
	"fmt"
	"strings"

	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/job"
	"vectis/internal/platform"
	"vectis/internal/source"
)

const (
	ExecutionBackendHost = "host"
	ExecutionBackendLima = "lima"
)

type ExecutorConfig struct {
	Backend                 string
	WorkspaceRoot           string
	CheckoutCacheRoot       string
	CheckoutCacheRemoteURLs []string
	Lima                    platform.VirtualMachineConfig
}

func NewJobExecutor(cfg ExecutorConfig) (*job.Executor, string, error) {
	processExecutor, backend, err := NewProcessExecutor(cfg)
	if err != nil {
		return nil, "", err
	}

	options := []job.ExecutorOption{}
	if workspaceRoot := strings.TrimSpace(cfg.WorkspaceRoot); workspaceRoot != "" {
		options = append(options, job.WithWorkspaceRoot(workspaceRoot))
	}

	if checkoutCacheRoot := strings.TrimSpace(cfg.CheckoutCacheRoot); checkoutCacheRoot != "" && len(cfg.CheckoutCacheRemoteURLs) > 0 {
		checkoutCache, err := source.NewWorkerCheckoutCache(checkoutCacheRoot, cfg.CheckoutCacheRemoteURLs)
		if err != nil {
			return nil, "", err
		}

		options = append(options, job.WithCheckoutCache(checkoutCache))
	}

	if processExecutor != nil {
		options = append(options,
			job.WithVMProcessExecutor(processExecutor),
			job.WithDefaultIsolation(action.IsolationVM),
		)
	}

	return job.NewExecutor(options...), backend, nil
}

func NewProcessExecutor(cfg ExecutorConfig) (interfaces.ExecExecutor, string, error) {
	switch backend := strings.TrimSpace(cfg.Backend); backend {
	case "", ExecutionBackendHost:
		return nil, ExecutionBackendHost, nil
	case ExecutionBackendLima:
		lima := cfg.Lima
		if strings.TrimSpace(lima.Provider) == "" {
			lima.Provider = platform.VirtualMachineProviderLima
		}

		executor, err := platform.NewVirtualMachineCommandExecutor(lima)
		if err != nil {
			return nil, "", err
		}

		return executor, ExecutionBackendLima, nil
	default:
		return nil, "", fmt.Errorf("unknown execution backend %q", backend)
	}
}

func ExecutionCapabilitiesForBackend(backend string) (string, string, []string) {
	switch strings.TrimSpace(backend) {
	case "", ExecutionBackendHost:
		return ExecutionBackendHost, action.IsolationHost, []string{action.IsolationHost}
	case ExecutionBackendLima:
		return ExecutionBackendLima, action.IsolationVM, []string{action.IsolationHost, action.IsolationVM}
	default:
		return strings.TrimSpace(backend), "", nil
	}
}
