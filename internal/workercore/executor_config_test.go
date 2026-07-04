package workercore

import (
	"strings"
	"testing"

	"vectis/internal/action"
	"vectis/internal/platform"
)

func TestNewJobExecutorHostConfig(t *testing.T) {
	executor, backend, err := NewJobExecutor(ExecutorConfig{})
	if err != nil {
		t.Fatalf("NewJobExecutor(host): %v", err)
	}

	if backend != ExecutionBackendHost {
		t.Fatalf("backend = %q, want %q", backend, ExecutionBackendHost)
	}

	processExecutor, isolation, err := executor.ResolveProcessExecutor("")
	if err != nil {
		t.Fatalf("ResolveProcessExecutor(default): %v", err)
	}

	if isolation != action.IsolationHost {
		t.Fatalf("default isolation = %q, want %q", isolation, action.IsolationHost)
	}

	if processExecutor == nil {
		t.Fatal("host process executor is nil")
	}
}

func TestNewJobExecutorLimaConfigRegistersVMDefault(t *testing.T) {
	executor, backend, err := NewJobExecutor(ExecutorConfig{
		Backend: ExecutionBackendLima,
		Lima: platform.VirtualMachineConfig{
			Instance: "vectis-worker",
		},
	})

	if err != nil {
		t.Fatalf("NewJobExecutor(lima): %v", err)
	}

	if backend != ExecutionBackendLima {
		t.Fatalf("backend = %q, want %q", backend, ExecutionBackendLima)
	}

	defaultExecutor, defaultIsolation, err := executor.ResolveProcessExecutor("")
	if err != nil {
		t.Fatalf("ResolveProcessExecutor(default): %v", err)
	}

	if defaultIsolation != action.IsolationVM {
		t.Fatalf("default isolation = %q, want %q", defaultIsolation, action.IsolationVM)
	}

	if _, ok := defaultExecutor.(*platform.VirtualMachineCommandExecutor); !ok {
		t.Fatalf("default executor = %T, want *platform.VirtualMachineCommandExecutor", defaultExecutor)
	}

	hostExecutor, hostIsolation, err := executor.ResolveProcessExecutor(action.IsolationHost)
	if err != nil {
		t.Fatalf("ResolveProcessExecutor(host): %v", err)
	}

	if hostIsolation != action.IsolationHost {
		t.Fatalf("host isolation = %q, want %q", hostIsolation, action.IsolationHost)
	}

	if hostExecutor == nil {
		t.Fatal("host process executor is nil")
	}
}

func TestNewProcessExecutorUnknownBackend(t *testing.T) {
	_, _, err := NewProcessExecutor(ExecutorConfig{Backend: "wat"})
	if err == nil || !strings.Contains(err.Error(), `unknown execution backend "wat"`) {
		t.Fatalf("NewProcessExecutor error = %v, want unknown backend", err)
	}
}

func TestExecutionCapabilitiesForBackend(t *testing.T) {
	tests := []struct {
		name                 string
		backend              string
		wantBackend          string
		wantDefaultIsolation string
		wantSupported        []string
	}{
		{
			name:                 "default host",
			backend:              "",
			wantBackend:          ExecutionBackendHost,
			wantDefaultIsolation: action.IsolationHost,
			wantSupported:        []string{action.IsolationHost},
		},
		{
			name:                 "lima",
			backend:              ExecutionBackendLima,
			wantBackend:          ExecutionBackendLima,
			wantDefaultIsolation: action.IsolationVM,
			wantSupported:        []string{action.IsolationHost, action.IsolationVM},
		},
		{
			name:        "unknown",
			backend:     "wat",
			wantBackend: "wat",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBackend, gotDefaultIsolation, gotSupported := ExecutionCapabilitiesForBackend(tt.backend)
			if gotBackend != tt.wantBackend {
				t.Fatalf("backend = %q, want %q", gotBackend, tt.wantBackend)
			}

			if gotDefaultIsolation != tt.wantDefaultIsolation {
				t.Fatalf("default isolation = %q, want %q", gotDefaultIsolation, tt.wantDefaultIsolation)
			}

			if strings.Join(gotSupported, ",") != strings.Join(tt.wantSupported, ",") {
				t.Fatalf("supported isolation = %v, want %v", gotSupported, tt.wantSupported)
			}
		})
	}
}
