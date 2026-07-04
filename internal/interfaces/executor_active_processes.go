package interfaces

import (
	"os/exec"
	"sync"
)

var activeProcessRegistry sync.Map

func registerActiveProcess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}

	activeProcessRegistry.Store(cmd, struct{}{})
}

func unregisterActiveProcess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}

	activeProcessRegistry.Delete(cmd)
}

func isActiveProcess(cmd *exec.Cmd) bool {
	if cmd == nil {
		return false
	}

	_, ok := activeProcessRegistry.Load(cmd)
	return ok
}

// TerminateActiveProcesses best-effort terminates commands still known to this
// process. Worker-core calls this during shutdown to avoid leaving launched
// actions behind when the service exits before normal task cancellation
// finishes.
func TerminateActiveProcesses() {
	activeProcessRegistry.Range(func(key, _ any) bool {
		cmd, ok := key.(*exec.Cmd)
		if ok && cmd.Process != nil {
			terminateActiveProcess(cmd)
		}

		return true
	})
}
