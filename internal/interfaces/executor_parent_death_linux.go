//go:build linux

package interfaces

import (
	"os/exec"
	"syscall"
)

func configureCommandParentDeathSignal(cmd *exec.Cmd) {
	if cmd == nil || cmd.SysProcAttr == nil {
		return
	}

	if cmd.SysProcAttr.Pdeathsig == 0 {
		cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
	}
}
