//go:build unix

package interfaces

import (
	"os/exec"
	"syscall"
)

func configureCommandProcessIsolation(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	cmd.SysProcAttr.Setpgid = true
}
