//go:build !unix

package interfaces

import "os/exec"

func configureCommandProcessIsolation(*exec.Cmd) {}

func terminateActiveProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}

	_ = cmd.Process.Kill()
}
