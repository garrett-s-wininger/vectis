//go:build !windows

package main

import (
	"os"
	"os/exec"
	"syscall"
)

func configureServiceCommand(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func terminateSignal() os.Signal {
	return syscall.SIGTERM
}

func signalProcesses(procs []*exec.Cmd, sig os.Signal) {
	sysSig, ok := sig.(syscall.Signal)
	if !ok {
		return
	}

	for _, proc := range procs {
		if proc.Process != nil {
			_ = syscall.Kill(-proc.Process.Pid, sysSig)
		}
	}
}

func forceKillProcesses(procs []*exec.Cmd) {
	signalProcesses(procs, syscall.SIGKILL)
}
