//go:build windows

package main

import (
	"os"
	"os/exec"
)

func configureServiceCommand(*exec.Cmd) {}

func terminateSignal() os.Signal {
	return os.Kill
}

func signalProcesses(procs []*exec.Cmd, _ os.Signal) {
	forceKillProcesses(procs)
}

func forceKillProcesses(procs []*exec.Cmd) {
	for _, proc := range procs {
		if proc.Process != nil {
			_ = proc.Process.Kill()
		}
	}
}
