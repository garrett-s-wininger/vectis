//go:build !windows

package main

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
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

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}

	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}

	if runtime.GOOS != "linux" {
		return true
	}

	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}

	text := string(data)
	closeParen := strings.LastIndex(text, ")")
	if closeParen < 0 || closeParen+2 >= len(text) {
		return true
	}

	state, _, _ := strings.Cut(strings.TrimSpace(text[closeParen+1:]), " ")
	return state != "Z"
}
