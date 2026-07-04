//go:build unix

package interfaces

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

var commandProcessGroupCancelGrace = 5 * time.Second

func configureCommandProcessIsolation(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}

	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}

	cmd.SysProcAttr.Setpgid = true
	configureCommandParentDeathSignal(cmd)

	cmd.Cancel = func() error {
		err := signalCommandProcessGroup(cmd, syscall.SIGTERM)
		if err != nil {
			return err
		}

		scheduleCommandProcessGroupKill(cmd, commandProcessGroupCancelGrace)
		return nil
	}

	if cmd.WaitDelay == 0 {
		cmd.WaitDelay = commandProcessGroupCancelGrace
	}
}

func signalCommandProcessGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	pgid, err := commandProcessGroupID(cmd)
	if err != nil {
		return err
	}

	if err := syscall.Kill(-pgid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}

		return err
	}

	return nil
}

func commandProcessGroupID(cmd *exec.Cmd) (int, error) {
	if cmd == nil || cmd.Process == nil {
		return 0, os.ErrProcessDone
	}

	return cmd.Process.Pid, nil
}

func scheduleCommandProcessGroupKill(cmd *exec.Cmd, grace time.Duration) {
	if grace <= 0 {
		_ = signalCommandProcessGroup(cmd, syscall.SIGKILL)
		return
	}

	go func() {
		timer := time.NewTimer(grace)
		defer timer.Stop()
		<-timer.C

		if !isActiveProcess(cmd) {
			return
		}

		_ = signalCommandProcessGroup(cmd, syscall.SIGKILL)
	}()
}

func terminateActiveProcess(cmd *exec.Cmd) {
	if err := signalCommandProcessGroup(cmd, syscall.SIGTERM); err != nil {
		return
	}

	scheduleCommandProcessGroupKill(cmd, commandProcessGroupCancelGrace)
}
