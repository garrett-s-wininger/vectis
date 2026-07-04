//go:build unix

package actionlauncher

import "syscall"

var resetDispositionSignals = []syscall.Signal{
	syscall.SIGHUP,
	syscall.SIGINT,
	syscall.SIGQUIT,
	syscall.SIGTERM,
	syscall.SIGPIPE,
	syscall.SIGALRM,
	syscall.SIGUSR1,
	syscall.SIGUSR2,
	syscall.SIGCHLD,
	syscall.SIGCONT,
	syscall.SIGTSTP,
	syscall.SIGTTIN,
	syscall.SIGTTOU,
}

func resetSignalDispositions() error {
	for _, sig := range resetDispositionSignals {
		if err := resetSignalDisposition(sig); err != nil {
			return err
		}
	}

	return nil
}
