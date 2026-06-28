//go:build !linux

package interfaces

import "os/exec"

func configureCommandParentDeathSignal(*exec.Cmd) {}
