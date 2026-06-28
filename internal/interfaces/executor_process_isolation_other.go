//go:build !unix

package interfaces

import "os/exec"

func configureCommandProcessIsolation(*exec.Cmd) {}
