//go:build !unix

package actionlauncher

import "os/exec"

func prepare() error {
	return nil
}

func execTarget(target string, argv, env []string) error {
	cmd := exec.Command(target, argv[1:]...)
	cmd.Env = env
	return cmd.Run()
}
