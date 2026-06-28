//go:build unix && !linux && !darwin

package actionlauncher

func resetSignalMask() error {
	return nil
}
