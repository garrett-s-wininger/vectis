package gitcmd

var noAutoMaintenanceSettings = [][2]string{
	{"gc.auto", "0"},
	{"gc.autoDetach", "false"},
	{"gc.autoPackLimit", "0"},
	{"gc.writeCommitGraph", "false"},
	{"maintenance.auto", "false"},
	{"maintenance.strategy", "none"},
	{"fetch.writeCommitGraph", "false"},
	{"fetch.unpackLimit", "1"},
}

func NoAutoMaintenanceSettings() [][2]string {
	return append([][2]string(nil), noAutoMaintenanceSettings...)
}

func NoAutoMaintenanceArgs(args ...string) []string {
	out := make([]string, 0, len(noAutoMaintenanceSettings)*2+len(args))
	for _, setting := range noAutoMaintenanceSettings {
		out = append(out, "-c", setting[0]+"="+setting[1])
	}

	out = append(out, args...)
	return out
}
