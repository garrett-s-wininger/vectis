package actionlauncher

import (
	"strings"
	"testing"
)

func TestCommandBuildsLaunchSpecInvocation(t *testing.T) {
	spec := LaunchSpec{
		Path: "/bin/echo",
		Args: []string{"hello", "world"},
	}

	path, args, err := Command(spec)
	if err != nil {
		t.Fatalf("Command: %v", err)
	}

	spec.Args[0] = "mutated"
	if enabled {
		if path == spec.Path {
			t.Fatalf("launcher path = target path %q, want re-exec launcher", path)
		}

		if len(args) != 4 || args[0] != modeArg || args[1] != "/bin/echo" || args[2] != "hello" || args[3] != "world" {
			t.Fatalf("launcher args = %#v, want mode, target, and copied target args", args)
		}

		return
	}

	if path != "/bin/echo" {
		t.Fatalf("path = %q, want direct target path", path)
	}

	if len(args) != 2 || args[0] != "hello" || args[1] != "world" {
		t.Fatalf("args = %#v, want copied target args", args)
	}
}

func TestCommandRequiresPath(t *testing.T) {
	_, _, err := Command(LaunchSpec{Path: " \t "})
	if err == nil || !strings.Contains(err.Error(), "command path is required") {
		t.Fatalf("Command error = %v, want command path error", err)
	}
}
