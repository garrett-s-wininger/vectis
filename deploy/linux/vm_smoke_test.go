package linux

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRunVMSmokeVerifyUsesStructuredGuestCommands(t *testing.T) {
	manager := &recordingVMManager{exists: true}
	out := t.TempDir()

	result, err := RunVMSmokeVerify(context.Background(), VMSmokeOptions{
		Manager:       manager,
		Instance:      "smoke-vm",
		ArtifactDir:   out,
		KeepArtifacts: true,
	})

	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "verified" || result.Provider != "recording" || result.Profile != VMSmokeProfileUnits || result.GuestCleaned {
		t.Fatalf("unexpected result: %+v", result)
	}

	if !manager.started {
		t.Fatalf("VM manager was not asked to start the instance")
	}

	if manager.copiedLocal != out || manager.copiedRemote != vmSmokeRemoteArtifactDir {
		t.Fatalf("copy = %s -> %s, want %s -> %s", manager.copiedLocal, manager.copiedRemote, out, vmSmokeRemoteArtifactDir)
	}

	if _, err := os.Stat(filepath.Join(out, filepath.FromSlash(vmSmokeBinDir), "vectis-cli")); err != nil {
		t.Fatalf("expected local smoke stub: %v", err)
	}

	for _, command := range manager.shellCommands {
		if len(command) >= 3 && command[0] == "sudo" && command[1] == "sh" && (command[2] == "-s" || command[2] == "-c") {
			t.Fatalf("guest smoke should not run shell scripts, got command %v", command)
		}
	}

	requireRecordedCommand(t, manager, []string{
		"sudo", "install", "-D", "-m", "0640", "-o", "root", "-g", "vectis",
		vmSmokeRemoteArtifactDir + "/env/vectis.env.example",
		"/etc/vectis/vectis.env",
	})

	requireRecordedCommandCount(t, manager, []string{
		"sudo", "install", "-D", "-m", "0644", "-o", "root", "-g", "root",
		vmSmokeRemoteArtifactDir + "/sysusers.d/vectis.conf",
		"/usr/lib/sysusers.d/vectis.conf",
	}, 1)

	requireRecordedCommandCount(t, manager, []string{
		"sudo", "install", "-D", "-m", "0644", "-o", "root", "-g", "root",
		vmSmokeRemoteArtifactDir + "/tmpfiles.d/vectis.conf",
		"/usr/lib/tmpfiles.d/vectis.conf",
	}, 1)

	requireRecordedCommand(t, manager, []string{
		"sudo", "systemd-analyze", "verify",
		"/etc/systemd/system/vectis-api.service",
		"/etc/systemd/system/vectis-catalog.service",
		"/etc/systemd/system/vectis-cell-ingress.service",
		"/etc/systemd/system/vectis-cron.service",
		"/etc/systemd/system/vectis-db-migrate.service",
		"/etc/systemd/system/vectis-docs.service",
		"/etc/systemd/system/vectis-local.service",
		"/etc/systemd/system/vectis-log-forwarder.service",
		"/etc/systemd/system/vectis-log.service",
		"/etc/systemd/system/vectis-queue.service",
		"/etc/systemd/system/vectis-reconciler.service",
		"/etc/systemd/system/vectis-registry.service",
		"/etc/systemd/system/vectis-worker.service",
		"/etc/systemd/system/vectis.target",
	})
}

func TestPrepareVMSmokeGuestArtifactsLocalProfileRequiresBinaryDir(t *testing.T) {
	_, err := prepareVMSmokeGuestArtifacts(VMSmokeOptions{
		Profile:     VMSmokeProfileLocal,
		ArtifactDir: t.TempDir(),
	}, []string{"vectis-local"})
	if err == nil {
		t.Fatal("expected missing binary directory error")
	}

	if !strings.Contains(err.Error(), "--binary-dir") {
		t.Fatalf("error = %v, want --binary-dir guidance", err)
	}
}

func TestRunVMSmokeVerifyLocalProfileInstallsRealBinaryWrappers(t *testing.T) {
	manager := &recordingVMManager{exists: true}
	out := t.TempDir()
	binaryDir := t.TempDir()

	for _, binary := range vmSmokeLocalRequiredBinaries() {
		writeFakeVMSmokeBinary(t, binaryDir, binary)
	}

	result, err := RunVMSmokeVerify(context.Background(), VMSmokeOptions{
		Manager:       manager,
		Instance:      "smoke-vm",
		Profile:       VMSmokeProfileLocal,
		BinaryDir:     binaryDir,
		ArtifactDir:   out,
		KeepArtifacts: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if result.Status != "verified" || result.Profile != VMSmokeProfileLocal || result.GuestCleaned {
		t.Fatalf("unexpected result: %+v", result)
	}

	wrapper, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(vmSmokeBinDir), "vectis-local"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(wrapper), vmSmokeMarker) || !strings.Contains(string(wrapper), vmSmokeGuestRealBinDir+"/vectis-local") {
		t.Fatalf("local wrapper did not point at real smoke binary:\n%s", wrapper)
	}

	realBinary, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(vmSmokeRealBinDir), "vectis-local"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(realBinary), "fake vectis-local") {
		t.Fatalf("real binary copy = %q", realBinary)
	}

	stub, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(vmSmokeBinDir), "vectis-cli"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(stub), vmSmokeMarker) || !strings.Contains(string(stub), "exit 0") {
		t.Fatalf("manifest-only binary should remain a smoke stub:\n%s", stub)
	}

	requireRecordedCommand(t, manager, []string{
		"sudo", "install", "-D", "-m", "0755", "-o", "root", "-g", "root",
		vmSmokeRemoteArtifactDir + "/smoke/real-bin/vectis-local",
		vmSmokeGuestRealBinDir + "/vectis-local",
	})

	requireRecordedCommand(t, manager, []string{
		"sudo", "install", "-D", "-m", "0755", "-o", "root", "-g", "root",
		vmSmokeRemoteArtifactDir + "/smoke/bin/vectis-local",
		"/usr/bin/vectis-local",
	})

	requireRecordedCommand(t, manager, []string{
		"sudo", "install", "-D", "-m", "0640", "-o", "root", "-g", "vectis",
		vmSmokeRemoteArtifactDir + "/env/vectis-local.env.example",
		"/etc/vectis/vectis-local.env",
	})

	requireRecordedCommand(t, manager, []string{"curl", "--version"})
	requireRecordedCommand(t, manager, []string{"sudo", "systemctl", "start", "vectis-local.service"})
	requireRecordedCommand(t, manager, []string{"sudo", "systemctl", "is-active", "--quiet", "vectis-local.service"})
	requireRecordedCommand(t, manager, []string{"curl", "-fsS", vmSmokeLocalHealthURL})
	requireNoRecordedCommand(t, manager, []string{"sudo", "systemctl", "start", "vectis-db-migrate.service"})
}

func TestRunVMSmokeVerifyCleansGuestAfterFailure(t *testing.T) {
	failCommand := []string{"sudo", "systemctl", "start", "vectis-db-migrate.service"}
	manager := &recordingVMManager{exists: true, failCommand: failCommand}

	_, err := RunVMSmokeVerify(context.Background(), VMSmokeOptions{
		Manager:     manager,
		Instance:    "smoke-vm",
		ArtifactDir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected verify failure")
	}

	requireRecordedCommand(t, manager, failCommand)
	requireRecordedCommandContaining(t, manager,
		[]string{"sudo", "rm", "-f"},
		vmSmokeMarkerDestination,
		"/etc/systemd/system/vectis-api.service",
		"/etc/vectis/vectis.env",
	)
}

func writeFakeVMSmokeBinary(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, name)
	content := []byte("#!/bin/sh\necho fake " + name + "\n")
	if err := os.WriteFile(path, content, 0o755); err != nil {
		t.Fatal(err)
	}
}

type recordingVMManager struct {
	exists        bool
	started       bool
	copiedLocal   string
	copiedRemote  string
	failCommand   []string
	shellCommands [][]string
}

func (m *recordingVMManager) Provider() string {
	return "recording"
}

func (m *recordingVMManager) CheckAvailable() error {
	return nil
}

func (m *recordingVMManager) InstanceExists(context.Context, string) (bool, error) {
	return m.exists, nil
}

func (m *recordingVMManager) Create(context.Context, string, string) error {
	m.exists = true
	return nil
}

func (m *recordingVMManager) Start(context.Context, string) error {
	m.started = true
	return nil
}

func (m *recordingVMManager) Stop(context.Context, string) error {
	return nil
}

func (m *recordingVMManager) Delete(context.Context, string) error {
	return nil
}

func (m *recordingVMManager) CopyDir(_ context.Context, localDir, _ string, remoteDir string) error {
	m.copiedLocal = localDir
	m.copiedRemote = remoteDir
	return nil
}

func (m *recordingVMManager) Shell(_ context.Context, _ string, stdin io.Reader, args ...string) error {
	if stdin != nil {
		return io.ErrUnexpectedEOF
	}

	command := append([]string{}, args...)
	m.shellCommands = append(m.shellCommands, command)
	if reflect.DeepEqual(command, m.failCommand) {
		return errors.New("recording VM command failed")
	}

	return nil
}

func requireRecordedCommand(t *testing.T, manager *recordingVMManager, want []string) {
	t.Helper()
	for _, got := range manager.shellCommands {
		if reflect.DeepEqual(got, want) {
			return
		}
	}

	t.Fatalf("missing guest command %v", want)
}

func requireNoRecordedCommand(t *testing.T, manager *recordingVMManager, want []string) {
	t.Helper()
	for _, got := range manager.shellCommands {
		if reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected guest command %v", want)
		}
	}
}

func requireRecordedCommandContaining(t *testing.T, manager *recordingVMManager, prefix []string, requiredArgs ...string) {
	t.Helper()
	for _, got := range manager.shellCommands {
		if len(got) < len(prefix) || !reflect.DeepEqual(got[:len(prefix)], prefix) {
			continue
		}

		missing := false
		for _, requiredArg := range requiredArgs {
			if !contains(got, requiredArg) {
				missing = true
				break
			}
		}

		if !missing {
			return
		}
	}

	t.Fatalf("missing guest command with prefix %v and args %v", prefix, requiredArgs)
}

func requireRecordedCommandCount(t *testing.T, manager *recordingVMManager, want []string, count int) {
	t.Helper()
	var gotCount int
	for _, got := range manager.shellCommands {
		if reflect.DeepEqual(got, want) {
			gotCount++
		}
	}

	if gotCount != count {
		t.Fatalf("guest command %v recorded %d times, want %d", want, gotCount, count)
	}
}
