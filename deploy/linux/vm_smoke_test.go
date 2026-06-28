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

	if result.Status != "verified" || result.Provider != "recording" || result.GuestCleaned {
		t.Fatalf("unexpected result: %+v", result)
	}

	if !manager.started {
		t.Fatalf("VM manager was not asked to start the instance")
	}

	if manager.copiedLocal != out || manager.copiedRemote != vmSmokeRemoteArtifactDir {
		t.Fatalf("copy = %s -> %s, want %s -> %s", manager.copiedLocal, manager.copiedRemote, out, vmSmokeRemoteArtifactDir)
	}

	if _, err := os.Stat(filepath.Join(out, filepath.FromSlash(vmSmokeBinDir), "vectis-cli")); err != nil {
		t.Fatalf("expected smoke stub: %v", err)
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

	requireRecordedCommand(t, manager, []string{"grep", "-qx", vmSmokeGuestProfile, vmSmokeGuestProfilePath})
	requireRecordedCommand(t, manager, []string{"grep", "-qx", expectedVMSmokeGuestPrepVersion(), vmSmokeGuestPrepPath})

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
		"/etc/systemd/system/vectis-artifact.service",
		"/etc/systemd/system/vectis-catalog.service",
		"/etc/systemd/system/vectis-cell-ingress.service",
		"/etc/systemd/system/vectis-cron.service",
		"/etc/systemd/system/vectis-db-migrate.service",
		"/etc/systemd/system/vectis-docs.service",
		"/etc/systemd/system/vectis-log-forwarder.service",
		"/etc/systemd/system/vectis-log.service",
		"/etc/systemd/system/vectis-orchestrator.service",
		"/etc/systemd/system/vectis-queue.service",
		"/etc/systemd/system/vectis-reconciler.service",
		"/etc/systemd/system/vectis-registry.service",
		"/etc/systemd/system/vectis-secrets.service",
		"/etc/systemd/system/vectis-spiffe.service",
		"/etc/systemd/system/vectis-worker-core.service",
		"/etc/systemd/system/vectis-worker.service",
		"/etc/systemd/system/vectis.target",
	})

	requireRecordedCommand(t, manager, append([]string{"sudo", "systemctl", "enable"}, expectedSmokeTargetMemberUnits()...))
	requireRecordedCommand(t, manager, []string{"sudo", "systemctl", "start", "vectis.target"})
	requireRecordedCommand(t, manager, []string{"sudo", "systemctl", "is-active", "--quiet", "vectis.target"})
}

func TestRunVMSmokeVerifyRequiresPreparedInstance(t *testing.T) {
	manager := &recordingVMManager{exists: false}

	_, err := RunVMSmokeVerify(context.Background(), VMSmokeOptions{
		Manager:  manager,
		Instance: "smoke-vm",
	})
	if err == nil {
		t.Fatal("expected missing prepared VM error")
	}

	if !strings.Contains(err.Error(), "mage vmDeploySmokePrepare") {
		t.Fatalf("missing prepared VM error = %q, want prep target hint", err)
	}

	if manager.created {
		t.Fatal("deploy smoke should not create a VM during verification")
	}

	if manager.started {
		t.Fatal("deploy smoke should not start a missing VM")
	}
}

func TestRunVMSmokeVerifyCleansGuestAfterFailure(t *testing.T) {
	failCommand := []string{"sudo", "systemctl", "start", "vectis.target"}
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
	requireRecordedCommand(t, manager, append([]string{"sudo", "systemctl", "disable"}, expectedSmokeTargetMemberUnits()...))
	requireRecordedCommandContaining(t, manager,
		[]string{"sudo", "rm", "-f"},
		vmSmokeMarkerDestination,
		"/etc/systemd/system/vectis-api.service",
		"/etc/vectis/vectis.env",
	)
}

func expectedSmokeTargetMemberUnits() []string {
	units := sortedKeys(expectedStandaloneExecs)
	return append([]string{}, units...)
}

type recordingVMManager struct {
	exists        bool
	created       bool
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

func (m *recordingVMManager) InstanceStatus(context.Context, string) (string, error) {
	return "Stopped", nil
}

func (m *recordingVMManager) Create(context.Context, string, string) error {
	m.created = true
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

func (m *recordingVMManager) CopyDirFrom(context.Context, string, string, string) error {
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
