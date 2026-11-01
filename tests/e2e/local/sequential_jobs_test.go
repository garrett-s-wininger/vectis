//go:build e2e

package local_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestE2ELocalSequentialJobs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("vectis-local e2e process cleanup uses Unix signals")
	}
	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")
	requireExecutable(t, cli, "vectis-cli")
	requireExecutable(t, local, "vectis-local")
	ports := localRestoreDrillPortSet()
	requireLocalPortsAvailable(t, append(ports.all(), 19092))
	dataHome := t.TempDir()
	env := localRestoreDrillEnv(t, root, dataHome, ports)
	env = localRestoreDrillEnvWithOverrides(env, map[string]string{
		"VECTIS_WORKER_CORE_METRICS_PORT": "19092",
		"VECTIS_RECONCILER_INTERVAL":      "100ms",
		"VECTIS_CATALOG_INTERVAL":         "100ms",
	})
	prepareLocalRestoreDrillEvidenceDirs(t, dataHome)
	proc := startVectisLocal(t, root, env, local, "--auth=false", "--setup=off", "--ui=false")
	t.Cleanup(func() { stopVectisLocal(t, proc) })
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("local stderr:\n%s", proc.stderr.String())
		}
	})
	waitForAPIReady(t, proc, ports.readyURL(), 2*time.Minute)
	first := filepath.Join(dataHome, "first.json")
	if err := os.WriteFile(first, []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo first"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var exampleRun string
	for index, path := range []string{first, filepath.Join(root, "examples", "log-viewer.json")} {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "run", path)
		cancel()
		if err != nil {
			t.Fatalf("submit job: %v\n%s", err, proc.stderr.String())
		}
		var run jobRunResult
		if err := json.Unmarshal([]byte(result.stdout), &run); err != nil {
			t.Fatal(err)
		}
		statuses, final := waitForRunTerminal(t, root, env, cli, run.RunID, 90*time.Second)
		want := "succeeded"
		if index == 1 {
			want = "failed"
			exampleRun = run.RunID
		}
		if final.Status != want {
			t.Fatalf("job %d status=%s want=%s observed=%v\n%s", index, final.Status, want, statuses, proc.stderr.String())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, err := runCommand(ctx, root, env, nil, cli, "runs", "repair", "mark-queued", exampleRun)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	_, final := waitForRunTerminal(t, root, env, cli, exampleRun, 90*time.Second)
	if final.Status != "failed" {
		t.Fatalf("second attempt status=%s, want failed", final.Status)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "runs", "tasks", exampleRun)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	var tasks runTasksResult
	if err := json.Unmarshal([]byte(result.stdout), &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Data) == 0 {
		t.Fatal("missing task history")
	}
	for _, task := range tasks.Data {
		if len(task.Attempts) != 2 {
			t.Fatalf("task %s has %d attempts, want 2", task.TaskID, len(task.Attempts))
		}
	}
	stopVectisLocal(t, proc)
	proc = startVectisLocal(t, root, env, local, "--auth=false", "--setup=off", "--ui=false")
	waitForAPIReady(t, proc, ports.readyURL(), 2*time.Minute)
	_, final = waitForRunTerminal(t, root, env, cli, exampleRun, 30*time.Second)
	if final.Status != "failed" {
		t.Fatalf("restored status=%s, want failed", final.Status)
	}
	db, err := sql.Open("sqlite3", "file:"+filepath.Join(dataHome, "vectis", "global", "db.sqlite3")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var outstanding int
		if err := db.QueryRow("SELECT COUNT(*) FROM cell_catalog_events WHERE status <> 'applied'").Scan(&outstanding); err != nil {
			t.Fatal(err)
		}
		if outstanding == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("catalog has %d unresolved events after restart", outstanding)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
