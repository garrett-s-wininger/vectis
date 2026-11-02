//go:build e2e

package local_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestE2ELocalRunControls(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local process cleanup uses Unix signals")
	}

	root := repoRoot(t)
	cli := e2eBinaryPath(t, root, "VECTIS_E2E_CLI", "vectis-cli")
	local := e2eBinaryPath(t, root, "VECTIS_E2E_LOCAL", "vectis-local")
	ports := localRestoreDrillPortSet()
	requireLocalPortsAvailable(t, append(ports.all(), 19092))

	dataHome := t.TempDir()
	env := localRestoreDrillEnvWithOverrides(localRestoreDrillEnv(t, root, dataHome, ports), map[string]string{
		"VECTIS_WORKER_CORE_METRICS_PORT": "19092", "VECTIS_CATALOG_INTERVAL": "100ms", "VECTIS_RECONCILER_INTERVAL": "100ms",
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
	base := fmt.Sprintf("http://localhost:%d", ports.api)
	request := func(method, path string, result any) int {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, method, base+path, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if result != nil && resp.StatusCode < 300 {
			if err := json.NewDecoder(resp.Body).Decode(result); err != nil {
				t.Fatal(err)
			}
		}

		return resp.StatusCode
	}

	submit := func() string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		result, err := runCommand(ctx, root, env, nil, cli, "--format", "json", "jobs", "run", filepath.Join(root, "examples", "run-controls.json"))
		if err != nil {
			t.Fatal(err)
		}

		var run jobRunResult
		if err := json.Unmarshal([]byte(result.stdout), &run); err != nil {
			t.Fatal(err)
		}

		return run.RunID
	}

	first := submit()
	deadline := time.Now().Add(20 * time.Second)
	for {
		var tasks struct {
			Data []struct {
				TaskKey string `json:"task_key"`
				Status  string `json:"status"`
			} `json:"data"`
		}

		request(http.MethodGet, "/api/v1/runs/"+first+"/tasks", &tasks)
		running := false
		for _, task := range tasks.Data {
			if task.TaskKey == "heartbeat" && task.Status == "running" {
				running = true
			}
		}

		if running {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("heartbeat did not start")
		}

		time.Sleep(time.Second)
	}

	queued := submit()
	deadline = time.Now().Add(5 * time.Second)
	for {
		status := request(http.MethodPost, "/api/v1/runs/"+queued+"/cancel", nil)
		if status == http.StatusAccepted {
			break
		}

		if status != http.StatusServiceUnavailable || time.Now().After(deadline) {
			t.Fatalf("queued cancellation returned %d", status)
		}

		time.Sleep(time.Second)
	}

	if status := request(http.MethodPost, "/api/v1/runs/"+first+"/cancel", nil); status != http.StatusAccepted {
		t.Fatalf("running cancellation returned %d", status)
	}

	for _, id := range []string{first, queued} {
		_, final := waitForRunTerminal(t, root, env, cli, id, 20*time.Second)
		if final.Status != "cancelled" {
			t.Fatalf("run %s status=%s", id, final.Status)
		}
	}

	var replay struct {
		RunID         string `json:"run_id"`
		ReplayOfRunID string `json:"replay_of_run_id"`
	}

	if status := request(http.MethodPost, "/api/v1/runs/"+first+"/replay", &replay); status != http.StatusAccepted {
		t.Fatalf("replay status=%d", status)
	}

	if replay.RunID == "" || replay.RunID == first || replay.ReplayOfRunID != first {
		t.Fatalf("unexpected replay %+v", replay)
	}

	_, final := waitForRunTerminal(t, root, env, cli, replay.RunID, 60*time.Second)
	if final.Status != "succeeded" {
		t.Fatalf("replayed run status=%s", final.Status)
	}

	original, err := showRun(t, root, env, cli, first)
	if err != nil || original.Status != "cancelled" {
		t.Fatalf("original changed: %+v, %v", original, err)
	}
}
