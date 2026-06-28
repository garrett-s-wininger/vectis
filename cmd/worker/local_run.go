package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/job"
)

type localLogClient struct {
	stdout io.Writer
	stderr io.Writer
}

type synchronizedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *synchronizedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

func newLocalLogClient(stdout, stderr io.Writer) *localLogClient {
	return &localLogClient{stdout: stdout, stderr: stderr}
}

func (c *localLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &localLogStream{stdout: c.stdout, stderr: c.stderr}, nil
}

func (c *localLogClient) Close() error {
	return nil
}

type localLogStream struct {
	stdout io.Writer
	stderr io.Writer
}

func (s *localLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return nil
	}

	w := s.stdout
	if chunk.GetStream() == api.Stream_STREAM_STDERR {
		w = s.stderr
	}

	_, err := fmt.Fprintln(w, string(chunk.GetData()))
	return err
}

func (s *localLogStream) CloseSend() error {
	return nil
}

func runLocalJob(ctx context.Context, jobPath, workspace string, stdout, stderr io.Writer) error {
	if jobPath == "" {
		return fmt.Errorf("job-json path is required")
	}

	if workspace == "" {
		return fmt.Errorf("workspace is required")
	}

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		return fmt.Errorf("resolve workspace: %w", err)
	}

	info, err := os.Stat(absWorkspace)
	if err != nil {
		return fmt.Errorf("stat workspace: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("workspace is not a directory: %s", absWorkspace)
	}

	body, err := os.ReadFile(jobPath)
	if err != nil {
		return fmt.Errorf("read job definition: %w", err)
	}

	var j api.Job
	if err := json.Unmarshal(body, &j); err != nil {
		return fmt.Errorf("invalid job JSON: %w", err)
	}

	if j.Id == nil || j.GetId() == "" {
		id := "local-job"
		j.Id = &id
	}

	if j.RunId == nil || j.GetRunId() == "" {
		runID := fmt.Sprintf("local-%d-%s", time.Now().UTC().Unix(), uuid.NewString())
		j.RunId = &runID
	}

	safeStdout := &synchronizedWriter{w: stdout}
	safeStderr := &synchronizedWriter{w: stderr}
	logger := interfaces.NewLogger("worker-local").WithOutput(safeStderr)
	client := newLocalLogClient(safeStdout, safeStderr)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(client)

	executor := job.NewExecutor()
	logDone := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = logDone

	err = executor.ExecuteJobInWorkspace(ctx, &j, client, logger, absWorkspace)

	select {
	case waiter := <-logDone:
		if waitErr := waiter.WaitForDone(10 * time.Second); waitErr != nil && err == nil {
			err = waitErr
		}
	default:
	}

	if err != nil {
		return err
	}

	return nil
}

var runLocalWorkspace string

var runLocalCmd = &cobra.Command{
	Use:   "run-local [job-json]",
	Short: "Execute a Vectis job locally without queue, database, or log service",
	Long:  `Execute a Vectis JSON job graph directly in a local workspace. Intended for developer and CI dogfood checks.`,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := runLocalJob(cmd.Context(), args[0], runLocalWorkspace, os.Stdout, os.Stderr); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	runLocalCmd.Flags().StringVar(&runLocalWorkspace, "workspace", "", "Workspace directory for local job execution")
	if err := runLocalCmd.MarkFlagRequired("workspace"); err != nil {
		panic(err)
	}

	rootCmd.AddCommand(runLocalCmd)
}
