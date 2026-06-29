package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"vectis/internal/storageverify"
)

var storageVerifyDir string

var storageCmd = &cobra.Command{
	Use:     "storage",
	Short:   "Inspect durable file storage",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var storageVerifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify durable storage integrity",
	Long: `Verify Vectis file-backed durable state without modifying it.

Use a subcommand for the storage surface being checked. The verifier reads the
native on-disk format for each surface and exits non-zero when corrupt files,
digest mismatches, unreplayable quarantine files, or malformed records are
found.`,
	Args: cobra.NoArgs,
	Run:  showCommandHelp,
}

var storageVerifyArtifactCmd = &cobra.Command{
	Use:   "artifact --dir <artifact-storage-dir>",
	Short: "Verify content-addressed artifact blobs",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeStorageVerification(cmd.Context(), os.Stdout, storageverify.SurfaceArtifact, storageVerifyDir))
	},
}

var storageVerifyLogsCmd = &cobra.Command{
	Use:   "logs --dir <log-storage-dir>",
	Short: "Verify durable run log files",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeStorageVerification(cmd.Context(), os.Stdout, storageverify.SurfaceLogs, storageVerifyDir))
	},
}

var storageVerifyQueueCmd = &cobra.Command{
	Use:   "queue --dir <queue-persistence-dir>",
	Short: "Verify queue snapshot and WAL files",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeStorageVerification(cmd.Context(), os.Stdout, storageverify.SurfaceQueue, storageVerifyDir))
	},
}

var storageVerifyLogForwarderSpoolCmd = &cobra.Command{
	Use:   "log-forwarder-spool --dir <spool-dir>",
	Short: "Verify log-forwarder CRC spool files",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeStorageVerification(cmd.Context(), os.Stdout, storageverify.SurfaceLogForwarderSpool, storageVerifyDir))
	},
}

var storageVerifyWorkerLogSpoolCmd = &cobra.Command{
	Use:   "worker-log-spool --dir <pending-spool-dir>",
	Short: "Verify worker pending log spool files",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(writeStorageVerification(cmd.Context(), os.Stdout, storageverify.SurfaceWorkerLogSpool, storageVerifyDir))
	},
}

func configureStorageVerifyDirFlag(cmd *cobra.Command, usage string) {
	cmd.Flags().StringVar(&storageVerifyDir, "dir", "", usage)
	_ = cmd.MarkFlagRequired("dir")
}

func writeStorageVerification(ctx context.Context, w io.Writer, surface, dir string) error {
	report, err := storageverify.Verify(ctx, surface, dir)
	if err != nil {
		return err
	}

	if outputIsJSON() {
		if err := writeJSON(w, report); err != nil {
			return err
		}
	} else if err := writeStorageVerificationText(w, report); err != nil {
		return err
	}

	if report.Status != storageverify.StatusOK {
		return fmt.Errorf("storage verification failed for %s: %d error(s)", report.Surface, len(report.Errors))
	}

	return nil
}

func writeStorageVerificationText(w io.Writer, report storageverify.Report) error {
	lines := []struct {
		key   string
		value any
	}{
		{"status", report.Status},
		{"surface", report.Surface},
		{"path", report.Path},
		{"checked_files", report.CheckedFiles},
		{"checked_bytes", report.CheckedBytes},
		{"records", report.Records},
		{"batches", report.Batches},
		{"warnings", len(report.Warnings)},
		{"errors", len(report.Errors)},
	}

	for _, line := range lines {
		if _, err := fmt.Fprintf(w, "%s=%v\n", line.key, line.value); err != nil {
			return err
		}
	}

	for i, finding := range report.Warnings {
		if err := writeStorageFindingText(w, "warning", i, finding); err != nil {
			return err
		}
	}

	for i, finding := range report.Errors {
		if err := writeStorageFindingText(w, "error", i, finding); err != nil {
			return err
		}
	}

	return nil
}

func writeStorageFindingText(w io.Writer, kind string, index int, finding storageverify.Finding) error {
	if _, err := fmt.Fprintf(w, "%s[%d].id=%s\n", kind, index, finding.ID); err != nil {
		return err
	}

	if finding.Path != "" {
		if _, err := fmt.Fprintf(w, "%s[%d].path=%s\n", kind, index, finding.Path); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(w, "%s[%d].message=%s\n", kind, index, finding.Message); err != nil {
		return err
	}

	return nil
}
