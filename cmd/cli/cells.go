package main

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
)

type cellsStatusResult struct {
	Cells []cellStatusResult `json:"cells"`
}

type cellStatusResult struct {
	CellID                  string                  `json:"cell_id"`
	Ready                   bool                    `json:"ready"`
	IngressRequired         bool                    `json:"ingress_required"`
	IngressConfigured       bool                    `json:"ingress_configured"`
	IngressReachable        bool                    `json:"ingress_reachable"`
	Status                  string                  `json:"status"`
	HTTPStatus              int                     `json:"http_status,omitempty"`
	Error                   string                  `json:"error,omitempty"`
	Queued                  int64                   `json:"queued"`
	Stuck                   int64                   `json:"stuck"`
	TaskContinuationPending int64                   `json:"task_continuation_pending"`
	TaskFinalizationPending int64                   `json:"task_finalization_pending"`
	CatalogPending          int64                   `json:"catalog_pending"`
	CatalogFailed           int64                   `json:"catalog_failed"`
	CatalogTotal            int64                   `json:"catalog_total"`
	Checks                  []cellStatusCheckResult `json:"checks"`
}

type cellStatusCheckResult struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Summary string `json:"summary,omitempty"`
}

func runCellsStatus(cmd *cobra.Command, args []string) {
	runCLIError(cellsStatus(os.Stdout))
}

func cellsStatus(w io.Writer) error {
	req, err := newAPIRequest(http.MethodGet, "/api/v1/cells/status", nil)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var result cellsStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}

	if outputIsJSON() {
		return writeJSON(w, result)
	}

	return writeCellsStatusText(w, result)
}

func writeCellsStatusText(w io.Writer, result cellsStatusResult) error {
	if len(result.Cells) == 0 {
		_, err := fmt.Fprintln(w, "No cells found")
		return err
	}

	sort.Slice(result.Cells, func(i, j int) bool {
		return result.Cells[i].CellID < result.Cells[j].CellID
	})

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "CELL\tREADY\tROUTE\tQUEUED\tSTUCK\tTASK REPAIR C/F\tCATALOG P/F/T\tCHECKS\tERROR")
	for _, cell := range result.Cells {
		status := strings.TrimSpace(cell.Status)
		if status == "" {
			status = "unknown"
		}

		errText := strings.TrimSpace(cell.Error)
		if errText == "" {
			errText = "-"
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d/%d\t%d/%d/%d\t%s\t%s\n",
			cell.CellID,
			readyText(cell.Ready),
			status,
			cell.Queued,
			cell.Stuck,
			cell.TaskContinuationPending,
			cell.TaskFinalizationPending,
			cell.CatalogPending,
			cell.CatalogFailed,
			cell.CatalogTotal,
			formatCellChecks(cell.Checks),
			errText,
		)
	}

	return tw.Flush()
}

func readyText(ready bool) string {
	if ready {
		return "yes"
	}

	return "no"
}

func formatCellChecks(checks []cellStatusCheckResult) string {
	parts := make([]string, 0, len(checks))
	for _, check := range checks {
		status := strings.TrimSpace(check.Status)
		if status == "" || status == "pass" {
			continue
		}

		id := strings.TrimSpace(check.ID)
		if id == "" {
			id = "check"
		}
		parts = append(parts, id+":"+status)
	}

	if len(parts) == 0 {
		return "-"
	}

	return strings.Join(parts, ",")
}

var cellsCmd = &cobra.Command{
	Use:     "cells",
	Short:   "Inspect execution cells",
	Long:    `Inspect execution cell routing, queued run pressure, task repair pressure, and catalog fan-in state.`,
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var cellsStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cell routing and catalog status",
	Long:  `Show cell ingress route readiness, queued run counts, dispatch and task repair pressure, and catalog inbox counts by cell.`,
	Args:  cobra.NoArgs,
	Run:   runCellsStatus,
}
