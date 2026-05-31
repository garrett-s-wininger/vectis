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
	CellID            string `json:"cell_id"`
	IngressRequired   bool   `json:"ingress_required"`
	IngressConfigured bool   `json:"ingress_configured"`
	IngressReachable  bool   `json:"ingress_reachable"`
	Status            string `json:"status"`
	HTTPStatus        int    `json:"http_status,omitempty"`
	Error             string `json:"error,omitempty"`
	Queued            int64  `json:"queued"`
	Stuck             int64  `json:"stuck"`
	CatalogPending    int64  `json:"catalog_pending"`
	CatalogFailed     int64  `json:"catalog_failed"`
	CatalogTotal      int64  `json:"catalog_total"`
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
	defer resp.Body.Close()

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
	fmt.Fprintln(tw, "CELL\tSTATUS\tQUEUED\tSTUCK\tCATALOG P/F/T\tERROR")
	for _, cell := range result.Cells {
		status := strings.TrimSpace(cell.Status)
		if status == "" {
			status = "unknown"
		}

		errText := strings.TrimSpace(cell.Error)
		if errText == "" {
			errText = "-"
		}

		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d/%d/%d\t%s\n",
			cell.CellID,
			status,
			cell.Queued,
			cell.Stuck,
			cell.CatalogPending,
			cell.CatalogFailed,
			cell.CatalogTotal,
			errText,
		)
	}

	return tw.Flush()
}

var cellsCmd = &cobra.Command{
	Use:     "cells",
	Short:   "Inspect execution cells",
	Long:    `Inspect execution cell routing, queued run pressure, stuck dispatches, and catalog fan-in state.`,
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var cellsStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cell routing and catalog status",
	Long:  `Show cell ingress route readiness, queued and stuck run counts, and catalog inbox counts by cell.`,
	Args:  cobra.NoArgs,
	Run:   runCellsStatus,
}
