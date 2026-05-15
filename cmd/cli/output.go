package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	outputText = "text"
	outputJSON = "json"
)

var cliOutputFormat = outputText

type cliActionResult struct {
	Status string `json:"status"`
}

type cliErrorResult struct {
	Error string `json:"error"`
}

func outputIsJSON() bool {
	return cliOutputFormat == outputJSON
}

func validateOutputFormat(format string) error {
	switch format {
	case outputText, outputJSON:
		return nil
	default:
		return fmt.Errorf("--format must be one of %q or %q", outputText, outputJSON)
	}
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeCompactJSON(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

func writeAction(w io.Writer, text string, result any) error {
	if outputIsJSON() {
		if result == nil {
			result = cliActionResult{Status: "ok"}
		}

		return writeJSON(w, result)
	}

	_, err := fmt.Fprintln(w, text)
	return err
}

func runCLIError(err error) {
	if err != nil {
		if outputIsJSON() {
			_ = writeJSON(os.Stderr, cliErrorResult{Error: err.Error()})
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

		os.Exit(1)
	}
}
