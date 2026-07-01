package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

type auditListOptions struct {
	EventType     string
	ActorID       int64
	TargetID      int64
	CorrelationID string
	Since         string
	Until         string
	Limit         int
}

type auditEventListResult struct {
	Events []auditEventResult `json:"events"`
	Limit  int                `json:"limit"`
}

type auditEventResult struct {
	ID            int64           `json:"id"`
	EventType     string          `json:"event_type"`
	ActorID       *int64          `json:"actor_id,omitempty"`
	TargetID      *int64          `json:"target_id,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	IPAddress     string          `json:"ip_address,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CreatedAt     string          `json:"created_at,omitempty"`
}

var auditCmd = &cobra.Command{
	Use:     "audit",
	Short:   "Review audit events",
	Long:    `Review API audit events for privileged changes, incident response, and retention evidence.`,
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var auditListCmd = &cobra.Command{
	Use:   "list",
	Short: "List audit events",
	Long:  `List API audit events with optional filters. Use --format json to retain machine-readable export evidence.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(auditList(os.Stdout, auditListOptions{
			EventType:     auditListEventType,
			ActorID:       auditListActorID,
			TargetID:      auditListTargetID,
			CorrelationID: auditListCorrelationID,
			Since:         auditListSince,
			Until:         auditListUntil,
			Limit:         auditListLimit,
		}))
	},
}

func configureAuditListFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&auditListEventType, "event-type", "", "Only include this audit event type")
	cmd.Flags().Int64Var(&auditListActorID, "actor-id", 0, "Only include events performed by this local user ID")
	cmd.Flags().Int64Var(&auditListTargetID, "target-id", 0, "Only include events for this local target ID")
	cmd.Flags().StringVar(&auditListCorrelationID, "correlation-id", "", "Only include events with this request correlation ID")
	cmd.Flags().StringVar(&auditListSince, "since", "", "Only include events at or after this RFC3339 timestamp or YYYY-MM-DD date")
	cmd.Flags().StringVar(&auditListUntil, "until", "", "Only include events at or before this RFC3339 timestamp or YYYY-MM-DD date")
	cmd.Flags().IntVar(&auditListLimit, "limit", 100, "Maximum events to return (1-1000)")
}

func auditList(w io.Writer, opts auditListOptions) error {
	params := url.Values{}
	setTrimmedQueryParam(params, "event_type", opts.EventType)
	setTrimmedQueryParam(params, "correlation_id", opts.CorrelationID)
	setTrimmedQueryParam(params, "since", opts.Since)
	setTrimmedQueryParam(params, "until", opts.Until)
	if opts.ActorID > 0 {
		params.Set("actor_id", strconv.FormatInt(opts.ActorID, 10))
	}
	if opts.TargetID > 0 {
		params.Set("target_id", strconv.FormatInt(opts.TargetID, 10))
	}
	if opts.Limit > 0 {
		params.Set("limit", strconv.Itoa(opts.Limit))
	}

	path := appendQueryParams("/api/v1/audit/events", params)
	var result auditEventListResult
	if err := doJSONAPI(http.MethodGet, path, nil, http.StatusOK, &result); err != nil {
		return err
	}

	if outputIsJSON() {
		return writeJSON(w, result)
	}

	return writeAuditEventsText(w, result)
}

func writeAuditEventsText(w io.Writer, result auditEventListResult) error {
	if len(result.Events) == 0 {
		_, err := fmt.Fprintln(w, "No audit events found")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tCREATED\tEVENT\tACTOR\tTARGET\tCORRELATION\tIP\tMETADATA")
	for _, event := range result.Events {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			event.ID,
			defaultDash(event.CreatedAt),
			defaultDash(event.EventType),
			formatOptionalInt64(event.ActorID),
			formatOptionalInt64(event.TargetID),
			defaultDash(event.CorrelationID),
			defaultDash(event.IPAddress),
			formatAuditMetadata(event.Metadata),
		)
	}

	return tw.Flush()
}

func formatOptionalInt64(v *int64) string {
	if v == nil {
		return "-"
	}

	return strconv.FormatInt(*v, 10)
}

func defaultDash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "-"
	}

	return value
}

func formatAuditMetadata(metadata json.RawMessage) string {
	if len(metadata) == 0 {
		return "-"
	}

	var compact bytes.Buffer
	if err := json.Compact(&compact, metadata); err == nil {
		return compact.String()
	}

	return string(metadata)
}
