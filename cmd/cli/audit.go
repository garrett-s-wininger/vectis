package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

const auditExportSchemaVersion = "vectis.audit.export.v1"

type auditListOptions struct {
	EventType     string
	ActorID       int64
	TargetID      int64
	CorrelationID string
	Since         string
	Until         string
	Cursor        string
	Limit         int
}

type auditEventListResult struct {
	Events     []auditEventResult `json:"events"`
	Limit      int                `json:"limit"`
	NextCursor string             `json:"next_cursor,omitempty"`
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

type auditExportOptions struct {
	auditListOptions
	OutputPath  string
	GeneratedAt time.Time
}

type auditExportFilters struct {
	EventType     string `json:"event_type,omitempty"`
	ActorID       *int64 `json:"actor_id,omitempty"`
	TargetID      *int64 `json:"target_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Since         string `json:"since,omitempty"`
	Until         string `json:"until,omitempty"`
}

type auditExportEvidence struct {
	SchemaVersion  string             `json:"schema_version"`
	GeneratedAt    string             `json:"generated_at"`
	Filters        auditExportFilters `json:"filters"`
	Limit          int                `json:"limit"`
	PageCount      int                `json:"page_count"`
	RowCount       int                `json:"row_count"`
	MayBeTruncated bool               `json:"may_be_truncated"`
	NewestEventAt  string             `json:"newest_event_at,omitempty"`
	OldestEventAt  string             `json:"oldest_event_at,omitempty"`
	EventsSHA256   string             `json:"events_sha256"`
	Events         []auditEventResult `json:"events"`
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
			Cursor:        auditListCursor,
			Limit:         auditListLimit,
		}))
	},
}

var auditExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export audit events with evidence",
	Long:  `Export API audit events in a retained evidence envelope. Use --output to write the export JSON to a file.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		runCLIError(auditExport(os.Stdout, auditExportOptions{
			auditListOptions: auditListOptions{
				EventType:     auditExportEventType,
				ActorID:       auditExportActorID,
				TargetID:      auditExportTargetID,
				CorrelationID: auditExportCorrelationID,
				Since:         auditExportSince,
				Until:         auditExportUntil,
				Cursor:        "",
				Limit:         auditExportLimit,
			},
			OutputPath: auditExportOutputPath,
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
	cmd.Flags().StringVar(&auditListCursor, "cursor", "", "Continue listing after a previous audit event cursor")
	cmd.Flags().IntVar(&auditListLimit, "limit", 100, "Maximum events to return (1-1000)")
}

func configureAuditExportFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&auditExportEventType, "event-type", "", "Only include this audit event type")
	cmd.Flags().Int64Var(&auditExportActorID, "actor-id", 0, "Only include events performed by this local user ID")
	cmd.Flags().Int64Var(&auditExportTargetID, "target-id", 0, "Only include events for this local target ID")
	cmd.Flags().StringVar(&auditExportCorrelationID, "correlation-id", "", "Only include events with this request correlation ID")
	cmd.Flags().StringVar(&auditExportSince, "since", "", "Only include events at or after this RFC3339 timestamp or YYYY-MM-DD date")
	cmd.Flags().StringVar(&auditExportUntil, "until", "", "Only include events at or before this RFC3339 timestamp or YYYY-MM-DD date")
	cmd.Flags().IntVar(&auditExportLimit, "limit", 1000, "Maximum events to return (1-1000)")
	cmd.Flags().StringVarP(&auditExportOutputPath, "output", "o", "-", "Export JSON output path, or '-' for stdout")
}

func auditEventsPath(opts auditListOptions) string {
	params := url.Values{}
	setTrimmedQueryParam(params, "event_type", opts.EventType)
	setTrimmedQueryParam(params, "correlation_id", opts.CorrelationID)
	setTrimmedQueryParam(params, "since", opts.Since)
	setTrimmedQueryParam(params, "until", opts.Until)
	setTrimmedQueryParam(params, "cursor", opts.Cursor)
	if opts.ActorID > 0 {
		params.Set("actor_id", strconv.FormatInt(opts.ActorID, 10))
	}
	if opts.TargetID > 0 {
		params.Set("target_id", strconv.FormatInt(opts.TargetID, 10))
	}
	if opts.Limit > 0 {
		params.Set("limit", strconv.Itoa(opts.Limit))
	}

	return appendQueryParams("/api/v1/audit/events", params)
}

func fetchAuditEvents(opts auditListOptions) (auditEventListResult, error) {
	var result auditEventListResult
	if err := doJSONAPI(http.MethodGet, auditEventsPath(opts), nil, http.StatusOK, &result); err != nil {
		return auditEventListResult{}, err
	}

	return result, nil
}

func auditList(w io.Writer, opts auditListOptions) error {
	result, err := fetchAuditEvents(opts)
	if err != nil {
		return err
	}

	if outputIsJSON() {
		return writeJSON(w, result)
	}

	return writeAuditEventsText(w, result)
}

func auditExport(w io.Writer, opts auditExportOptions) error {
	if opts.Limit <= 0 {
		opts.Limit = 1000
	}

	result, pageCount, err := fetchAllAuditEvents(opts.auditListOptions)
	if err != nil {
		return err
	}

	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	evidence, err := buildAuditExportEvidence(result, opts.auditListOptions, generatedAt, pageCount)
	if err != nil {
		return err
	}

	outputPath := strings.TrimSpace(opts.OutputPath)
	if outputPath == "" || outputPath == "-" {
		return writeJSON(w, evidence)
	}

	payload, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return fmt.Errorf("encode audit export: %w", err)
	}

	payload = append(payload, '\n')
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		return fmt.Errorf("write audit export: %w", err)
	}

	if outputIsJSON() {
		return writeJSON(w, map[string]any{
			"status":           "exported",
			"path":             outputPath,
			"row_count":        evidence.RowCount,
			"may_be_truncated": evidence.MayBeTruncated,
			"events_sha256":    evidence.EventsSHA256,
		})
	}

	fmt.Fprintf(w, "audit_export_path=%s\n", outputPath)
	fmt.Fprintf(w, "audit_export_rows=%d\n", evidence.RowCount)
	fmt.Fprintf(w, "audit_export_may_be_truncated=%t\n", evidence.MayBeTruncated)
	fmt.Fprintf(w, "audit_export_events_sha256=%s\n", evidence.EventsSHA256)
	return nil
}

func fetchAllAuditEvents(opts auditListOptions) (auditEventListResult, int, error) {
	if strings.TrimSpace(opts.Cursor) != "" {
		return auditEventListResult{}, 0, fmt.Errorf("audit export manages cursors internally; omit --cursor")
	}

	var combined auditEventListResult
	pageCount := 0
	seenCursors := map[string]bool{}
	for {
		page, err := fetchAuditEvents(opts)
		if err != nil {
			return auditEventListResult{}, 0, err
		}

		pageCount++
		if combined.Limit == 0 {
			combined.Limit = page.Limit
		}

		combined.Events = append(combined.Events, page.Events...)
		nextCursor := strings.TrimSpace(page.NextCursor)
		if nextCursor == "" {
			return combined, pageCount, nil
		}

		if seenCursors[nextCursor] {
			return auditEventListResult{}, pageCount, fmt.Errorf("audit export pagination cursor repeated")
		}

		seenCursors[nextCursor] = true
		opts.Cursor = nextCursor
	}
}

func buildAuditExportEvidence(result auditEventListResult, opts auditListOptions, generatedAt time.Time, pageCount int) (auditExportEvidence, error) {
	eventsSHA256, err := auditEventsSHA256(result.Events)
	if err != nil {
		return auditExportEvidence{}, err
	}

	filters := auditExportFilters{
		EventType:     strings.TrimSpace(opts.EventType),
		CorrelationID: strings.TrimSpace(opts.CorrelationID),
		Since:         strings.TrimSpace(opts.Since),
		Until:         strings.TrimSpace(opts.Until),
	}

	if opts.ActorID > 0 {
		filters.ActorID = &opts.ActorID
	}

	if opts.TargetID > 0 {
		filters.TargetID = &opts.TargetID
	}

	evidence := auditExportEvidence{
		SchemaVersion:  auditExportSchemaVersion,
		GeneratedAt:    generatedAt.UTC().Format(time.RFC3339),
		Filters:        filters,
		Limit:          result.Limit,
		PageCount:      pageCount,
		RowCount:       len(result.Events),
		MayBeTruncated: strings.TrimSpace(result.NextCursor) != "",
		EventsSHA256:   eventsSHA256,
		Events:         result.Events,
	}

	for _, event := range result.Events {
		createdAt := strings.TrimSpace(event.CreatedAt)
		if createdAt == "" {
			continue
		}

		if evidence.NewestEventAt == "" || createdAt > evidence.NewestEventAt {
			evidence.NewestEventAt = createdAt
		}

		if evidence.OldestEventAt == "" || createdAt < evidence.OldestEventAt {
			evidence.OldestEventAt = createdAt
		}
	}

	return evidence, nil
}

func auditEventsSHA256(events []auditEventResult) (string, error) {
	payload, err := json.Marshal(events)
	if err != nil {
		return "", fmt.Errorf("hash audit events: %w", err)
	}

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
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

	if err := tw.Flush(); err != nil {
		return err
	}

	if strings.TrimSpace(result.NextCursor) != "" {
		fmt.Fprintf(w, "\nMore audit events available. Continue with --cursor %s.\n", result.NextCursor)
	}

	return nil
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
