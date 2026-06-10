package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"vectis/internal/action/actionconfig"
	"vectis/internal/action/actionregistry"
)

var actionResolveIgnorePolicy bool
var actionListIgnorePolicy bool

type actionResolveResult struct {
	Reference         string                    `json:"reference"`
	ResolvedReference string                    `json:"resolved_reference"`
	Descriptor        actionregistry.Descriptor `json:"descriptor"`
}

type actionListResult struct {
	Actions []actionregistry.Descriptor `json:"actions"`
}

func runResolveAction(cmd *cobra.Command, args []string) {
	runCLIError(resolveAction(os.Stdout, args[0], actionResolveIgnorePolicy))
}

func runListActions(cmd *cobra.Command, args []string) {
	runCLIError(listActions(os.Stdout, actionListIgnorePolicy))
}

func resolveAction(w io.Writer, uses string, ignorePolicy bool) error {
	resolver, err := actionResolverForCLI(ignorePolicy)
	if err != nil {
		return err
	}

	descriptor, err := resolver.ResolveDescriptor(uses)
	if err != nil {
		return err
	}

	result := actionResolveResult{
		Reference:         strings.TrimSpace(uses),
		ResolvedReference: descriptor.ResolvedReference(),
		Descriptor:        descriptor,
	}

	if outputIsJSON() {
		return writeJSON(w, result)
	}

	return writeActionResolveText(w, result)
}

func listActions(w io.Writer, ignorePolicy bool) error {
	resolver, err := actionResolverForCLI(ignorePolicy)
	if err != nil {
		return err
	}

	lister, ok := resolver.(actionregistry.DescriptorLister)
	if !ok {
		return fmt.Errorf("configured action resolver does not support listing")
	}

	descriptors, err := lister.ListDescriptors()
	if err != nil {
		return err
	}

	result := actionListResult{Actions: descriptors}
	if outputIsJSON() {
		return writeJSON(w, result)
	}

	return writeActionListText(w, result)
}

func actionResolverForCLI(ignorePolicy bool) (actionregistry.Resolver, error) {
	if ignorePolicy {
		return actionconfig.UnrestrictedDescriptorResolver()
	}

	return actionconfig.DescriptorResolver()
}

func writeActionResolveText(w io.Writer, result actionResolveResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Reference:\t%s\n", result.Reference)
	fmt.Fprintf(tw, "Resolved:\t%s\n", result.ResolvedReference)
	fmt.Fprintf(tw, "Name:\t%s\n", result.Descriptor.CanonicalName)
	if result.Descriptor.DisplayName != "" {
		fmt.Fprintf(tw, "Display name:\t%s\n", result.Descriptor.DisplayName)
	}

	fmt.Fprintf(tw, "Version:\t%s\n", result.Descriptor.Version)
	fmt.Fprintf(tw, "Digest:\t%s\n", result.Descriptor.Digest)
	fmt.Fprintf(tw, "Source:\t%s\n", result.Descriptor.Source)
	fmt.Fprintf(tw, "Runtime:\t%s\n", result.Descriptor.Runtime)
	fmt.Fprintf(tw, "Status:\t%s\n", result.Descriptor.LifecycleStatus())
	if result.Descriptor.StatusReason != "" {
		fmt.Fprintf(tw, "Status reason:\t%s\n", result.Descriptor.StatusReason)
	}

	if len(result.Descriptor.Capabilities) > 0 {
		fmt.Fprintf(tw, "Capabilities:\t%s\n", joinCapabilities(result.Descriptor.Capabilities))
	}

	return tw.Flush()
}

func writeActionListText(w io.Writer, result actionListResult) error {
	if len(result.Actions) == 0 {
		_, err := fmt.Fprintln(w, "No actions found")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAME\tVERSION\tSOURCE\tRUNTIME\tSTATUS\tDIGEST\tDISPLAY NAME\tSTATUS REASON")
	for _, descriptor := range result.Actions {
		displayName := descriptor.DisplayName
		if displayName == "" {
			displayName = "-"
		}

		statusReason := descriptor.StatusReason
		if statusReason == "" {
			statusReason = "-"
		}

		fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			descriptor.CanonicalName,
			descriptor.Version,
			descriptor.Source,
			descriptor.Runtime,
			descriptor.LifecycleStatus(),
			descriptor.Digest,
			displayName,
			statusReason,
		)
	}

	return tw.Flush()
}

func joinCapabilities(capabilities []actionregistry.Capability) string {
	out := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		if capability == "" {
			continue
		}

		out = append(out, string(capability))
	}

	return strings.Join(out, ", ")
}

var actionsCmd = &cobra.Command{
	Use:     "actions",
	Short:   "Resolve and inspect action descriptors",
	Long:    `Resolve action references against the configured action registry and inspect the descriptor digest used for pinning.`,
	GroupID: cliGroupWorkflows,
	Run:     showCommandHelp,
}

var actionsListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available action descriptors",
	Long: `List action descriptors available from the configured action registry.

By default this uses the configured action registry policy. Use --ignore-policy to include descriptors that namespace or source policy would hide.`,
	Args: cobra.NoArgs,
	Run:  runListActions,
}

var actionsResolveCmd = &cobra.Command{
	Use:   "resolve [uses]",
	Short: "Resolve an action reference",
	Long: `Resolve an action reference such as examples/greet@v1 or builtins/shell.

By default this uses the configured action registry policy. Use --ignore-policy to discover the digest for a version selector when preparing a digest-pinned reference.`,
	Args: cobra.ExactArgs(1),
	Run:  runResolveAction,
}

func configureActionListFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&actionListIgnorePolicy, "ignore-policy", false, "List without action registry policy filters")
}

func configureActionResolveFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&actionResolveIgnorePolicy, "ignore-policy", false, "Resolve without action registry policy checks")
}
