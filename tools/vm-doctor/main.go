package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"

	"vectis/internal/platform"
)

const (
	defaultMode              = "status"
	defaultTimeout           = 10 * time.Minute
	defaultPrepVersion       = "2"
	defaultDeployInstance    = "vectis-deploy-smoke"
	defaultBuilderInstance   = "vectis-package-builder"
	defaultDebSmokeInstance  = "vectis-package-smoke"
	defaultRPMSmokeInstance  = "vectis-package-rpm-smoke"
	defaultBuilderGoVersion  = "1.25.10"
	defaultBuilderCacheRoot  = "/var/tmp/vectis-package-local-cache"
	defaultBuilderWorkspaces = "/var/tmp/vectis-package-local-workspaces"
	prepRoot                 = "/etc/vectis-vm-prep"
)

var errDoctorFailed = errors.New("vm doctor found issues")

type options struct {
	mode                 string
	provider             string
	timeout              time.Duration
	packerPath           string
	prepVersion          string
	lane                 string
	deployLimaPath       string
	deployInstance       string
	builderLimaPath      string
	builderInstance      string
	builderGoVersion     string
	builderCacheRoot     string
	builderWorkspaceRoot string
	smokeLimaPath        string
	debSmokeInstance     string
	rpmSmokeInstance     string
}

type lane struct {
	name          string
	instance      string
	provider      string
	providerPath  string
	prepareTarget string
	checkTarget   string
	expectations  []guestExpectation
	checks        []guestCheck
}

type guestExpectation struct {
	label string
	path  string
	want  string
}

type guestCheck struct {
	label string
	args  []string
}

type laneReport struct {
	lane            lane
	exists          bool
	status          string
	issues          []string
	startedByRun    bool
	stoppedAfterRun bool
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, errDoctorFailed) {
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "vm-doctor: %v\n", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	switch opts.mode {
	case "status":
		return runStatus(ctx, opts, stdout)
	case "doctor":
		return runDoctor(ctx, opts, stdout)
	default:
		return fmt.Errorf("unknown mode %q", opts.mode)
	}
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	opts := options{
		mode:                 defaultMode,
		provider:             platform.VirtualMachineProviderAuto,
		timeout:              defaultTimeout,
		packerPath:           "packer",
		prepVersion:          defaultPrepVersion,
		deployLimaPath:       "limactl",
		deployInstance:       defaultDeployInstance,
		builderLimaPath:      "limactl",
		builderInstance:      defaultBuilderInstance,
		builderGoVersion:     defaultBuilderGoVersion,
		builderCacheRoot:     defaultBuilderCacheRoot,
		builderWorkspaceRoot: defaultBuilderWorkspaces,
		smokeLimaPath:        "limactl",
		debSmokeInstance:     defaultDebSmokeInstance,
		rpmSmokeInstance:     defaultRPMSmokeInstance,
	}

	timeoutRaw := opts.timeout.String()
	flags := flag.NewFlagSet("vm-doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.mode, "mode", opts.mode, "status or doctor")
	flags.StringVar(&opts.provider, "provider", opts.provider, "VM provider")
	flags.StringVar(&timeoutRaw, "timeout", timeoutRaw, "overall timeout")
	flags.StringVar(&opts.packerPath, "packer", opts.packerPath, "packer executable")
	flags.StringVar(&opts.prepVersion, "prep-version", opts.prepVersion, "expected prepared VM marker version")
	flags.StringVar(&opts.lane, "lane", opts.lane, "VM lane to inspect, or all")
	flags.StringVar(&opts.deployLimaPath, "deploy-lima-bin", opts.deployLimaPath, "limactl executable for deploy smoke VM")
	flags.StringVar(&opts.deployInstance, "deploy-instance", opts.deployInstance, "deploy smoke VM instance")
	flags.StringVar(&opts.builderLimaPath, "builder-lima-bin", opts.builderLimaPath, "limactl executable for package builder VM")
	flags.StringVar(&opts.builderInstance, "builder-instance", opts.builderInstance, "package builder VM instance")
	flags.StringVar(&opts.builderGoVersion, "builder-go-version", opts.builderGoVersion, "expected package builder Go version")
	flags.StringVar(&opts.builderCacheRoot, "builder-cache-root", opts.builderCacheRoot, "expected package builder cache root")
	flags.StringVar(&opts.builderWorkspaceRoot, "builder-workspace-root", opts.builderWorkspaceRoot, "expected package builder workspace root")
	flags.StringVar(&opts.smokeLimaPath, "smoke-lima-bin", opts.smokeLimaPath, "limactl executable for package smoke VMs")
	flags.StringVar(&opts.debSmokeInstance, "deb-smoke-instance", opts.debSmokeInstance, "DEB package smoke VM instance")
	flags.StringVar(&opts.rpmSmokeInstance, "rpm-smoke-instance", opts.rpmSmokeInstance, "RPM package smoke VM instance")

	if err := flags.Parse(args); err != nil {
		return options{}, err
	}

	timeout, err := time.ParseDuration(timeoutRaw)
	if err != nil {
		return options{}, fmt.Errorf("parse timeout %q: %w", timeoutRaw, err)
	}

	opts.timeout = timeout
	opts.mode = strings.ToLower(strings.TrimSpace(opts.mode))
	opts.provider = platform.ResolveVirtualMachineProvider(opts.provider)
	opts.lane = strings.ToLower(strings.TrimSpace(opts.lane))
	if opts.lane == "" {
		opts.lane = "all"
	}

	opts.prepVersion = strings.TrimSpace(opts.prepVersion)
	if opts.prepVersion == "" {
		opts.prepVersion = defaultPrepVersion
	}

	return opts, nil
}

func runStatus(ctx context.Context, opts options, stdout io.Writer) error {
	selected, err := selectedLanes(opts)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "VM status (provider=%s prep-version=%s)\n\n", opts.provider, opts.prepVersion)
	printHostTools(stdout, opts, selected)
	fmt.Fprintln(stdout)

	for _, lane := range selected {
		report := inspectLaneStatus(ctx, lane)
		printLaneStatus(stdout, report)
	}

	return nil
}

func runDoctor(ctx context.Context, opts options, stdout io.Writer) error {
	selected, err := selectedLanes(opts)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "VM doctor (provider=%s prep-version=%s)\n\n", opts.provider, opts.prepVersion)
	hostIssues := printHostTools(stdout, opts, selected)
	fmt.Fprintln(stdout)

	var issueCount int
	issueCount += hostIssues
	for _, lane := range selected {
		report := inspectLaneDoctor(ctx, lane)
		issueCount += len(report.issues)
		printLaneDoctor(stdout, report)
	}

	if issueCount > 0 {
		fmt.Fprintf(stdout, "\nFound %d issue(s). Repair with the target shown for each lane, then rerun mage vmDoctor.\n", issueCount)
		return errDoctorFailed
	}

	fmt.Fprintln(stdout, "\nAll prepared VM lanes look healthy.")
	return nil
}

func selectedLanes(opts options) ([]lane, error) {
	all := lanes(opts)
	if opts.lane == "" || opts.lane == "all" {
		return all, nil
	}

	for _, candidate := range all {
		if candidate.name == opts.lane {
			return []lane{candidate}, nil
		}
	}

	names := make([]string, 0, len(all))
	for _, lane := range all {
		names = append(names, lane.name)
	}

	sort.Strings(names)
	return nil, fmt.Errorf("unknown VM lane %q; expected one of: all, %s", opts.lane, strings.Join(names, ", "))
}

func lanes(opts options) []lane {
	return []lane{
		{
			name:          "deploy-smoke",
			instance:      opts.deployInstance,
			provider:      opts.provider,
			providerPath:  opts.deployLimaPath,
			prepareTarget: "mage vmDeploySmokePrepare",
			checkTarget:   "mage vmDeploySmokeCheck",
			expectations: []guestExpectation{
				{label: "profile", path: prepRoot + "/deploy-smoke-profile", want: "systemd"},
				{label: "prep-version", path: prepRoot + "/deploy-smoke-prep-version", want: opts.prepVersion},
			},
			checks: []guestCheck{
				{label: "systemctl", args: []string{"systemctl", "--version"}},
				{label: "systemd-analyze", args: []string{"systemd-analyze", "--version"}},
				{label: "systemd-sysusers", args: []string{"systemd-sysusers", "--version"}},
				{label: "systemd-tmpfiles", args: []string{"systemd-tmpfiles", "--version"}},
			},
		},
		{
			name:          "package-builder",
			instance:      opts.builderInstance,
			provider:      opts.provider,
			providerPath:  opts.builderLimaPath,
			prepareTarget: "mage vmPackageBuilderPrepare",
			checkTarget:   "mage vmPackageBuilderCheck",
			expectations: []guestExpectation{
				{label: "prep-version", path: prepRoot + "/package-builder-prep-version", want: opts.prepVersion},
			},
			checks: []guestCheck{
				{label: "go", args: []string{"sh", "-lc", `PATH=/usr/local/go/bin:$PATH; test "$(go env GOVERSION)" = "$1"`, "vectis-builder-go", "go" + opts.builderGoVersion}},
				{label: "mage", args: []string{"mage", "--version"}},
				{label: "cc", args: []string{"cc", "--version"}},
				{label: "cache-root", args: []string{"test", "-d", opts.builderCacheRoot}},
				{label: "workspace-root", args: []string{"test", "-d", opts.builderWorkspaceRoot}},
			},
		},
		packageSmokeLane(opts, "package-smoke-deb", opts.debSmokeInstance, "deb", "mage vmPackageSmokeDebPrepare", "mage vmPackageSmokeDebCheck", []guestCheck{
			{label: "dpkg", args: []string{"dpkg", "--version"}},
			{label: "dpkg-deb", args: []string{"dpkg-deb", "--version"}},
			{label: "systemctl", args: []string{"systemctl", "--version"}},
			{label: "systemd-sysusers", args: []string{"systemd-sysusers", "--version"}},
			{label: "systemd-tmpfiles", args: []string{"systemd-tmpfiles", "--version"}},
		}),
		packageSmokeLane(opts, "package-smoke-rpm", opts.rpmSmokeInstance, "rpm", "mage vmPackageSmokeRPMPrepare", "mage vmPackageSmokeRPMCheck", []guestCheck{
			{label: "rpm", args: []string{"rpm", "--version"}},
			{label: "systemctl", args: []string{"systemctl", "--version"}},
			{label: "systemd-sysusers", args: []string{"systemd-sysusers", "--version"}},
			{label: "systemd-tmpfiles", args: []string{"systemd-tmpfiles", "--version"}},
		}),
	}
}

func packageSmokeLane(opts options, name, instance, profile, prepareTarget, checkTarget string, checks []guestCheck) lane {
	return lane{
		name:          name,
		instance:      instance,
		provider:      opts.provider,
		providerPath:  opts.smokeLimaPath,
		prepareTarget: prepareTarget,
		checkTarget:   checkTarget,
		expectations: []guestExpectation{
			{label: "profile", path: prepRoot + "/package-smoke-profile", want: profile},
			{label: "prep-version", path: prepRoot + "/package-smoke-prep-version", want: opts.prepVersion},
		},
		checks: checks,
	}
}

func printHostTools(stdout io.Writer, opts options, selected []lane) int {
	var issues int
	tools := []struct {
		name     string
		path     string
		required bool
	}{
		{name: "packer", path: opts.packerPath},
	}
	for _, lane := range selected {
		tools = append(tools, struct {
			name     string
			path     string
			required bool
		}{
			name:     lane.name + " provider",
			path:     lane.providerPath,
			required: true,
		})
	}

	fmt.Fprintln(stdout, "Host tools:")
	seen := map[string]struct{}{}
	for _, tool := range tools {
		key := tool.path
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		if _, err := exec.LookPath(tool.path); err != nil {
			if tool.required {
				issues++
			}

			fmt.Fprintf(stdout, "  [missing] %-16s %s\n", tool.name, tool.path)
			continue
		}

		fmt.Fprintf(stdout, "  [ok]      %-16s %s\n", tool.name, tool.path)
	}

	return issues
}

func inspectLaneStatus(ctx context.Context, l lane) laneReport {
	report := laneReport{lane: l, status: "unknown"}
	manager, err := newManager(l, io.Discard, io.Discard)
	if err != nil {
		report.issues = append(report.issues, err.Error())
		return report
	}

	if err := manager.CheckAvailable(); err != nil {
		report.issues = append(report.issues, err.Error())
		return report
	}

	exists, err := manager.InstanceExists(ctx, l.instance)
	if err != nil {
		report.issues = append(report.issues, err.Error())
		return report
	}

	report.exists = exists
	if !exists {
		report.status = "missing"
		report.issues = append(report.issues, "instance is missing")
		return report
	}

	status, err := manager.InstanceStatus(ctx, l.instance)
	if err != nil {
		report.issues = append(report.issues, err.Error())
	} else {
		report.status = status
	}

	return report
}

func inspectLaneDoctor(ctx context.Context, l lane) laneReport {
	report := inspectLaneStatus(ctx, l)
	if !report.exists || len(report.issues) > 0 {
		return report
	}

	wasStopped := strings.EqualFold(report.status, "stopped")
	manager, err := newManager(l, io.Discard, io.Discard)
	if err != nil {
		report.issues = append(report.issues, err.Error())
		return report
	}

	if err := manager.Start(ctx, l.instance); err != nil {
		report.issues = append(report.issues, fmt.Sprintf("start instance: %v", err))
		return report
	}

	report.startedByRun = wasStopped
	for _, expectation := range l.expectations {
		got, err := guestOutput(ctx, l, "cat", expectation.path)
		if err != nil {
			report.issues = append(report.issues, fmt.Sprintf("%s missing or unreadable: %v", expectation.label, err))
			continue
		}

		got = strings.TrimSpace(got)
		if got != expectation.want {
			report.issues = append(report.issues, fmt.Sprintf("%s = %q, want %q", expectation.label, got, expectation.want))
		}
	}

	for _, check := range l.checks {
		if err := guestCheckCommand(ctx, l, check.args...); err != nil {
			report.issues = append(report.issues, fmt.Sprintf("%s check failed: %v", check.label, err))
		}
	}

	status, err := manager.InstanceStatus(ctx, l.instance)
	if err == nil {
		report.status = status
	}

	if wasStopped {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultTimeout)
		defer cancel()

		if err := manager.Stop(stopCtx, l.instance); err != nil {
			report.issues = append(report.issues, fmt.Sprintf("stop instance after doctor: %v", err))
			return report
		}

		report.status = "Stopped"
		report.stoppedAfterRun = true
	}

	return report
}

func printLaneStatus(stdout io.Writer, report laneReport) {
	state := "ok"
	if len(report.issues) > 0 {
		state = "issue"
	}

	fmt.Fprintf(stdout, "[%s] %s instance=%s status=%s\n", state, report.lane.name, report.lane.instance, report.status)
	if len(report.issues) > 0 {
		for _, issue := range report.issues {
			fmt.Fprintf(stdout, "  - %s\n", issue)
		}

		fmt.Fprintf(stdout, "  repair: %s\n", report.lane.prepareTarget)
	}
}

func printLaneDoctor(stdout io.Writer, report laneReport) {
	printLaneStatus(stdout, report)
	if report.startedByRun {
		if report.stoppedAfterRun {
			fmt.Fprintln(stdout, "  note: started for inspection and stopped afterward")
		} else {
			fmt.Fprintln(stdout, "  note: started for inspection; stop did not complete")
		}
	}

	if len(report.issues) == 0 {
		fmt.Fprintf(stdout, "  check: %s\n", report.lane.checkTarget)
	}
}

func guestOutput(ctx context.Context, l lane, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	manager, err := newManager(l, &stdout, &stderr)
	if err != nil {
		return "", err
	}

	if err := manager.Shell(ctx, l.instance, nil, args...); err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}

	return stdout.String(), nil
}

func guestCheckCommand(ctx context.Context, l lane, args ...string) error {
	var stderr bytes.Buffer
	manager, err := newManager(l, io.Discard, &stderr)
	if err != nil {
		return err
	}

	if err := manager.Shell(ctx, l.instance, nil, args...); err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
	}

	return nil
}

func newManager(l lane, stdout, stderr io.Writer) (platform.VirtualMachineManager, error) {
	return platform.NewVirtualMachineManager(platform.VirtualMachineManagerConfig{
		Provider:     l.provider,
		ProviderPath: l.providerPath,
		Stdout:       stdout,
		Stderr:       stderr,
	})
}
