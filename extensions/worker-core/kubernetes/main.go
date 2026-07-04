package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	api "vectis/api/gen/go"
	sdk "vectis/sdk/workercore"

	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	defaultSocketPath   = "/tmp/vectis-worker-core-kubernetes.sock"
	defaultNamespace    = "default"
	defaultTaskImage    = "busybox:1.36"
	defaultKubectl      = "kubectl"
	defaultWaitTimeout  = 30 * time.Minute
	defaultPollInterval = time.Second
	logDrainTimeout     = 2 * time.Second
	logReadTimeout      = 5 * time.Second
	logReadPollInterval = 250 * time.Millisecond

	reasonUnsupportedTask = "kubernetes.unsupported_task"
	reasonJobFailed       = "kubernetes.job_failed"
	reasonJobUnknown      = "kubernetes.job_unknown"
)

var scpURLRe = regexp.MustCompile(`^[\w.-]+@[\w.-]+:[\w./~-]+$`)

func main() {
	socketPath := flag.String("socket", envDefault("VECTIS_WORKER_CORE_SOCKET", defaultSocketPath), "Unix socket served by the Kubernetes worker core")
	namespace := flag.String("namespace", envDefault("KUBERNETES_NAMESPACE", defaultNamespace), "Kubernetes namespace for task Jobs")
	image := flag.String("image", envDefault("VECTIS_KUBERNETES_WORKER_CORE_IMAGE", defaultTaskImage), "Container image used for script task Jobs")
	kubectl := flag.String("kubectl", envDefault("KUBECTL", defaultKubectl), "kubectl executable")
	waitTimeout := flag.Duration("wait-timeout", envDurationDefault("VECTIS_KUBERNETES_WORKER_CORE_WAIT_TIMEOUT", defaultWaitTimeout), "Maximum time to wait for a Kubernetes Job to complete")
	pollInterval := flag.Duration("poll-interval", envDurationDefault("VECTIS_KUBERNETES_WORKER_CORE_POLL_INTERVAL", defaultPollInterval), "Kubernetes Job status poll interval")
	deleteAfter := flag.Bool("delete-after", envBoolDefault("VECTIS_KUBERNETES_WORKER_CORE_DELETE_AFTER", false), "Delete task Jobs after terminal completion")
	flag.Parse()

	core := newKubernetesCore(coreConfig{
		Namespace:    *namespace,
		Image:        *image,
		Kubectl:      *kubectl,
		WaitTimeout:  *waitTimeout,
		PollInterval: *pollInterval,
		DeleteAfter:  *deleteAfter,
	}, newKubectlRunner(*kubectl))

	server, listener, err := sdk.NewUnixCoreServer(*socketPath, core, sdk.ServiceOptions{})
	if err != nil {
		log.Fatalf("create Kubernetes worker core server: %v", err)
	}
	defer os.Remove(*socketPath)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Printf("Kubernetes worker core listening on %s", *socketPath)
	if err := server.Serve(listener); err != nil && ctx.Err() == nil {
		log.Fatalf("serve Kubernetes worker core: %v", err)
	}
}

type coreConfig struct {
	Namespace    string
	Image        string
	Kubectl      string
	WaitTimeout  time.Duration
	PollInterval time.Duration
	DeleteAfter  bool
}

type kubernetesCore struct {
	cfg    coreConfig
	runner kubeRunner

	mu        sync.Mutex
	cancelled map[string]string
}

func newKubernetesCore(cfg coreConfig, runner kubeRunner) *kubernetesCore {
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	if cfg.Namespace == "" {
		cfg.Namespace = defaultNamespace
	}

	cfg.Image = strings.TrimSpace(cfg.Image)
	if cfg.Image == "" {
		cfg.Image = defaultTaskImage
	}

	cfg.Kubectl = strings.TrimSpace(cfg.Kubectl)
	if cfg.Kubectl == "" {
		cfg.Kubectl = defaultKubectl
	}

	if cfg.WaitTimeout <= 0 {
		cfg.WaitTimeout = defaultWaitTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if runner == nil {
		runner = newKubectlRunner(cfg.Kubectl)
	}

	return &kubernetesCore{
		cfg:       cfg,
		runner:    runner,
		cancelled: map[string]string{},
	}
}

func (c *kubernetesCore) Describe(context.Context) (sdk.Description, error) {
	return sdk.Description{
		ProtocolVersion:    sdk.ProtocolVersion,
		SupportedIsolation: []string{"host"},
		Capabilities: []sdk.Capability{
			{Name: sdk.CapabilityExecute, Version: "v1"},
			{Name: sdk.CapabilityCancelTask, Version: "v1"},
			{Name: sdk.CapabilityShellLogCallback, Version: "v1"},
			{Name: sdk.CapabilityShellArtifactPush, Version: "v1"},
			{Name: "example.kubernetes-job", Version: "v1"},
			{Name: "example.custom-process-action", Version: "v1"},
		},
		Metadata: map[string]string{
			"provider":  "kubernetes",
			"mode":      "kubectl",
			"namespace": c.cfg.Namespace,
			"image":     c.cfg.Image,
		},
	}, nil
}

func (c *kubernetesCore) ExecuteTask(ctx context.Context, task sdk.Task) (sdk.Result, error) {
	spec, err := executableTaskSpec(task)
	if err != nil {
		return sdk.FailureWithReason(reasonUnsupportedTask, err.Error()), nil
	}

	jobName := jobNameFor(task.Job.GetRunId(), task.TaskKey, task.Session.ID())
	manifest, err := buildJobManifest(jobSpecInput{
		Name:      jobName,
		Namespace: c.cfg.Namespace,
		Image:     c.cfg.Image,
		Command:   spec.Command,
		ExtraEnv:  spec.Env,
		JobID:     task.Job.GetId(),
		RunID:     task.Job.GetRunId(),
		TaskKey:   task.TaskKey,
		SessionID: task.Session.ID(),
	})

	if err != nil {
		return sdk.Result{}, err
	}

	c.clearCancel(task.Session.ID())
	if err := sendLog(ctx, task, fmt.Sprintf("kubernetes worker core applying job %s/%s\n", c.cfg.Namespace, jobName)); err != nil {
		return sdk.Result{}, err
	}

	if err := c.runner.ApplyJob(ctx, c.cfg.Namespace, manifest); err != nil {
		if ctx.Err() != nil {
			return sdk.Cancelled(ctx.Err().Error()), nil
		}

		return sdk.ExternalUnavailable(fmt.Sprintf("apply Kubernetes Job %s/%s: %v", c.cfg.Namespace, jobName, err)), nil
	}

	logCtx, stopLogs := context.WithCancel(ctx)
	logDone := make(chan error, 1)
	deliveredLogs := &deliveredLogBuffer{}
	go func() {
		logDone <- c.runner.StreamLogs(logCtx, c.cfg.Namespace, jobName, func(line []byte) error {
			return deliveredLogs.send(ctx, task, line)
		})
	}()

	outcome, waitErr := c.runner.WaitJob(ctx, c.cfg.Namespace, jobName, c.cfg.WaitTimeout, c.cfg.PollInterval)
	logDrained := false
	select {
	case <-logDone:
		logDrained = true
	case <-time.After(logDrainTimeout):
	}

	stopLogs()
	if !logDrained {
		select {
		case <-logDone:
		case <-time.After(500 * time.Millisecond):
		}
	}

	if waitErr == nil && outcome.Phase != jobRunning && task.Session.LogsEnabled() {
		if err := c.drainTerminalLogs(ctx, task, jobName, deliveredLogs); err != nil {
			return sdk.ExternalUnavailable(fmt.Sprintf("read Kubernetes Job logs for %s/%s: %v", c.cfg.Namespace, jobName, err)), nil
		}
	}

	if c.cfg.DeleteAfter && outcome.Phase != jobRunning {
		_ = c.runner.DeleteJob(context.Background(), c.cfg.Namespace, jobName)
	}

	if reason, ok := c.cancelReason(task.Session.ID()); ok {
		_ = c.runner.DeleteJob(context.Background(), c.cfg.Namespace, jobName)
		return sdk.Cancelled("cancelled: " + reason), nil
	}

	if waitErr != nil {
		if ctx.Err() != nil {
			_ = c.runner.DeleteJob(context.Background(), c.cfg.Namespace, jobName)
			return sdk.Cancelled(ctx.Err().Error()), nil
		}

		return sdk.ExternalUnavailable(fmt.Sprintf("watch Kubernetes Job %s/%s: %v", c.cfg.Namespace, jobName, waitErr)), nil
	}

	switch outcome.Phase {
	case jobSucceeded:
		if err := sendLog(ctx, task, fmt.Sprintf("kubernetes worker core job %s/%s succeeded\n", c.cfg.Namespace, jobName)); err != nil {
			return sdk.Result{}, err
		}

		return sdk.Success(), nil
	case jobFailed:
		return sdk.FailureWithReason(reasonJobFailed, outcome.MessageOrDefault("Kubernetes Job failed")), nil
	case jobCancelled:
		return sdk.Cancelled(outcome.MessageOrDefault("Kubernetes Job was cancelled")), nil
	default:
		return sdk.UnknownWithReason(reasonJobUnknown, outcome.MessageOrDefault("Kubernetes Job reached an unknown state")), nil
	}
}

func (c *kubernetesCore) CancelTask(ctx context.Context, req sdk.CancelRequest) error {
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		return fmt.Errorf("task session id is required")
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "cancel requested"
	}

	c.mu.Lock()
	c.cancelled[sessionID] = reason
	c.mu.Unlock()

	jobName := jobNameFor(req.RunID, req.TaskKey, sessionID)
	return c.runner.DeleteJob(ctx, c.cfg.Namespace, jobName)
}

func (c *kubernetesCore) cancelReason(sessionID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reason, ok := c.cancelled[sessionID]
	return reason, ok
}

func (c *kubernetesCore) clearCancel(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelled, sessionID)
}

type executableSpec struct {
	Command string
	Env     []envVar
}

func executableTaskSpec(task sdk.Task) (executableSpec, error) {
	node := findTaskNode(task.Job.GetRoot(), task.TaskKey)
	if node == nil {
		return executableSpec{}, fmt.Errorf("task node %q not found", task.TaskKey)
	}

	uses := strings.TrimSpace(node.GetUses())
	if uses == "builtins/script" {
		return scriptTaskSpec(task, node)
	}

	if strings.HasPrefix(uses, "builtins/") {
		return executableSpec{}, fmt.Errorf("Kubernetes worker core example supports builtins/script and frozen custom process actions only, got %q", uses)
	}

	return customProcessTaskSpec(task, node, uses)
}

func scriptTaskSpec(task sdk.Task, node *api.Node) (executableSpec, error) {
	if hasChildTasks(node) {
		return executableSpec{}, fmt.Errorf("builtins/script task %q must be a leaf task for Kubernetes execution", task.TaskKey)
	}

	command := strings.TrimSpace(node.GetWith()["script"])
	if command == "" {
		return executableSpec{}, fmt.Errorf("builtins/script task %q requires with.script", task.TaskKey)
	}

	return executableSpec{Command: command}, nil
}

func customProcessTaskSpec(task sdk.Task, node *api.Node, uses string) (executableSpec, error) {
	if hasChildTasks(node) {
		return executableSpec{}, fmt.Errorf("custom process action %q must be a leaf task for Kubernetes execution", uses)
	}

	if len(node.GetInputs()) > 0 {
		return executableSpec{}, fmt.Errorf("custom process action %q uses dataflow-bound inputs, which this Kubernetes example does not support", uses)
	}

	lock, err := actionLockForTask(task, node, uses)
	if err != nil {
		return executableSpec{}, err
	}

	descriptor := lock.GetDescriptor_()
	if descriptor == nil || strings.TrimSpace(descriptor.GetCanonicalName()) == "" {
		return executableSpec{}, fmt.Errorf("custom action %q has no frozen descriptor", uses)
	}

	if descriptor.GetLocalOnly() {
		return executableSpec{}, fmt.Errorf("custom action %q is local_only and cannot be moved to Kubernetes", uses)
	}

	if runtime := strings.TrimSpace(descriptor.GetRuntime()); runtime != "process" {
		return executableSpec{}, fmt.Errorf("custom action %q runtime %q is not supported by this Kubernetes example", uses, runtime)
	}

	if workingDirectory := strings.TrimSpace(descriptor.GetRuntimeConfig()["working_directory"]); workingDirectory != "" {
		return executableSpec{}, fmt.Errorf("custom process action %q configures working_directory %q, which this Kubernetes example does not mount", uses, workingDirectory)
	}

	command := processCommand(descriptor.GetRuntimeConfig())
	if command == "" {
		return executableSpec{}, fmt.Errorf("custom process action %q requires runtime_config.command", uses)
	}

	inputs := cloneStringMap(node.GetWith())
	if err := validateCustomInputs(uses, descriptor.GetInputSchema(), inputs); err != nil {
		return executableSpec{}, err
	}

	return executableSpec{
		Command: command,
		Env:     customProcessEnv(descriptor, inputs),
	}, nil
}

func hasChildTasks(node *api.Node) bool {
	return len(node.GetSteps()) > 0 || len(node.GetPorts()) > 0
}

func actionLockForTask(task sdk.Task, node *api.Node, uses string) (*api.WorkerCoreActionLock, error) {
	var matching []*api.WorkerCoreActionLock
	for _, lock := range task.Session.ActionLocks() {
		if lock == nil || strings.TrimSpace(lock.GetUses()) != uses {
			continue
		}

		matching = append(matching, lock)
	}

	if len(matching) == 0 {
		return nil, fmt.Errorf("custom action %q requires a frozen action descriptor in the worker-core session", uses)
	}

	nodeID := strings.TrimSpace(node.GetId())
	taskKey := strings.TrimSpace(task.TaskKey)
	for _, lock := range matching {
		lockNodeID := strings.TrimSpace(lock.GetNodeId())
		lockNodePath := strings.TrimSpace(lock.GetNodePath())
		if nodeID != "" && lockNodeID == nodeID {
			return lock, nil
		}

		if taskKey != "" && lockNodeID == taskKey {
			return lock, nil
		}

		if taskKey == "root" && lockNodePath == "root" {
			return lock, nil
		}
	}

	if len(matching) == 1 {
		return matching[0], nil
	}

	return nil, fmt.Errorf("custom action %q has multiple frozen descriptors and none match task %q", uses, task.TaskKey)
}

func processCommand(config map[string]string) string {
	if command := strings.TrimSpace(config["command"]); command != "" {
		return command
	}

	return strings.TrimSpace(config["entrypoint"])
}

func validateCustomInputs(uses string, schema *api.WorkerCoreInputSchema, inputs map[string]string) error {
	fields := schema.GetFields()
	if len(fields) == 0 {
		if schema.GetAllowUnknown() || len(inputs) == 0 {
			return nil
		}

		for _, key := range sortedMapKeys(inputs) {
			return fmt.Errorf("custom action %q input %q is not declared by the frozen descriptor", uses, key)
		}
	}

	known := make(map[string]*api.WorkerCoreInputField, len(fields))
	for _, field := range fields {
		if field == nil {
			continue
		}

		known[field.GetName()] = field
	}

	if !schema.GetAllowUnknown() {
		for _, key := range sortedMapKeys(inputs) {
			if _, ok := known[key]; !ok {
				return fmt.Errorf("custom action %q input %q is not declared by the frozen descriptor", uses, key)
			}
		}
	}

	for _, field := range fields {
		if field == nil {
			continue
		}

		value, exists := inputs[field.GetName()]
		if field.GetRequired() && (!exists || strings.TrimSpace(value) == "") {
			return fmt.Errorf("custom action %q input %q is required", uses, field.GetName())
		}

		if exists && strings.TrimSpace(value) != "" {
			if err := validateCustomInputType(field.GetType(), value); err != nil {
				return fmt.Errorf("custom action %q input %q %w", uses, field.GetName(), err)
			}
		}
	}

	return nil
}

func validateCustomInputType(fieldType, value string) error {
	switch strings.TrimSpace(fieldType) {
	case "", "string":
		return nil
	case "number":
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			return fmt.Errorf("must be a valid number")
		}
	case "url":
		parsed, err := url.Parse(value)
		if (err != nil || parsed.Scheme == "" || parsed.Host == "") && !scpURLRe.MatchString(value) {
			return fmt.Errorf("must be a valid URL")
		}
	default:
		return fmt.Errorf("uses unsupported input type %q", fieldType)
	}
	return nil
}

func customProcessEnv(descriptor *api.WorkerCoreActionDescriptor, inputs map[string]string) []envVar {
	env := []envVar{
		{Name: "VECTIS_ACTION_NAME", Value: descriptor.GetCanonicalName()},
		{Name: "VECTIS_ACTION_VERSION", Value: descriptor.GetVersion()},
		{Name: "VECTIS_ACTION_DIGEST", Value: descriptor.GetDigest()},
	}

	for _, key := range sortedMapKeys(inputs) {
		envKey := inputEnvKey(key)
		if envKey == "" {
			continue
		}

		env = append(env, envVar{Name: envKey, Value: inputs[key]})
	}

	return env
}

func inputEnvKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}

	var b strings.Builder
	b.WriteString("VECTIS_INPUT_")
	wrote := false
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r - 'a' + 'A')
			wrote = true
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
			wrote = true
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			wrote = true
		default:
			b.WriteByte('_')
		}
	}

	if !wrote {
		return ""
	}

	return b.String()
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}

	return out
}

func findTaskNode(root *api.Node, taskKey string) *api.Node {
	taskKey = strings.TrimSpace(taskKey)
	if root == nil {
		return nil
	}

	if taskKey == "" || taskKey == "root" || strings.TrimSpace(root.GetId()) == taskKey {
		return root
	}

	return findNodeByID(root, taskKey)
}

func findNodeByID(node *api.Node, id string) *api.Node {
	if node == nil {
		return nil
	}

	if strings.TrimSpace(node.GetId()) == id {
		return node
	}

	for _, child := range node.GetSteps() {
		if found := findNodeByID(child, id); found != nil {
			return found
		}
	}

	for _, port := range node.GetPorts() {
		for _, child := range port.GetNodes() {
			if found := findNodeByID(child, id); found != nil {
				return found
			}
		}
	}

	return nil
}

func sendLog(ctx context.Context, task sdk.Task, message string) error {
	if !task.Session.LogsEnabled() {
		return nil
	}

	if !strings.HasSuffix(message, "\n") {
		message += "\n"
	}

	return task.Session.SendLog(ctx, &api.LogChunk{
		RunId: proto.String(task.Job.GetRunId()),
		Data:  []byte(message),
	})
}

type deliveredLogBuffer struct {
	mu   sync.Mutex
	data []byte
}

func (b *deliveredLogBuffer) send(ctx context.Context, task sdk.Task, chunk []byte) error {
	if err := sendLog(ctx, task, string(chunk)); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, chunk...)
	return nil
}

func (b *deliveredLogBuffer) remaining(logs []byte) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if bytes.HasPrefix(logs, b.data) {
		return logs[len(b.data):]
	}

	return logs
}

func (c *kubernetesCore) drainTerminalLogs(ctx context.Context, task sdk.Task, jobName string, delivered *deliveredLogBuffer) error {
	deadline := time.NewTimer(logReadTimeout)
	defer deadline.Stop()

	ticker := time.NewTicker(logReadPollInterval)
	defer ticker.Stop()

	var lastErr error
	for {
		logs, err := c.runner.ReadLogs(ctx, c.cfg.Namespace, jobName)
		if err == nil {
			lastErr = nil
			remaining := delivered.remaining(logs)
			if len(remaining) > 0 {
				return delivered.send(ctx, task, remaining)
			}

			if len(logs) > 0 {
				return nil
			}
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastErr
			}

			return nil
		case <-deadline.C:
			if lastErr != nil {
				return lastErr
			}

			return nil
		case <-ticker.C:
		}
	}
}

type jobPhase string

const (
	jobRunning   jobPhase = "running"
	jobSucceeded jobPhase = "succeeded"
	jobFailed    jobPhase = "failed"
	jobCancelled jobPhase = "cancelled"
	jobUnknown   jobPhase = "unknown"
)

type jobOutcome struct {
	Phase    jobPhase
	Reason   string
	Message  string
	ExitCode int
}

func (o jobOutcome) MessageOrDefault(fallback string) string {
	parts := make([]string, 0, 3)
	if strings.TrimSpace(o.Message) != "" {
		parts = append(parts, strings.TrimSpace(o.Message))
	}

	if strings.TrimSpace(o.Reason) != "" {
		parts = append(parts, "reason="+strings.TrimSpace(o.Reason))
	}

	if o.ExitCode != 0 {
		parts = append(parts, fmt.Sprintf("exit_code=%d", o.ExitCode))
	}

	if len(parts) == 0 {
		return fallback
	}

	return strings.Join(parts, " ")
}

type kubeRunner interface {
	ApplyJob(context.Context, string, []byte) error
	StreamLogs(context.Context, string, string, func([]byte) error) error
	ReadLogs(context.Context, string, string) ([]byte, error)
	WaitJob(context.Context, string, string, time.Duration, time.Duration) (jobOutcome, error)
	DeleteJob(context.Context, string, string) error
}

type kubectlRunner struct {
	kubectl string
}

func newKubectlRunner(kubectl string) kubectlRunner {
	kubectl = strings.TrimSpace(kubectl)
	if kubectl == "" {
		kubectl = defaultKubectl
	}

	return kubectlRunner{kubectl: kubectl}
}

func (r kubectlRunner) ApplyJob(ctx context.Context, namespace string, manifest []byte) error {
	_, err := r.run(ctx, namespace, manifest, "apply", "-f", "-")
	return err
}

func (r kubectlRunner) DeleteJob(ctx context.Context, namespace, name string) error {
	_, err := r.run(ctx, namespace, nil, "delete", "job", name, "--ignore-not-found=true")
	return err
}

func (r kubectlRunner) StreamLogs(ctx context.Context, namespace, name string, send func([]byte) error) error {
	for ctx.Err() == nil {
		sent, err := r.streamLogsOnce(ctx, namespace, name, send)
		if ctx.Err() != nil {
			return nil
		}

		if err == nil {
			if sent {
				return nil
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		if sent {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}

	return nil
}

func (r kubectlRunner) ReadLogs(ctx context.Context, namespace, name string) ([]byte, error) {
	return r.run(ctx, namespace, nil, "logs", "job/"+name, "--container", "task")
}

func (r kubectlRunner) WaitJob(ctx context.Context, namespace, name string, timeout, pollInterval time.Duration) (jobOutcome, error) {
	if timeout <= 0 {
		timeout = defaultWaitTimeout
	}

	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		outcome, terminal, err := r.inspectJob(ctx, namespace, name)
		if err != nil {
			return jobOutcome{Phase: jobUnknown, Message: err.Error()}, err
		}

		if terminal {
			return outcome, nil
		}

		select {
		case <-ctx.Done():
			return jobOutcome{Phase: jobCancelled, Message: ctx.Err().Error()}, nil
		case <-deadline.C:
			return jobOutcome{Phase: jobUnknown, Reason: "wait_timeout", Message: fmt.Sprintf("timed out after %s", timeout)}, nil
		case <-ticker.C:
		}
	}
}

func (r kubectlRunner) inspectJob(ctx context.Context, namespace, name string) (jobOutcome, bool, error) {
	out, err := r.run(ctx, namespace, nil, "get", "job", name, "-o", "json")
	if err != nil {
		return jobOutcome{}, false, err
	}

	var job struct {
		Status struct {
			Active     int `json:"active,omitempty"`
			Succeeded  int `json:"succeeded,omitempty"`
			Failed     int `json:"failed,omitempty"`
			Conditions []struct {
				Type    string `json:"type,omitempty"`
				Status  string `json:"status,omitempty"`
				Reason  string `json:"reason,omitempty"`
				Message string `json:"message,omitempty"`
			} `json:"conditions,omitempty"`
		} `json:"status,omitempty"`
	}

	if err := json.Unmarshal(out, &job); err != nil {
		return jobOutcome{}, false, fmt.Errorf("parse job status: %w", err)
	}

	for _, condition := range job.Status.Conditions {
		if !strings.EqualFold(condition.Status, "true") {
			continue
		}

		switch condition.Type {
		case "Complete":
			return jobOutcome{Phase: jobSucceeded, Reason: condition.Reason, Message: condition.Message}, true, nil
		case "Failed":
			return jobOutcome{Phase: jobFailed, Reason: condition.Reason, Message: condition.Message}, true, nil
		}
	}

	if job.Status.Succeeded > 0 {
		return jobOutcome{Phase: jobSucceeded}, true, nil
	}

	if job.Status.Failed > 0 && job.Status.Active == 0 {
		return jobOutcome{Phase: jobFailed, Message: "job has failed pods", ExitCode: 1}, true, nil
	}

	return jobOutcome{Phase: jobRunning}, false, nil
}

func (r kubectlRunner) streamLogsOnce(ctx context.Context, namespace, name string, send func([]byte) error) (bool, error) {
	args := r.args(namespace, "logs", "-f", "job/"+name, "--container", "task")
	cmd := exec.CommandContext(ctx, r.kubectl, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return false, err
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return false, err
	}

	sent := false
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var sendErr error
	for scanner.Scan() {
		sent = true
		line := append([]byte(nil), scanner.Bytes()...)
		line = append(line, '\n')
		if err := send(line); err != nil {
			sendErr = err
			_ = cmd.Process.Kill()
			break
		}
	}

	if scanErr := scanner.Err(); scanErr != nil && sendErr == nil {
		sendErr = scanErr
	}

	waitErr := cmd.Wait()
	if sendErr != nil {
		return sent, sendErr
	}

	if waitErr != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return sent, fmt.Errorf("%w: %s", waitErr, detail)
		}

		return sent, waitErr
	}

	return sent, nil
}

func (r kubectlRunner) run(ctx context.Context, namespace string, stdin []byte, args ...string) ([]byte, error) {
	args = r.args(namespace, args...)
	cmd := exec.CommandContext(ctx, r.kubectl, args...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return nil, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, detail)
		}

		return nil, fmt.Errorf("kubectl %s: %w", strings.Join(args, " "), err)
	}

	return stdout.Bytes(), nil
}

func (r kubectlRunner) args(namespace string, args ...string) []string {
	if strings.TrimSpace(namespace) == "" {
		return append([]string(nil), args...)
	}

	out := []string{"-n", namespace}
	out = append(out, args...)
	return out
}

type jobSpecInput struct {
	Name      string
	Namespace string
	Image     string
	Command   string
	ExtraEnv  []envVar
	JobID     string
	RunID     string
	TaskKey   string
	SessionID string
}

func buildJobManifest(in jobSpecInput) ([]byte, error) {
	backoffLimit := int32(0)
	automountServiceAccountToken := false
	labels := map[string]string{
		"app.kubernetes.io/name":      "vectis-worker-core-task",
		"app.kubernetes.io/component": "worker-core-task",
		"vectis.dev/run":              labelValue(in.RunID),
		"vectis.dev/task":             labelValue(in.TaskKey),
		"vectis.dev/session":          labelValue(in.SessionID),
	}

	annotations := map[string]string{
		"vectis.dev/job-id":     in.JobID,
		"vectis.dev/run-id":     in.RunID,
		"vectis.dev/task-key":   in.TaskKey,
		"vectis.dev/session-id": in.SessionID,
	}

	env := []envVar{
		{Name: "VECTIS_JOB_ID", Value: in.JobID},
		{Name: "VECTIS_RUN_ID", Value: in.RunID},
		{Name: "VECTIS_TASK_KEY", Value: in.TaskKey},
		{Name: "VECTIS_SESSION_ID", Value: in.SessionID},
	}

	env = append(env, in.ExtraEnv...)
	manifest := batchJob{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Metadata: objectMeta{
			Name:        in.Name,
			Namespace:   in.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: batchJobSpec{
			BackoffLimit: &backoffLimit,
			Template: podTemplateSpec{
				Metadata: objectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: podSpec{
					RestartPolicy:                "Never",
					AutomountServiceAccountToken: &automountServiceAccountToken,
					Containers: []containerSpec{
						{
							Name:    "task",
							Image:   in.Image,
							Command: []string{"sh", "-c", in.Command},
							Env:     env,
						},
					},
				},
			},
		},
	}

	return yaml.Marshal(manifest)
}

type batchJob struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Metadata   objectMeta   `yaml:"metadata"`
	Spec       batchJobSpec `yaml:"spec"`
}

type objectMeta struct {
	Name        string            `yaml:"name,omitempty"`
	Namespace   string            `yaml:"namespace,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

type batchJobSpec struct {
	BackoffLimit *int32          `yaml:"backoffLimit,omitempty"`
	Template     podTemplateSpec `yaml:"template"`
}

type podTemplateSpec struct {
	Metadata objectMeta `yaml:"metadata,omitempty"`
	Spec     podSpec    `yaml:"spec"`
}

type podSpec struct {
	RestartPolicy                string          `yaml:"restartPolicy"`
	AutomountServiceAccountToken *bool           `yaml:"automountServiceAccountToken,omitempty"`
	Containers                   []containerSpec `yaml:"containers"`
}

type containerSpec struct {
	Name    string   `yaml:"name"`
	Image   string   `yaml:"image"`
	Command []string `yaml:"command"`
	Env     []envVar `yaml:"env,omitempty"`
}

type envVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

func jobNameFor(runID, taskKey, sessionID string) string {
	base := dnsLabel(strings.Trim(runID+"-"+taskKey, "-"), 42)
	sum := sha256.Sum256([]byte(runID + "\n" + taskKey + "\n" + sessionID))

	return dnsLabel("vectis-"+base+"-"+hex.EncodeToString(sum[:])[:12], 63)
}

func labelValue(value string) string {
	return dnsLabel(value, 63)
}

func dnsLabel(value string, max int) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false

			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "unknown"
	}

	if max > 0 && len(out) > max {
		out = strings.Trim(out[:max], "-")
	}

	if out == "" {
		return "unknown"
	}

	return out
}

func envDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	return value
}

func envDurationDefault(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("Ignoring invalid %s=%q: %v", key, value, err)
		return fallback
	}

	return parsed
}

func envBoolDefault(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		log.Printf("Ignoring invalid %s=%q", key, value)
		return fallback
	}
}

var _ sdk.Core = (*kubernetesCore)(nil)
var _ kubeRunner = kubectlRunner{}
