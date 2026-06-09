package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

const (
	defaultPostgresImage = "postgres:18-alpine"
	postgresDatabase     = "vectis"
	postgresPassword     = "vectis"
	postgresUser         = "vectis"

	envDatabaseDriver  = "VECTIS_PERF_DATABASE_DRIVER"
	envDatabaseDSN     = "VECTIS_PERF_DATABASE_DSN"
	envPodman          = "VECTIS_PERF_PODMAN"
	envPostgresImage   = "VECTIS_PERF_POSTGRES_IMAGE"
	envPostgresProfile = "VECTIS_PERF_POSTGRES_DURABILITY"
	envPostgresPort    = "VECTIS_PERF_POSTGRES_PORT"
	envVectisDriver    = "VECTIS_DATABASE_DRIVER"
	envVectisDSN       = "VECTIS_DATABASE_DSN"

	postgresDurabilityUnsafe = "unsafe"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}

	if len(args) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: perf-postgres-podman -- <command> [args...]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	podman := os.Getenv(envPodman)
	if podman == "" {
		podman = "podman"
	}

	image := os.Getenv(envPostgresImage)
	if image == "" {
		image = defaultPostgresImage
	}

	hostPort, err := postgresHostPort()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "choose postgres host port: %v\n", err)
		os.Exit(1)
	}

	containerName := fmt.Sprintf("vectis-perf-postgres-%d-%d", os.Getpid(), time.Now().UnixNano())
	containerID, err := startPostgres(ctx, podman, image, containerName, hostPort)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "start podman postgres: %v\n", err)
		os.Exit(1)
	}
	defer removeContainer(podman, containerID)

	if err := waitForPostgres(ctx, podman, containerID); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "wait for podman postgres: %v\n", err)
		os.Exit(1)
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable",
		postgresUser,
		postgresPassword,
		hostPort,
		postgresDatabase,
	)

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = append(
		os.Environ(),
		envDatabaseDriver+"=pgx",
		envDatabaseDSN+"="+dsn,
		envVectisDriver+"=pgx",
		envVectisDSN+"="+dsn,
	)

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}

		_, _ = fmt.Fprintf(os.Stderr, "run benchmark command: %v\n", err)
		os.Exit(1)
	}
}

func postgresHostPort() (string, error) {
	if port := os.Getenv(envPostgresPort); port != "" {
		return port, nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected listener address %q", listener.Addr())
	}

	return fmt.Sprintf("%d", addr.Port), nil
}

func startPostgres(ctx context.Context, podman, image, containerName, hostPort string) (string, error) {
	args := []string{
		"run",
		"--detach",
		"--rm",
		"--name",
		containerName,
		"-e",
		"POSTGRES_DB=" + postgresDatabase,
		"-e",
		"POSTGRES_PASSWORD=" + postgresPassword,
		"-e",
		"POSTGRES_USER=" + postgresUser,
		"-p",
		"127.0.0.1:" + hostPort + ":5432",
		image,
		"postgres",
		"-c",
		"shared_preload_libraries=pg_stat_statements",
		"-c",
		"pg_stat_statements.track=all",
	}

	if postgresDurabilityProfile() == postgresDurabilityUnsafe {
		args = append(
			args,
			"-c",
			"fsync=off",
			"-c",
			"synchronous_commit=off",
			"-c",
			"full_page_writes=off",
		)
	}

	output, err := exec.CommandContext(ctx, podman, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", podman, args, err, string(output))
	}

	return strings.TrimSpace(string(output)), nil
}

func postgresDurabilityProfile() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv(envPostgresProfile)))
}

func waitForPostgres(ctx context.Context, podman, containerID string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastOutput string
	for {
		output, err := exec.CommandContext(ctx, podman, "exec", containerID, "pg_isready", "-U", postgresUser, "-d", postgresDatabase).CombinedOutput()
		if err == nil {
			return nil
		}
		lastOutput = string(output)

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; last pg_isready output: %s", ctx.Err(), lastOutput)
		case <-ticker.C:
		}
	}
}

func removeContainer(podman, containerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, podman, "rm", "-f", "-t", "0", containerID).CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove podman container %s: %v\n%s", containerID, err, string(output))
	}
}
