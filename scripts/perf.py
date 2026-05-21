#!/usr/bin/env python3
"""Vectis performance harness.

This is intentionally small today: it runs checked-in Go benchmark suites while
adding the artifact and comparison shape needed for a broader solution-wide
performance suite.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Sequence


DEFAULT_QUEUE_BENCH = (
    r"BenchmarkQueue_(EnqueueDequeue_RoundTrip|ConcurrentEnqueueDequeue|"
    r"SustainedLoad|RingLatency)"
)

DEFAULT_DAL_BENCH = r"BenchmarkDAL_"
DEFAULT_ARTIFACT_DIR = "artifacts/perf"


@dataclass
class BenchmarkMetric:
    value: str
    unit: str


@dataclass
class BenchmarkResult:
    name: str
    iterations: str
    metrics: list[BenchmarkMetric]


@dataclass
class HarnessMetadata:
    suite: str
    started_at: str
    finished_at: str
    duration_seconds: float
    command: list[str]
    status: int
    git_commit: str
    git_dirty: bool
    go_version: str
    goos: str
    goarch: str
    cpu: str
    pkg: str


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Vectis performance suites.")
    subparsers = parser.add_subparsers(dest="suite", required=True)

    add_go_benchmark_suite(
        subparsers,
        name="queue",
        help_text="Run queue microbenchmarks.",
        package="./internal/queue",
        default_bench=DEFAULT_QUEUE_BENCH,
        bench_env="VECTIS_PERF_QUEUE_BENCH",
    )

    add_go_benchmark_suite(
        subparsers,
        name="dal",
        help_text="Run DAL hot-path microbenchmarks.",
        package="./internal/dal",
        default_bench=DEFAULT_DAL_BENCH,
        bench_env="VECTIS_PERF_DAL_BENCH",
    )

    compare = subparsers.add_parser("compare", help="Compare two Go benchmark outputs with benchstat.")
    compare.add_argument("--baseline", required=True, help="Baseline Go benchmark output.")
    compare.add_argument("--current", required=True, help="Current Go benchmark output.")
    compare.add_argument(
        "--benchstat",
        default=os.getenv("BENCHSTAT", "benchstat"),
        help="benchstat binary to use.",
    )

    args = parser.parse_args(argv)
    if hasattr(args, "go_package"):
        return run_go_benchmark_suite(args)

    if args.suite == "compare":
        return run_benchstat(args.benchstat, Path(args.baseline), Path(args.current), None)

    parser.error(f"unsupported suite {args.suite}")
    return 2


def add_go_benchmark_suite(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    name: str,
    help_text: str,
    package: str,
    default_bench: str,
    bench_env: str,
) -> None:
    suite = subparsers.add_parser(name, help=help_text)

    suite.add_argument(
        "--benchtime",
        default=os.getenv("VECTIS_PERF_BENCHTIME", "2s"),
        help="Go benchmark duration per scenario.",
    )

    suite.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("VECTIS_PERF_COUNT", "1")),
        help="Go benchmark repetition count.",
    )

    suite.add_argument(
        "--bench",
        default=os.getenv(bench_env, default_bench),
        help="Go benchmark regex.",
    )

    suite.add_argument(
        "--go",
        default=os.getenv("GO", "go"),
        help="Go binary to use.",
    )

    suite.add_argument(
        "--artifact-dir",
        default=os.getenv("VECTIS_PERF_ARTIFACT_DIR", DEFAULT_ARTIFACT_DIR),
        help="Directory where run artifacts are written.",
    )

    suite.add_argument(
        "--run-name",
        default=os.getenv("VECTIS_PERF_RUN_NAME", ""),
        help="Optional artifact run name. Defaults to timestamp-suite.",
    )

    suite.add_argument(
        "--baseline",
        default=os.getenv("VECTIS_PERF_BASELINE", ""),
        help="Optional baseline Go benchmark output to compare with benchstat.",
    )

    suite.add_argument(
        "--benchstat",
        default=os.getenv("BENCHSTAT", "benchstat"),
        help="benchstat binary to use when --baseline is provided.",
    )

    suite.set_defaults(go_package=package)


def run_go_benchmark_suite(args: argparse.Namespace) -> int:
    command = [
        args.go,
        "test",
        args.go_package,
        "-run",
        "^$",
        "-bench",
        args.bench,
        "-benchtime",
        args.benchtime,
        "-count",
        str(args.count),
        "-benchmem",
    ]

    started = dt.datetime.now(dt.UTC)
    started_monotonic = time.monotonic()
    run_name = args.run_name or f"{started.strftime('%Y%m%dT%H%M%SZ')}-{args.suite}"
    artifact_root = Path(args.artifact_dir)
    run_dir = artifact_root / sanitize_path_part(run_name)
    run_dir.mkdir(parents=True, exist_ok=True)

    print_heading("Vectis performance harness")
    print(f"Suite              : {args.suite}")
    print(f"Package            : {args.go_package}")
    print(f"Benchmark duration : {args.benchtime}")
    print(f"Repetitions        : {args.count}")
    print(f"Benchmark pattern  : {args.bench}")
    print(f"Artifacts          : {run_dir}")

    proc = subprocess.run(
        command,
        cwd=repo_root(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    finished = dt.datetime.now(dt.UTC)
    duration = time.monotonic() - started_monotonic
    raw_output = proc.stdout

    raw_path = run_dir / "go-bench.txt"
    raw_path.write_text(raw_output, encoding="utf-8")

    env = parse_go_benchmark_environment(raw_output)
    results = parse_go_benchmark_results(raw_output)
    metadata = HarnessMetadata(
        suite=args.suite,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=round(duration, 6),
        command=command,
        status=proc.returncode,
        git_commit=git_output(["rev-parse", "--short=12", "HEAD"], "unknown"),
        git_dirty=git_dirty(),
        go_version=command_output([args.go, "version"], "unknown"),
        goos=env.get("goos", ""),
        goarch=env.get("goarch", ""),
        cpu=env.get("cpu", ""),
        pkg=env.get("pkg", ""),
    )

    write_json(run_dir / "summary.json", {
        "metadata": asdict(metadata),
        "results": [
            {
                "name": r.name,
                "iterations": r.iterations,
                "metrics": [asdict(m) for m in r.metrics],
            }
            for r in results
        ],
    })

    write_markdown_summary(run_dir / "summary.md", metadata, results, raw_path)

    print_heading("Environment")
    print_environment(metadata)

    if proc.returncode != 0:
        print_heading("Benchmark failed")
        print(raw_output, end="" if raw_output.endswith("\n") else "\n")
        print_artifact_footer(run_dir)
        return proc.returncode

    print_heading("Benchmark summary")
    print_benchmark_summary(results)

    compare_status = 0
    if args.baseline:
        print_heading("benchstat comparison")
        compare_status = run_benchstat(args.benchstat, Path(args.baseline), raw_path, run_dir / "benchstat.txt")

    print_heading("Raw Go benchmark output")
    print(raw_output, end="" if raw_output.endswith("\n") else "\n")
    print_next_checks(args.suite)
    print_artifact_footer(run_dir)
    return compare_status


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def sanitize_path_part(value: str) -> str:
    value = value.strip()
    if not value:
        return "perf-run"

    return re.sub(r"[^A-Za-z0-9._-]+", "-", value)


def parse_go_benchmark_environment(output: str) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in output.splitlines():
        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        if key in {"goos", "goarch", "pkg", "cpu"}:
            env[key] = value.strip()

    return env


def parse_go_benchmark_results(output: str) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    for line in output.splitlines():
        if not line.startswith("Benchmark"):
            continue

        fields = line.split()
        if len(fields) < 2:
            continue

        name = re.sub(r"-\d+$", "", fields[0])
        metrics: list[BenchmarkMetric] = []
        i = 2

        while i + 1 < len(fields):
            metrics.append(BenchmarkMetric(value=fields[i], unit=fields[i + 1]))
            i += 2

        results.append(BenchmarkResult(name=name, iterations=fields[1], metrics=metrics))

    return results


def print_heading(title: str) -> None:
    print()
    print(title)
    print("=" * len(title))


def print_environment(metadata: HarnessMetadata) -> None:
    rows = [
        ("goos", metadata.goos),
        ("goarch", metadata.goarch),
        ("pkg", metadata.pkg),
        ("cpu", metadata.cpu),
        ("go", metadata.go_version),
        ("commit", metadata.git_commit + (" dirty" if metadata.git_dirty else "")),
    ]

    for key, value in rows:
        if value:
            print(f"{key + ':':<10} {value}")


def print_benchmark_summary(results: list[BenchmarkResult]) -> None:
    if not results:
        print("No benchmark rows parsed.")
        return

    for result in results:
        print(f"- {result.name}")
        print(f"  iterations: {result.iterations}")

        for metric in result.metrics:
            print(f"  {metric.unit + ':':<18} {metric.value}")


def print_next_checks(suite: str) -> None:
    print_heading("Next manual checks")
    if suite == "dal":
        print("- Trigger path: pair these results with API trigger latency and accepted-to-enqueued timing.")
        print("- Worker scale: compare TryClaim/finalize timing against DB pool pressure under multiple workers.")
        print("- Postgres check: rerun the same workload in a deployed stack before making production capacity claims.")
        print("- Query shape: investigate list/repair scans when table-size scenarios move more than normal variance.")
        return

    print("- Trigger bursts: submit stored-job triggers in batches and record accepted latency plus queued-run age.")
    print("- Worker scale: vary worker count and watch claim failures, queue depth, and terminal run latency.")
    print("- Log readers: open concurrent /api/v1/runs/{id}/logs clients and watch active stream and replay metrics.")
    print("- Cron scale: seed schedules, advance time, and record schedule-to-run latency.")


def print_artifact_footer(run_dir: Path) -> None:
    print_heading("Artifacts")
    print(f"- raw benchmark output: {run_dir / 'go-bench.txt'}")
    print(f"- JSON summary        : {run_dir / 'summary.json'}")
    print(f"- Markdown summary    : {run_dir / 'summary.md'}")


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_markdown_summary(
    path: Path,
    metadata: HarnessMetadata,
    results: list[BenchmarkResult],
    raw_path: Path,
) -> None:
    lines = [
        "# Vectis Performance Run",
        "",
        f"- Suite: `{metadata.suite}`",
        f"- Started: `{metadata.started_at}`",
        f"- Duration: `{metadata.duration_seconds}s`",
        f"- Status: `{metadata.status}`",
        f"- Git commit: `{metadata.git_commit}`" + (" dirty" if metadata.git_dirty else ""),
        f"- Go: `{metadata.go_version}`",
        f"- Raw output: `{raw_path.name}`",
        "",
        "## Results",
        "",
    ]

    if not results:
        lines.append("No benchmark rows parsed.")
    else:
        lines.append("| Benchmark | Iterations | Metrics |")
        lines.append("| --- | ---: | --- |")

        for result in results:
            metrics = ", ".join(f"{m.value} {m.unit}" for m in result.metrics)
            lines.append(f"| `{result.name}` | {result.iterations} | {metrics} |")

    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def run_benchstat(benchstat: str, baseline: Path, current: Path, output_path: Path | None) -> int:
    if not baseline.is_file():
        print(f"baseline benchmark output not found: {baseline}", file=sys.stderr)
        return 2

    if not current.is_file():
        print(f"current benchmark output not found: {current}", file=sys.stderr)
        return 2

    if shutil.which(benchstat) is None:
        print(f"benchstat not found: {benchstat}", file=sys.stderr)
        print("Install golang.org/x/perf/cmd/benchstat or set BENCHSTAT.", file=sys.stderr)
        return 127

    proc = subprocess.run(
        [benchstat, str(baseline), str(current)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if output_path is not None:
        output_path.write_text(proc.stdout, encoding="utf-8")

    return proc.returncode


def git_output(args: list[str], default: str) -> str:
    return command_output(["git", *args], default)


def git_dirty() -> bool:
    status = command_output(["git", "status", "--porcelain"], "")
    return bool(status.strip())


def command_output(command: list[str], default: str) -> str:
    try:
        proc = subprocess.run(
            command,
            cwd=repo_root(),
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
    except OSError:
        return default

    if proc.returncode != 0:
        return default

    return proc.stdout.strip() or default


if __name__ == "__main__":
    raise SystemExit(main())
