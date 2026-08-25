"""Benchmark kola IPC against the Podman KDB-X fixture server.

Emits a JSON document sharing the same top-level core as the Node benchmark
(`subject`, `fixture`, `runtime`, `method`, plus per-table operations) so the
two suites stay comparable without forcing a single rigid schema on both.
"""

import argparse
import importlib.metadata
import json
import os
import platform
import statistics
import time
from pathlib import Path

import polars as pl

from kola import Q, serialize_as_ipc_bytes6

SCHEMA_VERSION = 2
DEFAULT_TABLES = ("trade", "wide", "depth")
DEFAULT_OPERATIONS = ("read", "send", "serialize")
OPERATION_DESCRIPTIONS = {
    "read": "q.sync(table) returning a polars DataFrame",
    "send": "q.sync('{count x}', frame) acknowledged by row count",
    "serialize": "serialize_as_ipc_bytes6 without network",
}
DEFAULT_WARMUPS = 2
DEFAULT_ITERATIONS = 100


def percentile_ns(ordered, fraction):
    """Nearest-rank percentile over an ascending sample list."""
    rank = max(0, min(len(ordered) - 1, round(fraction * (len(ordered) - 1))))
    return ordered[rank]


def measure(operation, warmups, iterations):
    for _ in range(warmups):
        operation()

    samples = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        operation()
        samples.append(time.perf_counter_ns() - started)

    ordered = sorted(samples)
    return {
        "iterations": iterations,
        "min_ms": ordered[0] / 1_000_000,
        "mean_ms": statistics.mean(samples) / 1_000_000,
        "median_ms": statistics.median(samples) / 1_000_000,
        "p90_ms": percentile_ns(ordered, 0.90) / 1_000_000,
        "p95_ms": percentile_ns(ordered, 0.95) / 1_000_000,
        "p99_ms": percentile_ns(ordered, 0.99) / 1_000_000,
        "max_ms": ordered[-1] / 1_000_000,
        "stdev_ms": statistics.stdev(samples) / 1_000_000 if len(samples) > 1 else 0.0,
        "samples_ms": [sample / 1_000_000 for sample in samples],
    }


def with_throughput(metrics, payload_bytes):
    metrics["payload_bytes"] = payload_bytes
    metrics["throughput_mib_s"] = payload_bytes / (metrics["median_ms"] / 1000) / 2**20
    return metrics


def benchmark_table(q, table, warmups, iterations, operations, payload_bytes=None):
    frame = q.sync(table)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError(f"{table} returned {type(frame).__name__}, expected DataFrame")

    payload_source = "provided"
    if payload_bytes is None:
        # Measuring the payload requires this subject's serializer; callers whose
        # serializer cannot handle the table pass the size from another subject.
        payload_bytes = len(serialize_as_ipc_bytes6("sync", False, frame))
        payload_source = "measured"
    expected_rows = frame.height

    # client -> q: table upload acknowledged by a server-side row count.
    def send_frame():
        actual_rows = q.sync("{count x}", frame)
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"q counted {actual_rows} rows for {table}, expected {expected_rows}"
            )

    measured = {
        # q -> client: full query round-trip ending in a materialized DataFrame.
        "read": lambda: q.sync(table),
        "send": send_frame,
        # Pure client-side q IPC serialization: no network, isolates the codec.
        "serialize": lambda: serialize_as_ipc_bytes6("sync", False, frame),
    }

    report = {
        "rows": frame.height,
        "columns": frame.width,
        "estimated_memory_bytes": frame.estimated_size(),
        "ipc_message_bytes": payload_bytes,
        "payload_bytes_source": payload_source,
    }
    for operation in operations:
        report[operation] = with_throughput(
            measure(measured[operation], warmups, iterations), payload_bytes
        )
    return report


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark kola IPC against the Podman KDB-X fixture server."
    )
    parser.add_argument("--host", default=os.environ.get("KOLA_TEST_Q_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("KOLA_TEST_Q_PORT", "1801"))
    )
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS)
    parser.add_argument("--tables", nargs="+", choices=DEFAULT_TABLES, default=DEFAULT_TABLES)
    parser.add_argument(
        "--operations",
        nargs="+",
        choices=DEFAULT_OPERATIONS,
        default=DEFAULT_OPERATIONS,
        help="Operations to measure; defaults to all of them.",
    )
    parser.add_argument(
        "--payload-bytes",
        type=int,
        help="Externally measured IPC payload size; skips this subject's own "
        "payload probe (only sensible with a single table).",
    )
    parser.add_argument(
        "--subject-label",
        default=os.environ.get("KOLA_BENCH_SUBJECT", "local"),
        help="How this interpreter's kola package should be labelled in the report.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.payload_bytes is not None and len(args.tables) > 1:
        parser.error("--payload-bytes requires exactly one table")
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    args.tables = list(dict.fromkeys(args.tables))
    args.operations = list(dict.fromkeys(args.operations))
    return args


def main():
    args = parse_args()
    q = Q(args.host, args.port, timeout=120)
    connected = False
    try:
        q.connect()
        connected = True
        result = {
            "schemaVersion": SCHEMA_VERSION,
            "subject": {
                "package": "kola",
                "label": args.subject_label,
                "version": importlib.metadata.version("kola"),
            },
            "fixture": {
                "host": args.host,
                "port": args.port,
                "q": q.sync(".z.K"),
                "rows": q.sync(".kola.rows"),
                "tables": args.tables,
            },
            "runtime": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "polars": pl.__version__,
            },
            "method": {
                "warmups": args.warmups,
                "iterations": args.iterations,
                "timer": "perf_counter_ns",
                "operations": {
                    operation: OPERATION_DESCRIPTIONS[operation]
                    for operation in args.operations
                },
            },
            "tables": {
                table: benchmark_table(
                    q, table, args.warmups, args.iterations, args.operations,
                    args.payload_bytes,
                )
                for table in args.tables
            },
        }
    finally:
        if connected:
            q.disconnect()

    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
