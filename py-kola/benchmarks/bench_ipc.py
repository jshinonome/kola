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

DEFAULT_TABLES = ("trade", "wide", "depth")


def measure(operation, warmups, iterations):
    for _ in range(warmups):
        operation()

    samples = []
    for _ in range(iterations):
        started = time.perf_counter_ns()
        operation()
        samples.append(time.perf_counter_ns() - started)

    ordered = sorted(samples)
    percentile_index = max(0, (95 * len(ordered) + 99) // 100 - 1)
    median_ns = statistics.median(samples)
    return {
        "iterations": iterations,
        "mean_ms": statistics.mean(samples) / 1_000_000,
        "median_ms": median_ns / 1_000_000,
        "min_ms": min(samples) / 1_000_000,
        "p95_ms": ordered[percentile_index] / 1_000_000,
        "stdev_ms": statistics.stdev(samples) / 1_000_000
        if len(samples) > 1
        else 0.0,
        "samples_ms": [sample / 1_000_000 for sample in samples],
    }


def benchmark_table(q, table, warmups, iterations):
    frame = q.sync(table)
    if not isinstance(frame, pl.DataFrame):
        raise TypeError(f"{table} returned {type(frame).__name__}, expected DataFrame")

    payload_bytes = len(serialize_as_ipc_bytes6("sync", False, frame))
    expected_rows = frame.height

    query = measure(lambda: q.sync(table), warmups, iterations)

    def send_frame():
        actual_rows = q.sync("{count x}", frame)
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"q counted {actual_rows} rows for {table}, expected {expected_rows}"
            )

    send = measure(send_frame, warmups, iterations)
    query["throughput_mib_s"] = payload_bytes / (query["median_ms"] / 1000) / 2**20
    send["throughput_mib_s"] = payload_bytes / (send["median_ms"] / 1000) / 2**20

    return {
        "rows": frame.height,
        "columns": frame.width,
        "estimated_memory_bytes": frame.estimated_size(),
        "ipc_message_bytes": payload_bytes,
        "query": query,
        "send": send,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Benchmark kola IPC against the Podman KDB-X fixture server."
    )
    parser.add_argument("--host", default=os.environ.get("KOLA_TEST_Q_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("KOLA_TEST_Q_PORT", "1801"))
    )
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--tables", nargs="+", choices=DEFAULT_TABLES, default=DEFAULT_TABLES)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("--warmups must be non-negative")
    if args.iterations < 1:
        parser.error("--iterations must be positive")
    args.tables = list(dict.fromkeys(args.tables))
    return args


def main():
    args = parse_args()
    q = Q(args.host, args.port, timeout=120)
    connected = False
    try:
        q.connect()
        connected = True
        result = {
            "environment": {
                "kola": importlib.metadata.version("kola"),
                "polars": pl.__version__,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "q": q.sync(".z.K"),
                "fixture_rows": q.sync(".kola.rows"),
                "host": args.host,
                "port": args.port,
                "warmups": args.warmups,
                "iterations": args.iterations,
            },
            "tables": {
                table: benchmark_table(q, table, args.warmups, args.iterations)
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
