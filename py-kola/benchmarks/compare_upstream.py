"""Benchmark the locally built kola wheel against the published upstream release.

Both subjects import as ``kola``, so each runs in its own uv-managed virtual
environment with an identical pinned Python dependency set, executing the same
``bench_ipc.py`` against the same running KDB-X fixture server. The merged
report keeps both raw documents and adds per-operation median speedups
(``upstream_median / local_median``; values above 1 mean the local build is
faster). Every step fails hard: there are no fallbacks.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
PY_KOLA_DIR = BENCH_DIR.parent
WORK_DIR = BENCH_DIR / ".compare"
OPERATIONS = ("read", "send", "serialize")
STRIPPED_ENVIRONMENT = ("KX_BEARER_TOKEN", "KDB_LICENSE_B64")


def run(arguments, **kwargs):
    print("+", " ".join(str(argument) for argument in arguments), flush=True)
    subprocess.run(arguments, check=True, **kwargs)


def venv_python(venv: Path) -> Path:
    if os.name == "nt":
        return venv / "Scripts" / "python.exe"
    return venv / "bin" / "python"


def build_local_wheel() -> Path:
    dist = WORK_DIR / "dist"
    run(["uv", "build", "--wheel", "--out-dir", str(dist), str(PY_KOLA_DIR)])
    wheels = sorted(dist.glob("kola-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one local wheel in {dist}, found {wheels}")
    return wheels[0]


def create_subject_environment(name: str, python: str, packages: list[str]) -> Path:
    venv = WORK_DIR / f"venv-{name}"
    run(["uv", "venv", "--python", python, str(venv)])
    run(["uv", "pip", "install", "--python", str(venv_python(venv)), *packages])
    return venv


def run_subject(name: str, venv: Path, args, payload_hints=None) -> dict:
    """Run one benchmark process per table+operation so a native crash in one
    subject (observed in upstream releases) is recorded instead of destroying
    every other measurement. Failures are reported verbatim, never retried."""
    environment = {
        key: value for key, value in os.environ.items() if key not in STRIPPED_ENVIRONMENT
    }
    document = None
    tables = {}
    for table in args.tables:
        table_report = {}
        for operation in OPERATIONS:
            output = WORK_DIR / f"{name}-{table}-{operation}.json"
            command = [
                str(venv_python(venv)),
                str(BENCH_DIR / "bench_ipc.py"),
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--warmups",
                str(args.warmups),
                "--iterations",
                str(args.iterations),
                "--tables",
                table,
                "--operations",
                operation,
                "--subject-label",
                name,
                "--output",
                str(output),
            ]
            hint = (payload_hints or {}).get(table)
            if hint is not None:
                command += ["--payload-bytes", str(hint)]
            print("+", " ".join(command), flush=True)
            completed = subprocess.run(
                command, env=environment, stdout=subprocess.DEVNULL
            )
            if completed.returncode != 0:
                table_report[operation] = {
                    "failed": True,
                    "exit_code": completed.returncode,
                }
                print(
                    f"! {name} {table} {operation} exited with {completed.returncode}",
                    flush=True,
                )
                continue
            partial = json.loads(output.read_text(encoding="utf-8"))
            document = document or partial
            table_partial = partial["tables"][table]
            table_report.setdefault("rows", table_partial["rows"])
            table_report.setdefault("columns", table_partial["columns"])
            table_report.setdefault(
                "ipc_message_bytes", table_partial["ipc_message_bytes"]
            )
            table_report[operation] = table_partial[operation]
        tables[table] = table_report
    if document is None:
        raise RuntimeError(f"every benchmark process for subject {name} failed")
    document["tables"] = tables
    document["fixture"]["tables"] = list(args.tables)
    return document


def build_comparison(local: dict, upstream: dict) -> dict:
    comparison = {}
    for table, local_table in local["tables"].items():
        upstream_table = upstream["tables"][table]
        table_comparison = {}
        for operation in OPERATIONS:
            local_metrics = local_table.get(operation, {"failed": True})
            upstream_metrics = upstream_table.get(operation, {"failed": True})
            entry = {}
            if local_metrics.get("failed"):
                entry["local_failed"] = True
            else:
                entry["local_median_ms"] = local_metrics["median_ms"]
            if upstream_metrics.get("failed"):
                entry["upstream_failed"] = True
            else:
                entry["upstream_median_ms"] = upstream_metrics["median_ms"]
            if "local_median_ms" in entry and "upstream_median_ms" in entry:
                entry["speedup"] = entry["upstream_median_ms"] / entry["local_median_ms"]
            table_comparison[operation] = entry
        comparison[table] = table_comparison
    return comparison


def render_comparison(local: dict, upstream: dict, comparison: dict) -> str:
    lines = [
        f"local kola {local['subject']['version']} vs upstream kola "
        f"{upstream['subject']['version']} (speedup >1 means local is faster)",
        f"{'table':<8}{'operation':<12}{'local ms':>12}{'upstream ms':>14}{'speedup':>10}",
    ]
    for table, operations in sorted(comparison.items()):
        for operation in OPERATIONS:
            values = operations[operation]
            local_cell = (
                "crashed" if values.get("local_failed") else f"{values['local_median_ms']:.2f}"
            )
            upstream_cell = (
                "crashed"
                if values.get("upstream_failed")
                else f"{values['upstream_median_ms']:.2f}"
            )
            speedup_cell = f"{values['speedup']:.2f}x" if "speedup" in values else "-"
            lines.append(
                f"{table:<8}{operation:<12}{local_cell:>12}{upstream_cell:>14}{speedup_cell:>10}"
            )
    return "\n".join(lines)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Compare local and upstream kola wheels on one fixture server."
    )
    parser.add_argument("--host", default=os.environ.get("KOLA_TEST_Q_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("KOLA_TEST_Q_PORT", "1801"))
    )
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument(
        "--tables", nargs="+", default=["trade", "wide", "depth"],
        help="Fixture tables to benchmark for both subjects.",
    )
    parser.add_argument(
        "--upstream-spec",
        default="kola",
        help="pip requirement for the upstream subject (e.g. kola==2.5.1)",
    )
    parser.add_argument(
        "--polars-spec",
        default="polars==1.44.0",
        help="Exact polars pin shared by both subjects to keep the comparison controlled.",
    )
    parser.add_argument("--python", default="3.12")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main():
    args = parse_args()
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True)

    wheel = build_local_wheel()
    local_venv = create_subject_environment(
        "local", args.python, [str(wheel), args.polars_spec]
    )
    upstream_venv = create_subject_environment(
        "upstream", args.python, [args.upstream_spec, args.polars_spec]
    )

    local = run_subject("local", local_venv, args)
    payload_hints = {
        table: report["ipc_message_bytes"]
        for table, report in local["tables"].items()
        if "ipc_message_bytes" in report
    }
    upstream = run_subject("upstream", upstream_venv, args, payload_hints)

    comparison = build_comparison(local, upstream)
    report = {
        "schemaVersion": 1,
        "kind": "kola-python-upstream-comparison",
        "subjects": {"local": local, "upstream": upstream},
        "comparison": comparison,
    }

    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(render_comparison(local, upstream, comparison))
    return 0


if __name__ == "__main__":
    sys.exit(main())
