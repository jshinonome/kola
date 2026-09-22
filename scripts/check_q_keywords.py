"""Recheck the reference keyword snapshot with q on PATH; emit JSON to stdout.

Run from any directory: python scripts/check_q_keywords.py
Only resolves and serializes function values; never applies the functions.
"""

import json
import subprocess
from datetime import date
from pathlib import Path


def main():
    snapshot = Path(__file__).resolve().parents[1] / "py-kola/test/q_keyword_types.json"
    data = json.loads(snapshot.read_text())
    lines = ['-1 "VERSION|",string .z.K;']
    for entry in data["keywords"]:
        name = entry["name"]
        quoted_name = json.dumps(name)
        lines.append(
            f"-1 {json.dumps(name + '|')},@["
            '{f:value x;b:8_-8!f;(string type f),"|",'
            '(string "i"$first b),"|",$[2=count b;string "i"$last b;""]};'
            f'{quoted_name};{{"error|",x}}];'
        )
    lines.append("exit 0")
    result = subprocess.run(
        ["q", "-q"],
        input="\n".join(lines) + "\n",
        text=True,
        capture_output=True,
        check=True,
        timeout=30,
    )
    if result.stderr:
        raise RuntimeError(result.stderr)
    output = result.stdout.splitlines()
    if not output or not output[0].startswith("VERSION|"):
        raise RuntimeError("q did not return a version")
    rows = []
    for line in output[1:]:
        name, *parts = line.split("|")
        row = {"name": name}
        if parts[0] == "error":
            row["error"] = "|".join(parts[1:])
        else:
            row.update(
                q_type=int(parts[0]),
                wire_type=int(parts[1]),
                code=int(parts[2]) if parts[2] else None,
            )
        rows.append(row)
    if [row["name"] for row in rows] != [row["name"] for row in data["keywords"]]:
        raise RuntimeError("q did not return every keyword in order")
    data.update(
        q_version=output[0].split("|", 1)[1],
        checked_date=date.today().isoformat(),
        keywords=rows,
        scope="177 reference-card keywords plus <>, <=, >=; version-specific observations",
    )
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
