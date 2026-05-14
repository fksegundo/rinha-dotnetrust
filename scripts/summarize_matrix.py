#!/usr/bin/env python3
import json
import pathlib
import sys


def read_config(path):
    values = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def main():
    if len(sys.argv) != 2:
        print("Usage: summarize_matrix.py <results-root>", file=sys.stderr)
        return 2

    root = pathlib.Path(sys.argv[1])
    rows = []
    for run_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        config = read_config(run_dir / "config.txt")
        result_path = run_dir / "results.json"
        status = (run_dir / "status.txt").read_text().strip() if (run_dir / "status.txt").exists() else "missing"
        row = {
            "run": run_dir.name,
            "status": status,
            "leaf_size": config.get("leaf_size", ""),
            "max_leaf_visits": config.get("max_leaf_visits", ""),
            "p99": "",
            "final_score": "",
            "fp": "",
            "fn": "",
            "http_errors": "",
        }
        if result_path.exists():
            data = json.loads(result_path.read_text())
            scoring = data.get("scoring", {})
            breakdown = scoring.get("breakdown", {})
            row.update(
                {
                    "p99": data.get("p99", ""),
                    "final_score": scoring.get("final_score", ""),
                    "fp": breakdown.get("false_positive_detections", ""),
                    "fn": breakdown.get("false_negative_detections", ""),
                    "http_errors": breakdown.get("http_errors", ""),
                }
            )
        rows.append(row)

    out = root / "summary.tsv"
    headers = ["run", "status", "leaf_size", "max_leaf_visits", "p99", "final_score", "fp", "fn", "http_errors"]
    out.write_text(
        "\t".join(headers)
        + "\n"
        + "\n".join("\t".join(str(row[h]) for h in headers) for row in rows)
        + "\n"
    )
    print(out)


if __name__ == "__main__":
    raise SystemExit(main())
