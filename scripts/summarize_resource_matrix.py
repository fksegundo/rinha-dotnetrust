#!/usr/bin/env python3
import json
import pathlib
import re
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


def parse_stats(path):
    stats = {}
    if not path.exists():
        return stats
    pattern = re.compile(r"^(?P<name>\S+)\s+(?P<cpu>\S+)\s+(?P<mem>.+?)\s+(?P<mempct>\S+)$")
    for line in path.read_text().splitlines():
        match = pattern.match(line.strip())
        if not match:
            continue
        name = match.group("name")
        if "-search-" in name:
            key = "search"
        elif "-api1-" in name:
            key = "api1"
        elif "-api2-" in name:
            key = "api2"
        elif "-lb-" in name:
            key = "lb"
        else:
            continue
        stats[f"{key}_docker_cpu"] = match.group("cpu")
        stats[f"{key}_docker_mem"] = match.group("mem")
    return stats


def main():
    if len(sys.argv) != 2:
        print("Usage: summarize_resource_matrix.py <results-root>", file=sys.stderr)
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
            "search_cpus": config.get("search_cpus", ""),
            "api1_cpus": config.get("api1_cpus", ""),
            "api2_cpus": config.get("api2_cpus", ""),
            "lb_cpus": config.get("lb_cpus", ""),
            "search_memory": config.get("search_memory", ""),
            "api1_memory": config.get("api1_memory", ""),
            "api2_memory": config.get("api2_memory", ""),
            "lb_memory": config.get("lb_memory", ""),
            "p99": "",
            "final_score": "",
            "fp": "",
            "fn": "",
            "http_errors": "",
            "search_docker_cpu": "",
            "api1_docker_cpu": "",
            "api2_docker_cpu": "",
            "lb_docker_cpu": "",
            "search_docker_mem": "",
            "api1_docker_mem": "",
            "api2_docker_mem": "",
            "lb_docker_mem": "",
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
        row.update(parse_stats(run_dir / "docker-stats.txt"))
        rows.append(row)

    headers = [
        "run",
        "status",
        "search_cpus",
        "api1_cpus",
        "api2_cpus",
        "lb_cpus",
        "search_memory",
        "api1_memory",
        "api2_memory",
        "lb_memory",
        "p99",
        "final_score",
        "fp",
        "fn",
        "http_errors",
        "search_docker_cpu",
        "api1_docker_cpu",
        "api2_docker_cpu",
        "lb_docker_cpu",
        "search_docker_mem",
        "api1_docker_mem",
        "api2_docker_mem",
        "lb_docker_mem",
    ]
    out = root / "summary.tsv"
    out.write_text(
        "\t".join(headers)
        + "\n"
        + "\n".join("\t".join(str(row.get(h, "")) for h in headers) for row in rows)
        + "\n"
    )
    print(out)


if __name__ == "__main__":
    raise SystemExit(main())
