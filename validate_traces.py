"""Validate that both demo jobs' traces span the head and Kali nodes.

Queries the Jaeger query API for the latest trace of each job root span
(``job-1-task-actor`` / ``job-2-pool``) under the job's own service name and
checks, per trace:

- every process in the trace carries the job's ``service.name`` (no spans
  leaking out under the base service or the other job's),
- spans were produced on BOTH nodes (``host.name`` process tag: the Mac's
  hostname and ``kali``),
- the expected worker-side child spans are present.

Usage::

    .venv/bin/python validate_traces.py

Exit code 0 when every check passes, 1 otherwise.
"""

import json
import socket
import sys
import urllib.request

JAEGER = "http://localhost:16686"
KALI_HOST = "kali"

# Per-job expectations: the service.name the job's spans must all carry and
# the worker-side custom span names it must produce.
JOBS = {
    "job-1-task-actor": {
        "service": "ray-otel-job-1",
        "children": {"fetch_data.db_lookup", "Processor.process.transform"},
    },
    "job-2-pool": {
        "service": "ray-otel-job-2",
        "children": {"scale_item"},
    },
}


def get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def find_trace(root_name: str, service: str) -> dict | None:
    url = (
        f"{JAEGER}/api/traces?service={service}&operation={root_name}"
        "&limit=50&lookback=1h"
    )
    # Jaeger does not guarantee newest-first ordering, so don't trust the
    # first match — an older run of the same job may still be stored.
    candidates = [
        t
        for t in get(url).get("data", [])
        if any(
            not s.get("references") and s["operationName"] == root_name
            for s in t["spans"]
        )
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda t: max(s["startTime"] for s in t["spans"]))


def span_hosts(trace: dict) -> dict[str, list[str]]:
    """Map each hostname to the span names it produced."""
    hosts: dict[str, list[str]] = {}
    for span in trace["spans"]:
        process = trace["processes"][span["processID"]]
        host = next(
            (t["value"] for t in process.get("tags", []) if t["key"] == "host.name"),
            "<unknown>",
        )
        hosts.setdefault(host, []).append(span["operationName"])
    return hosts


def main() -> int:
    head_host = socket.gethostname()
    failures = []

    for root_name, job in JOBS.items():
        service = job["service"]
        trace = find_trace(root_name, service)
        if trace is None:
            failures.append(
                f"{root_name}: no trace found in Jaeger (service {service})"
            )
            continue

        services = {p["serviceName"] for p in trace["processes"].values()}
        hosts = span_hosts(trace)
        span_names = [s["operationName"] for s in trace["spans"]]
        missing = job["children"] - set(span_names)

        print(f"{root_name} (service {service}): {len(trace['spans'])} spans")
        for host in sorted(hosts):
            print(f"  {host}: {len(hosts[host])} spans")

        job_failures = []
        if services != {service}:
            job_failures.append(
                f"{root_name}: spans under unexpected services {sorted(services)}"
            )
        if missing:
            job_failures.append(f"{root_name}: missing spans {sorted(missing)}")
        for required in (head_host, KALI_HOST):
            if required not in hosts:
                job_failures.append(f"{root_name}: no spans from {required}")
        if job_failures:
            failures.extend(job_failures)
        else:
            print("  OK: uniform service name, spans on head + kali, expected children")
        print()

    if failures:
        print("VALIDATION FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
