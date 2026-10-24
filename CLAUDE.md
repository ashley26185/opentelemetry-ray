# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Ray + OpenTelemetry → Jaeger tracing demo. Two jobs (`job-1-task-actor`, `job-2-pool`) run traced Ray tasks/actors/Pool workloads, exporting OTLP/HTTP spans straight to a local Jaeger all-in-one container (no collector). Single-node works out of the box; a two-node cluster (Mac head + Kali VM worker) is supported with node pinning. Full walkthrough: README.md. Distributed-tracing how-to with the reasoning behind every choice: TRACING.md.

## Commands

- Setup: `uv venv .venv --python 3.13.13 && uv pip install --python .venv/bin/python -r requirements.txt`
- Jaeger: `docker run -d --name jaeger -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/all-in-one:latest`
- Run (single-node, or connects to a running cluster): `PYTHONPATH="$PWD" .venv/bin/python main.py`
- Run per-job service names (separate driver processes — required, see architecture): `PYTHONPATH="$PWD" .venv/bin/python main.py job-1` (service `ray-otel-job-1`), `main.py job-2` (service `ray-otel-job-2`)
- Validate traces: `.venv/bin/python validate_traces.py` (queries Jaeger; exit 1 unless each job's latest trace is uniformly under its service and contains spans from the Mac and from kali)
- Cluster — head (from repo root):
  `PYTHONPATH="$PWD" .venv/bin/ray start --head --port=6379 --node-ip-address=10.0.0.14 --resources='{"head": 1}' --tracing-startup-hook=tracing_setup:setup_tracing --disable-usage-stats`
- Cluster — Kali worker:
  `ssh kali 'PYTHONPATH=/home/ashley/ray-shared OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.0.14:4318 nohup ~/ray-venv/bin/ray start --address=10.0.0.14:6379 --resources='"'"'{"kali": 1}'"'"' --disable-usage-stats >/tmp/ray-worker.log 2>&1 &'`
- Stop: `ray stop` on each node (`ssh kali '~/ray-venv/bin/ray stop'`).
- No tests/lint/build — this is a demo repo; "the tests" are run + validate against Jaeger.

## How tracing works here

- `tracing_setup.py` is a Ray tracing startup hook (`tracing_setup:setup_tracing`) that runs once per process (driver + every worker). It installs the global `TracerProvider` (`SimpleSpanProcessor` — deliberate, Ray never shuts the provider down) exporting OTLP/HTTP to Jaeger; the Resource carries `service.name` + `host.name` (node attribution used by validation).
- **The hook string lives in GCS.** Only the process that *creates* the cluster stores it: single-node via `ray.init(_tracing_startup_hook=...)`, a `ray start` cluster via `--tracing-startup-hook=...` on the head. Every connecting process (workers AND drivers) reads it from GCS. A connecting driver's `_tracing_startup_hook` kwarg is silently ignored — no hook in GCS means the demo runs fine but produces zero spans, with no warnings.
- Ray instruments every `@ray.remote` call: PRODUCER span in the caller, CONSUMER span in the worker; context propagates via `_ray_trace_ctx`. Custom child spans (`fetch_data.db_lookup`, `Processor.process.transform`, `scale_item`) nest under the CONSUMER span by opening a span inside the function. `ray.util.multiprocessing.Pool` is traced the same way (its internal `PoolActor` is `@ray.remote`), but `map()` batches items per `run_batch` call — hence the per-item `scale_item` spans.
- `main.py`: `pin(node)` / `on(node)` pin tasks/actors to cluster nodes via custom resources (`head`/`kali`, requested as 0.001 each), falling back to unpinned scheduling when the resources are absent. Job identity = root span names (`job-1-task-actor` / `job-2-pool`). Default `service.name` is `ray-otel-demo`; per-job services (`ray-otel-job-1/2`) come from running `main.py job-1` / `job-2` as separate driver processes — each sets `OTEL_SERVICE_NAME` before `ray.init`, and remote calls carry the same name in `runtime_env` env vars, which Ray applies to worker processes *before* the hook runs (Ray pools workers per runtime env, so jobs don't share workers). One process cannot swap services: OTel's `set_tracer_provider` is once-per-process and ignores later calls with a warning.
- `validate_traces.py`: finds each job's latest trace in the Jaeger query API and checks `host.name` process tags cover both the Mac's hostname and `kali`, plus expected child spans.

## Two-node cluster facts (learned the hard way)

- **Exact versions across nodes**: Ray requires identical Ray AND Python patch versions (2.47.1 / 3.13.13 here; 3.13.12 vs 3.13.13 is a hard failure). The VM venv was built with `uv` (`~/.local/bin/uv venv --seed --python 3.13.13 ~/ray-venv`); `ashley`'s sudo needs a password, so avoid apt.
- **Head must advertise the Mac's Wi-Fi IP** (`--node-ip-address=10.0.0.14`): a driver's auto-detected node IP (from the default route) must match a node registered in GCS, and the VM can only reach the Mac through UTM's shared-network NAT. If the Wi-Fi IP changes, restart both nodes with the new IP.
- **VM worker environment**: workers inherit the raylet's env, so `PYTHONPATH=/home/ashley/ray-shared` (copy of `tracing_setup.py` + `requirements.txt`) and `OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.0.14:4318` must be set when starting the VM raylet — VM-side `localhost:4318` has no Jaeger.
- **SSH**: `Host kali` in `~/.ssh/config` → `ashley@192.168.64.3` (UTM shared network), key auth set up.
- **Sandboxed shells** (Claude Code Bash tool): IP auto-detection returns 127.0.0.1, so drivers fail with a "Some processes ... have not registered with GCS" retry loop. Set `RAY_NODE_IP_ADDRESS=10.0.0.14` and `RAY_ADDRESS=10.0.0.14:6379` when running drivers from the sandbox.
- Jaeger (Docker/colima) publishes 4318 on 0.0.0.0, so the VM reaches it via either 10.0.0.14 or 192.168.64.1.
