# Ray + OpenTelemetry → Jaeger demo

A minimal project to test OpenTelemetry tracing with Ray, exporting traces to a
local Jaeger running in Docker.

No OpenTelemetry Collector and no environment variables are needed for the
default local setup: spans are sent straight from Ray's driver/worker processes
to Jaeger's built-in OTLP receiver over HTTP.

```
driver + Ray workers ──OTLP/HTTP POST──▶ Jaeger all-in-one (Docker)
                                        ├─ OTLP/HTTP receiver :4318
                                        ├─ OTLP/gRPC receiver :4317
                                        └─ Jaeger UI          :16686
```

## Verified versions

| Component                     | Version   |
| ----------------------------- | --------- |
| Python                        | 3.13.13   |
| Ray                           | 2.47.1    |
| opentelemetry-api / -sdk / -exporter-otlp | 1.45.1 |
| Jaeger (all-in-one container) | latest    |

### Version notes

- The Ray 2.47.1 docs page
  [“Tracing”](https://docs.ray.io/en/releases-2.47.1/ray-observability/user-guides/ray-tracing.html)
  pins `opentelemetry-*==1.1.0` — that pin is stale (OTel 1.1.0 is from 2021 and
  not compatible with Python 3.13). The working pin for this setup is **1.45.1**
  (`requirements.txt`).
- `ray.init(_tracing_startup_hook=...)` is still accepted in Ray 2.47.1, but it
  no longer appears in `ray.init()`'s signature — it is popped from `**kwargs`
  internally. Ray stores the hook string in GCS, and each worker process (and
  the driver) imports it as `module:attribute` and runs it on connect; Ray also
  enables its task/actor span instrumentation automatically at that point
  (see `ray/_private/worker.py:connect()` and
  `ray/util/tracing/tracing_helper.py`).
- In `opentelemetry-exporter-otlp >= 1.36`, passing `endpoint=` explicitly uses
  the URL **as-is** — a bare `http://localhost:4318` results in a `404` from
  Jaeger. This project constructs the exporter without an endpoint, so the SDK
  resolves `OTEL_EXPORTER_OTLP_ENDPOINT` (or falls back to
  `http://localhost:4318/v1/traces`), which is exactly Jaeger's OTLP/HTTP port.

## What the demo does

`main.py` submits two jobs. With no cluster running it starts a local
single-node one; with a cluster up (see below) it connects to it and pins
each job's work to both nodes via the `head`/`kali` resources.

Job 1 — task/actor pipeline (root span `job-1-task-actor`):

```
job-1-task-actor                      (driver, custom span)
├── fetch_data ray.remote             (driver, Ray PRODUCER span — 3 tasks pinned to head, 3 to kali)
│   └── fetch_data ray.remote_worker  (worker, Ray CONSUMER span)
│       └── fetch_data.db_lookup      (worker, custom child span)
└── Processor.process ray.remote      (driver, PRODUCER — actor pinned to kali)
    └── Processor.process ray.remote_worker   (worker, CONSUMER)
        └── Processor.process.transform       (worker, custom child)
```

Job 2 — `ray.util.multiprocessing.Pool` pipeline (root span `job-2-pool`):
one Pool pinned to the head and one to kali — Ray's drop-in replacement for
the stdlib `multiprocessing.Pool`, backed by a small round-robin pool of
`@ray.remote` actors instead of OS processes:

```
job-2-pool                            (driver, custom span)
├── PoolActor.run_batch ray.remote    (driver, PRODUCER — Pool pinned to head)
│   └── PoolActor.run_batch ray.remote_worker   (worker, CONSUMER)
│       └── scale_item                (worker, custom child, one per batch item)
└── … second Pool on kali …
```

All spans land in a single trace — Ray propagates the trace context from the
driver into tasks/actors via a `_ray_trace_ctx` keyword argument.

The pool path traces automatically with no extra wiring: Ray's tracing hook
instruments *every* `@ray.remote`-decorated class, including its own
internal `PoolActor` (see `ray/actor.py:_make_actor`, which unconditionally
calls `_inject_tracing_into_class` at decoration time — the actual
enable/disable check happens lazily per-call against a global flag, so it
doesn't matter that `PoolActor` was decorated long before `ray.init()` runs).
`pool.map()`'s own `.remote()` call becomes the PRODUCER/CONSUMER span pair,
and a plain `tracer.start_as_current_span(...)` inside the mapped function
(`scale_item`) nests under the CONSUMER span exactly like `fetch_data.
db_lookup` and `Processor.process.transform` do above.

One wrinkle specific to `Pool`: `map()` batches multiple items into a single
`run_batch.remote()` call (size controlled by `chunksize`), so one PRODUCER/
CONSUMER span pair can cover several logical work items — that's why
`scale_item` opens its own child span per item rather than relying on the
CONSUMER span alone to identify individual items.

## Prerequisites

- **Python 3.13.13** (any way you like; the commands below assume `uv` is
  available, plain `venv` works too)
- **Docker** — any Docker runtime that exposes container ports on localhost:
  - macOS with **Docker Desktop**: install it and start the app.
  - macOS with **colima** (CLI-only, no GUI):
    ```bash
    brew install colima docker
    colima start --cpu 2 --memory 4
    ```

## Setup (one time)

```bash
cd opentelemetry-ray

# 1. Create an isolated environment with Python 3.13.13 and install pinned deps
uv venv .venv --python 3.13.13
uv pip install --python .venv/bin/python -r requirements.txt

# (without uv:)
#   python3.13 -m venv .venv
#   .venv/bin/pip install -r requirements.txt
```

## Start Jaeger

The `jaegertracing/all-in-one` image bundles the Jaeger collector, query
service, and UI in one container — and its collector has an **OTLP receiver
built in**, so no separate OpenTelemetry Collector is required:

- `16686` — Jaeger UI
- `4317`  — OTLP/gRPC receiver
- `4318`  — OTLP/HTTP receiver

```bash
docker run -d --name jaeger \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/all-in-one:latest
```

Verify it's up:

```bash
docker ps --filter name=jaeger
curl -s http://localhost:16686/api/services   # JSON with "data": [] means ready
```

Managing the container later:

```bash
docker stop jaeger     # pause
docker start jaeger    # resume
docker rm -f jaeger    # remove the container entirely
```

## Run the demo

```bash
# From this directory, so Ray workers can import tracing_setup
PYTHONPATH="$PWD" .venv/bin/python main.py
```

For per-job `service.name`s, run each job as its own driver process
(`set_tracer_provider` is once-per-process, so one process cannot switch
services between jobs):

```bash
PYTHONPATH="$PWD" .venv/bin/python main.py job-1   # all spans: ray-otel-job-1
PYTHONPATH="$PWD" .venv/bin/python main.py job-2   # all spans: ray-otel-job-2
```

With a cluster running (see next section), the same command connects to it
instead of starting a local one.

Then open **http://localhost:16686**, select service `ray-otel-demo` from the
dropdown, and click **Find Traces**.

## Run on the two-node cluster (Mac head + Kali VM worker)

The same `main.py` also runs against a Ray cluster spanning the Mac (head)
and the Kali VM (worker), pinning half of each job's work to each node so
every trace contains spans from both. The Kali VM sits behind UTM's shared
network (192.168.64.x) but can reach the Mac's main IP; the head must
advertise an address both the VM workers and Mac drivers can reach.

Start the head (Mac, from the project directory so workers can import
`tracing_setup`):

```bash
PYTHONPATH="$PWD" .venv/bin/ray start --head --port=6379 \
  --node-ip-address=10.0.0.14 --resources='{"head": 1}' \
  --tracing-startup-hook=tracing_setup:setup_tracing --disable-usage-stats
```

Start the worker (Kali VM; `~/.ssh/config` maps `Host kali` to
`ashley@192.168.64.3`, the VM's UTM shared-network address). The VM's venv
(`~/ray-venv`, Python 3.13.13 + Ray 2.47.1 + the pinned OpenTelemetry
packages) is built once with:

```bash
ssh kali '~/.local/bin/uv venv --seed --python 3.13.13 ~/ray-venv'
ssh kali 'mkdir -p ~/ray-shared'
scp requirements.txt tracing_setup.py kali:~/ray-shared/
ssh kali '~/ray-venv/bin/pip install -r ~/ray-shared/requirements.txt'
```

Then start the worker raylet with the hook module on `PYTHONPATH` and the
OTLP exporter pointed at the Mac's Jaeger (VM-side `localhost` would not
find it — Jaeger runs on the Mac):

```bash
ssh kali 'PYTHONPATH=/home/ashley/ray-shared \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.0.14:4318 \
  nohup ~/ray-venv/bin/ray start --address=10.0.0.14:6379 \
  --resources='\''{"kali": 1}'\'' --disable-usage-stats >/tmp/ray-worker.log 2>&1 &'
```

Notes:

- **Exact version matching.** Ray requires the identical Ray AND Python
  version (patch level included) on every node — 2.47.1 / 3.13.13 here.
- **Tracing hook lives in GCS.** `ray start --head --tracing-startup-hook`
  stores the hook string in GCS, and every process that connects (workers on
  both nodes *and* the driver) reads and runs it from there. A connecting
  driver's `_tracing_startup_hook` kwarg is **ignored** against an existing
  cluster — it only matters in single-node mode, where `ray.init()` creates
  the head itself. Without the flag on `ray start`, the demo runs fine but
  produces no spans.
- **Node resources.** `--resources='{"head": 1}'` and `{"kali": 1}` give the
  jobs stable labels to pin work by; `main.py` falls back to unpinned
  scheduling when they are absent.
- **Env vars on the VM.** Workers inherit the raylet's environment, so the
  `PYTHONPATH` and `OTEL_EXPORTER_OTLP_ENDPOINT` set at `ray start` time are
  what make the tracing hook importable and its exporter reach Jaeger.
- **Driver node IP.** On multi-homed hosts the driver's auto-detected IP
  must match a node registered in GCS. It normally does (both are the Mac's
  primary IP); if not, set `RAY_NODE_IP_ADDRESS` for the driver.

Run both jobs and validate:

```bash
PYTHONPATH="$PWD" .venv/bin/python main.py job-1   # all spans: ray-otel-job-1
PYTHONPATH="$PWD" .venv/bin/python main.py job-2   # all spans: ray-otel-job-2
.venv/bin/python validate_traces.py
```

`validate_traces.py` asks Jaeger for the latest `job-1-task-actor` and
`job-2-pool` traces and verifies each contains spans whose `host.name`
process tag is both the Mac's hostname and `kali`.

## How spans reach Jaeger (no env vars, no collector)

- The startup hook (`tracing_setup.py`) builds `OTLPSpanExporter()` **without
  an endpoint**, so the OpenTelemetry SDK uses its built-in default
  `http://localhost:4318/v1/traces` — Jaeger's OTLP/HTTP receiver. Nothing has
  to be exported or configured for the local default to work.
- Optional overrides (standard OpenTelemetry env vars):

  | Env var                        | Default               | Purpose                          |
  | ------------------------------ | --------------------- | -------------------------------- |
  | `OTEL_EXPORTER_OTLP_ENDPOINT`  | `http://localhost:4318` | Base URL of the OTLP receiver (SDK appends `/v1/traces`) |
  | `OTEL_SERVICE_NAME`            | `ray-otel-demo`       | Service name shown in Jaeger     |

  They are read inside the tracing hook, so they must be set in the environment
  the driver runs in (workers inherit them, and run the hook themselves).

**Service name vs job identity.** By default every span carries
`service.name = ray-otel-demo` and the jobs are told apart by their root
span names. In the per-job modes (`main.py job-1` / `main.py job-2`), the
whole trace — driver, head workers, Kali workers — lands under one service:

- **Driver**: the script sets `OTEL_SERVICE_NAME` before `ray.init()`, so
  the tracing hook names the driver's provider.
- **Workers**: every remote call carries the same name in a `runtime_env`
  env var, which Ray applies to worker processes *before* the hook runs
  (verified against Ray 2.47.1). Ray also pools workers per runtime env, so
  the two jobs never share worker processes.

This is why the jobs are separate driver processes rather than one script
switching names: OpenTelemetry's `set_tracer_provider` is once-per-process
by design, so a mid-process swap is impossible.

## Files

- `tracing_setup.py` — the OpenTelemetry startup hook
  (`tracing_setup:setup_tracing`). Installs the global `TracerProvider` with an
  OTLP/HTTP exporter and a resource carrying `service.name` + `host.name` (the
  latter is what `validate_traces.py` uses to attribute spans to nodes). Runs
  once per process (driver + every worker).
- `main.py` — the demo driver: `ray.init(_tracing_startup_hook=...)`, two
  jobs (a task/actor pipeline and a `Pool` pipeline), each pinning work to
  the head and Kali nodes when the `head`/`kali` resources exist.
- `validate_traces.py` — queries the Jaeger API and checks both jobs' traces
  contain spans from the head and from Kali.
- `TRACING.md` — the full guide to tracing a distributed Ray cluster: the
  mechanism, per-node setup, per-job service names, and troubleshooting.
- `requirements.txt` — pinned dependencies.

## Troubleshooting

- **`Failed to export spans batch code: 404`** — the exporter is posting to a
  URL without the `/v1/traces` signal path. Don't pass a bare base URL as
  `endpoint=` to `OTLPSpanExporter`; pass none (env-var resolution appends the
  path) or include the full path.
- **`cannot pickle '_thread.lock' object`** — a module-level `tracer` global is
  being captured in a remote function's closure. `Tracer` objects aren't
  picklable; call `trace.get_tracer(__name__)` inside each function instead.
- **Spans missing after a short run** — this project deliberately uses
  `SimpleSpanProcessor` (export at span end). If you switch to
  `BatchSpanProcessor`, note that Ray never calls `provider.shutdown()`, so the
  last batch can be dropped when the cluster exits.
