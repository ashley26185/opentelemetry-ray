# Ray + OpenTelemetry → Jaeger demo

A minimal project to test OpenTelemetry tracing with Ray, exporting traces to a
local Jaeger running in Docker.

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

`main.py` starts a local Ray cluster and runs remote tasks and an actor:

```
driver-root                          (driver, custom span)
├── fetch_data ray.remote            (driver, Ray PRODUCER span)
│   └── fetch_data ray.remote_worker (worker, Ray CONSUMER span)
│       └── fetch_data.db_lookup     (worker, custom child span)
├── … 4 more fetch_data tasks …
└── Processor.process ray.remote     (driver, PRODUCER)
    └── Processor.process ray.remote_worker   (worker, CONSUMER)
        └── Processor.process.transform       (worker, custom child)
```

All spans land in a single trace — Ray propagates the trace context from the
driver into tasks/actors via a `_ray_trace_ctx` keyword argument.

## Prerequisites

- Python 3.13.13 (any way you like; the commands below assume `uv` is available,
  plain `venv` works too)
- Docker. On macOS without Docker Desktop, [colima](https://github.com/abiosoft/colima)
  provides a CLI-only Docker runtime:
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

## Run

```bash
# 2. Start Jaeger (UI on 16686, OTLP/gRPC on 4317, OTLP/HTTP on 4318)
docker run -d --name jaeger \
  -p 16686:16686 -p 4317:4317 -p 4318:4318 \
  jaegertracing/all-in-one:latest

# 3. Run the demo (from this directory, so Ray workers can import tracing_setup)
PYTHONPATH="$PWD" .venv/bin/python main.py
```

Open **http://localhost:16686** in a browser, select service `ray-otel-demo`
from the dropdown, and click **Find Traces**.

## Configuration

| Env var                        | Default               | Purpose                          |
| ------------------------------ | --------------------- | -------------------------------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`  | `http://localhost:4318` | Base URL of the OTLP collector (SDK appends `/v1/traces`) |
| `OTEL_SERVICE_NAME`            | `ray-otel-demo`       | Service name shown in Jaeger     |

These are read inside the tracing hook, so they must be set in the environment
the driver runs in (workers inherit them, and run the hook themselves).

## Files

- `tracing_setup.py` — the OpenTelemetry startup hook
  (`tracing_setup:setup_tracing`). Installs the global `TracerProvider` with an
  OTLP/HTTP exporter. Runs once per process (driver + every worker).
- `main.py` — the demo driver: `ray.init(_tracing_startup_hook=...)`, a custom
  root span, traced remote tasks and an actor.
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
