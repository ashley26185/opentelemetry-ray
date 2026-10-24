# Tracing a distributed Ray cluster with OpenTelemetry → Jaeger

This guide explains how to get end-to-end tracing in a **multi-node Ray
cluster** (driver + workers on different machines), distilled from what it
actually took to make it work here: the mechanism, the exact commands, and
every failure mode hit along the way. The concrete implementation lives in
`tracing_setup.py` (the hook), `main.py` (the jobs), and
`validate_traces.py` (the checks).

## What Ray tracing looks like

Ray's tracing is built on a single idea: **one OpenTelemetry startup hook
that runs once in every process** — the driver and every worker on every
node. The hook's job is just to install a global `TracerProvider` with an
exporter (here: OTLP/HTTP straight to Jaeger's built-in receiver; no
collector, no agent).

On top of that hook, Ray instruments every `@ray.remote` call itself:

```
driver span (custom root, opened by you)
└── task_fn ray.remote          PRODUCER span   (in the calling process)
    └── task_fn ray.remote_worker  CONSUMER span (in the worker process)
        └── your custom child span               (opened inside the function)
```

The PRODUCER span is created in the caller, the CONSUMER span in the worker,
and the trace context is propagated between them automatically (Ray passes
it through a `_ray_trace_ctx` argument under the hood). Any span you open
inside a task function nests under the CONSUMER span. Actors and
`ray.util.multiprocessing.Pool` (whose internal `PoolActor` is itself
`@ray.remote`) are traced the same way with zero extra wiring.

## The one fact everything else follows from

**The hook string lives in GCS, and only the process that *creates* the
cluster writes it there.** Every process that connects — workers on all
nodes *and* drivers — reads the hook from GCS and runs it at connect time.

| Who creates the cluster | How the hook gets into GCS |
| --- | --- |
| The driver (`ray.init()` with no cluster running) | `ray.init(_tracing_startup_hook="module:func")` |
| `ray start --head` (a real cluster) | `ray start --head --tracing-startup-hook=module:func` |

Consequences, learned the hard way:

- A driver that **connects** to an existing cluster with
  `ray.init(_tracing_startup_hook=...)` has its kwarg **silently ignored**.
  The demo runs fine and produces **zero spans, with no warning**. If your
  cluster was started with `ray start`, the flag must go on the **head**.
- The hook module must be importable in **every** process, on every node:
  driver, head workers, remote workers. Workers inherit the raylet's
  environment, so put the module's directory in `PYTHONPATH` (or install the
  module into the venv) **when starting each raylet** — setting it only in
  the driver's shell does nothing for workers.
- Ray's task instrumentation is only switched on when a hook is found in
  GCS. No hook → no PRODUCER/CONSUMER spans either.

## The hook

`tracing_setup.py` — run once per process, it must:

1. Build the resource with **`service.name` and `host.name`**. `host.name`
   is what lets you prove a trace actually flowed through specific machines
   (Jaeger shows resource attributes as process tags).
2. Install the provider with `SimpleSpanProcessor` — deliberately. Ray
   never calls `provider.shutdown()`, and worker processes can be killed
   when the cluster stops, so a `BatchSpanProcessor` can silently drop the
   last spans of a short run.
3. Construct the OTLP exporter **without** an explicit `endpoint=`. In
   `opentelemetry-exporter-otlp >= 1.36`, a bare `http://host:4318` is used
   as-is and 404s; passing nothing lets the SDK resolve
   `OTEL_EXPORTER_OTLP_ENDPOINT` (or default to
   `http://localhost:4318/v1/traces`).

```python
def setup_tracing() -> None:
    provider = TracerProvider(
        resource=Resource.create({
            "service.name": os.environ.get("OTEL_SERVICE_NAME", "ray-otel-demo"),
            "host.name": socket.gethostname(),
        })
    )
    provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
```

## Versions: everything must match exactly

Ray requires the **identical Ray version and the identical Python version
(patch level included)** on every node — `3.13.13` vs `3.13.12` is a hard
`RuntimeError: Version mismatch` when the worker joins. Ray's docs pin
`opentelemetry-*==1.1.0`, which is stale (from 2021, and incompatible with
Python 3.13); **1.45.1** is verified working with Ray 2.47.1 / Python
3.13.13. Pin both sides from the same `requirements.txt` — the demo's
Kali VM venv was built from the repo's file verbatim.

## Step by step: two-node cluster

Assumptions: head node on host A (the Mac, `10.0.0.14`), worker node on
host B (the Kali VM, `192.168.64.3`), Jaeger with its OTLP/HTTP receiver
published on host A (`:4318`). Same Ray/Python/OTel versions everywhere.

**1. Start the head — with the hook flag:**

```bash
# On host A, from the repo root so head workers can import the hook
PYTHONPATH="$PWD" .venv/bin/ray start --head --port=6379 \
  --node-ip-address=10.0.0.14 \
  --resources='{"head": 1}' \
  --tracing-startup-hook=tracing_setup:setup_tracing \
  --disable-usage-stats
```

Two details matter:

- **`--node-ip-address`**: the head must advertise an address that (a) the
  worker nodes can reach, and (b) matches the IP a driver auto-detects from
  its default route — Ray matches the driver's detected node IP against
  registered nodes, and a mismatch means drivers fail with
  *"Some processes that the driver needs to connect to have not registered
  with GCS"* in a retry loop. On a multi-homed host, pick the primary
  interface's IP. (This demo's VM can reach the Mac's Wi-Fi IP through the
  UTM shared-network NAT, so one IP serves both sides.)
- **`--resources`**: node labels like `{"head": 1}` / `{"kali": 1}` let
  tasks be pinned to specific nodes (`options(resources={"kali": 0.001})`),
  which makes it easy to guarantee a trace touches a particular machine.

**2. Start the worker — with the hook importable and the exporter reachable:**

```bash
# On host B; workers inherit this raylet's environment
PYTHONPATH=/home/ashley/ray-shared \
OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.0.14:4318 \
ray start --address=10.0.0.14:6379 --resources='{"kali": 1}' \
  --disable-usage-stats
```

- `PYTHONPATH` must point at a directory containing the hook module (a copy
  of the repo's `tracing_setup.py` here).
- **`OTEL_EXPORTER_OTLP_ENDPOINT` must point at the collector from the
  worker's point of view** — a worker's `localhost` has no Jaeger on it.
  Setting it in the driver's environment does not reach workers; it must be
  in the **raylet's** environment at `ray start` time (workers inherit it).
- No hook flag on workers — they read it from GCS.

**3. Run a driver:**

```bash
# Any host that can reach the head; auto-connects to the cluster
PYTHONPATH="$PWD" .venv/bin/python main.py
```

When connecting to an existing cluster, drivers get the hook from GCS
automatically — no kwarg needed (and a kwarg would be ignored anyway).
The `_tracing_startup_hook` kwarg is only for single-node mode, where the
driver creates the cluster and must store the hook itself.

## Per-job / per-service names

`service.name` is a resource attribute — it is fixed per **process** at
provider-creation time. Two facts shape everything:

1. **`set_tracer_provider()` is once-per-process.** OpenTelemetry ignores
   later calls with *"Overriding of current TracerProvider is not allowed"*.
   You cannot swap services mid-process, so per-job services mean **one
   driver process per job**, each setting `OTEL_SERVICE_NAME` **before**
   `ray.init()`:

   ```bash
   PYTHONPATH="$PWD" .venv/bin/python main.py job-1   # service ray-otel-job-1
   PYTHONPATH="$PWD" .venv/bin/python main.py job-2   # service ray-otel-job-2
   ```

2. **Workers get their name from `runtime_env` env vars** — Ray applies
   `runtime_env={"env_vars": {"OTEL_SERVICE_NAME": ...}}` to a worker
   process *before* the tracing hook runs (verified against Ray 2.47.1),
   and it pools workers per runtime env, so different jobs never share
   workers:

   ```python
   fetch_data.options(
       resources={"kali": 0.001},
       runtime_env={"env_vars": {"OTEL_SERVICE_NAME": "ray-otel-job-1"}},
   ).remote(i)
   ```

   Keep the worker-side name equal to the driver's, and the entire trace —
   driver, PRODUCER, CONSUMER, custom children, on every node — lands under
   one uniform service.

Without per-job names, one service for the whole cluster is the normal
arrangement; distinguish workloads by root span names instead.

## Verifying the traces actually flowed through both nodes

Add `host.name` to the resource (above), run the jobs, then check Jaeger's
query API — `validate_traces.py` does exactly this:

1. Find each job's latest trace (`/api/traces?service=...&operation=...`).
   **Jaeger does not return newest-first** — pick the trace with the
   greatest `startTime`, not the first match, or an older run of the same
   job will fool you.
2. Assert every process in the trace carries the expected `service.name`
   (uniform-service check catches leakage).
3. Assert `host.name` process tags include both the head's hostname and the
   worker's (`kali`) — this is what proves the trace ran through the
   distributed cluster, not just the local node.
4. Assert the expected custom child spans are present and nested.

A healthy job-1 trace looks like:

```
job-1-task-actor                          [mac]   driver root
├── fetch_data ray.remote                 [mac]   PRODUCER (driver)
│   └── fetch_data ray.remote_worker      [kali]  CONSUMER (VM worker)
│       └── fetch_data.db_lookup          [kali]  custom child
└── Processor.process ray.remote          [mac]
    └── Processor.process ray.remote_worker [kali]
        └── Processor.process.transform    [kali]
```

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Everything runs, **zero spans**, no errors | Hook is not in GCS: cluster was started with plain `ray start` | `--tracing-startup-hook=...` on the **head**, restart worker nodes |
| Driver warns `Failed to run tracing startup hook` | Hook module not importable in that process | `PYTHONPATH` on the raylet env / install the module into every venv |
| Worker join fails: `Version mismatch ... Python: 3.13.13 vs 3.13.12` | Patch-level Python mismatch | Same exact Python on every node (rebuild venvs) |
| Driver loops: `Some processes ... have not registered with GCS` | Driver's auto-detected node IP matches no registered node | `--node-ip-address=<primary IP>` on the head, or set `RAY_NODE_IP_ADDRESS` for the driver |
| `Overriding of current TracerProvider is not allowed` | `set_tracer_provider` called twice in one process | By design — one service per process; use separate driver processes per service |
| Export errors `404` | `endpoint=` passed without `/v1/traces` path | Construct `OTLPSpanExporter()` with no endpoint; use env vars |
| Worker spans missing on one node | Exporter endpoint unreachable from that node (its `localhost` has no Jaeger) | `OTEL_EXPORTER_OTLP_ENDPOINT` to the collector's reachable address in the **raylet** env |
| `cannot pickle '_thread.lock' object` | Module-level `tracer` captured in a remote function's closure | `trace.get_tracer(__name__)` inside each function |
| Last spans of a run missing | `BatchSpanProcessor` and Ray never shuts the provider down | `SimpleSpanProcessor` for demo runs |
| Traces appear under two services | Driver and workers got different `OTEL_SERVICE_NAME` | Set it in the driver env *and* via `runtime_env` env vars; keep them equal |

## FAQ

**Do worker nodes need `--tracing-startup-hook` on their `ray start`?**

No — only the head takes the flag. The head stores the hook string in GCS,
and every process that later connects (drivers, and workers on *all* nodes)
reads it from there and runs it locally at its own connect time:

```
head: ray start --tracing-startup-hook=tracing_setup:setup_tracing
        └─ writes "tracing_setup:setup_tracing" into GCS     (only the head writes GCS)

worker node: ray start --address=...                          (no hook flag)
        └─ each worker process, at connect time:
             1. reads the hook string from GCS
             2. imports tracing_setup and runs setup_tracing() in its own process
```

The flag is not just unnecessary on workers — it is a no-op there, because
only the cluster-creating process writes that GCS key. What worker nodes
*do* need is whatever the hook requires to actually run on them:

- the hook module importable on that node (`PYTHONPATH` on the worker's
  `ray start`), and
- an exporter endpoint reachable from that node (`OTEL_EXPORTER_OTLP_ENDPOINT`
  in the worker raylet's environment — a worker's `localhost` has no Jaeger).

**Why set `OTEL_EXPORTER_OTLP_ENDPOINT` at `ray start` time instead of in the driver?**

Because workers inherit the *raylet's* environment, not the driver's — the
driver never spawns worker processes (the raylet does). The hook runs in
each worker process and reads env vars from whatever environment that
process was born with, so per-node settings like the hook module path and
the exporter endpoint belong on that node's `ray start` command line, not
in the driver's shell.

**The hook sets `service.name` — why doesn't `ray start` freeze it at cluster-start time? Does each job start new processes on the head?**

`ray start --tracing-startup-hook=...` only stores the hook's *name* in GCS —
the hook code does not execute at start time. The `service.name` in
`tracing_setup.py` is an expression (`os.environ.get("OTEL_SERVICE_NAME", ...)`)
evaluated later, inside each process, when it connects — against *that
process's* environment:

- The raylet/GCS processes from `ray start` never run the hook and produce
  no spans; they only spawn workers on demand.
- The driver's service comes from its own environment: `main.py job-1` sets
  `OTEL_SERVICE_NAME` before `ray.init()`, and the hook reads it when it runs
  in the driver process.
- Head and Kali workers get the per-job name from the `runtime_env` env vars
  carried by the task: the raylet applies them to the worker process *before*
  the hook runs, so the hook sees `OTEL_SERVICE_NAME=ray-otel-job-1` in that
  new process's environment. The `ray start` environment only supplies
  defaults (`PYTHONPATH`, exporter endpoint) — never `OTEL_SERVICE_NAME`.

And yes — each job starts new processes on every node it uses, the head
included. Ray pools workers per runtime_env, so job-1's tasks spawn one set
of worker processes and job-2's a separate set, each running the hook fresh
at connect time. "Tracing is initialized once per process" is per *worker
process*, not per node or per cluster:

```
ray start --head --tracing-startup-hook=...   ← stores "tracing_setup:setup_tracing" in GCS only
main.py job-1 (new driver process on the Mac)
  └─ hook runs in the driver, reads its own env  →  service ray-otel-job-1
task arrives with runtime_env env_vars {OTEL_SERVICE_NAME: ray-otel-job-1}
  └─ head raylet spawns a NEW worker process with that env → hook → ray-otel-job-1
  └─ kali raylet spawns a NEW worker process with that env → hook → ray-otel-job-1
```

A job that did *not* carry the env var would get workers under the default
`ray-otel-demo` — the "traces appear under two services" row above.
