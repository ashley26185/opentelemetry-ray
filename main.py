"""Ray + OpenTelemetry demo exporting traces to a local Jaeger.

Two jobs are submitted to a Ray cluster. In single-node mode (``python
main.py`` with no cluster running) they run on the local node; on the
two-node cluster (Mac head + Kali VM worker, see README) each job pins work
to BOTH nodes so every trace contains spans from the head and from Kali.

Span structure produced by one run:

    job-1-task-actor                      (driver, custom root span)
    ├── fetch_data ray.remote             (driver, Ray PRODUCER span, 3× on head)
    │   └── fetch_data ray.remote_worker  (worker, Ray CONSUMER span)
    │       └── fetch_data.db_lookup      (worker, custom child span)
    ├── fetch_data ray.remote             (driver, Ray PRODUCER span, 3× on kali)
    │   └── ...                           (same shape, produced on the VM)
    └── Processor.process ray.remote      (driver, PRODUCER, actor pinned to kali)
        └── Processor.process ray.remote_worker  (worker, CONSUMER, on kali)
            └── Processor.process.transform       (worker, custom child, on kali)

    job-2-pool                            (driver, custom root span)
    ├── PoolActor.run_batch ray.remote              (driver, PRODUCER, Pool pinned to head)
    │   └── PoolActor.run_batch ray.remote_worker   (worker, CONSUMER, on head)
    │       └── scale_item                          (worker, custom child, on head)
    └── ... second Pool pinned to kali ...          (same shape, on the VM)

Per-job ``service.name``: run the jobs as separate driver processes — each
invocation sets ``OTEL_SERVICE_NAME`` *before* ``ray.init()`` so the tracing
hook names the driver's spans, and every remote call carries the same name
in a ``runtime_env`` env var, which Ray applies to worker processes *before*
the hook runs (Ray also pools workers per runtime env, so the jobs never
share workers):

    python main.py job-1   # all spans under service ray-otel-job-1
    python main.py job-2   # all spans under service ray-otel-job-2

``python main.py`` with no argument runs both jobs in one driver under the
default service ``ray-otel-demo``. A provider swap inside one process is NOT
possible: OpenTelemetry's ``set_tracer_provider`` is once-per-process by
design, hence the separate driver processes.

``ray.util.multiprocessing.Pool`` is just a round-robin pool of plain
``@ray.remote`` actors (``PoolActor``) under the hood, so it is traced the
same way as the explicit task/actor calls above: no extra wiring needed. The
one wrinkle is that ``Pool.map`` batches several items into a single
``run_batch.remote()`` call (controlled by ``chunksize``), so one PRODUCER/
CONSUMER span pair can cover many logical work items — see ``scale_item``
below for how to still get a per-item child span out of that.
"""

import os
import sys
import time

import ray
from opentelemetry import trace
from ray.util.multiprocessing import Pool

JAEGER_UI = "http://localhost:16686"
DEFAULT_SERVICE = "ray-otel-demo"
JOB_SERVICES = {
    "job-1": "ray-otel-job-1",
    "job-2": "ray-otel-job-2",
}

# Note: don't keep the Tracer in a module-level global that remote functions
# capture in their closure — Tracer objects are not picklable. Get a fresh
# one inside each function (Ray workers resolve it against the global
# TracerProvider installed by the hook).
tracer = trace.get_tracer(__name__)


def job_env(service: str) -> dict:
    """Runtime env naming the service for a job's worker processes.

    Ray applies ``runtime_env`` env vars to worker processes *before* the
    tracing hook runs, and pools workers per runtime env — so every remote
    call carrying this env runs in workers whose TracerProvider was born
    with this ``service.name``, on the head and the Kali node alike. Must
    match the driver's own service name to keep the whole trace under one
    service.
    """
    return {"runtime_env": {"env_vars": {"OTEL_SERVICE_NAME": service}}}


def pin(node: str) -> dict:
    """Scheduling resources that pin a task/actor to one cluster node.

    The head node is started with ``--resources='{"head": 1}'`` and the Kali
    worker with ``--resources='{"kali": 1}'``. Returns an empty dict when the
    resource is absent (e.g. a plain single-node cluster), in which case Ray
    schedules the task anywhere.
    """
    if node in ray.cluster_resources():
        return {node: 0.001}
    return {}


def on(node: str) -> dict:
    """``@ray.remote(...).options()`` kwargs that pin work to a node, or ``{}``."""
    resources = pin(node)
    return {"resources": resources} if resources else {}


@ray.remote
def fetch_data(item_id: int) -> dict:
    """Leaf task: does a (pretend) database lookup with its own child span."""
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("fetch_data.db_lookup") as span:
        span.set_attribute("item_id", item_id)
        time.sleep(0.15)
    return {"item_id": item_id, "value": item_id * 2}


@ray.remote
class Processor:
    """Actor whose method runs a (pretend) transformation."""

    def process(self, data: dict) -> dict:
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span("Processor.process.transform") as span:
            span.set_attribute("item_id", data["item_id"])
            time.sleep(0.2)
        return {**data, "processed_by": self.__class__.__name__}


def scale_item(item: dict) -> dict:
    """Function run inside the pool's actor process, one call per batch item.

    ``Pool`` ships this function to the (already-remote) ``PoolActor`` as
    plain cloudpickled data and calls it directly inside ``run_batch`` — it
    is never itself wrapped with ``@ray.remote``. The CONSUMER span Ray
    opens around ``run_batch`` is still the "current span" while this runs,
    so a plain nested span here still shows up as its child, even though the
    whole batch (potentially several items) shares that one CONSUMER span.
    """
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("scale_item") as span:
        span.set_attribute("item_id", item["item_id"])
        time.sleep(0.1)
    return {**item, "scaled_by": "pool"}


def job_task_actor(service: str) -> None:
    """Job 1: fetch_data tasks split across head and Kali, actor on Kali."""
    with tracer.start_as_current_span("job-1-task-actor"):
        # Half the fetch_data tasks on the head node, half on the Kali VM.
        refs = [
            fetch_data.options(**on("head"), **job_env(service)).remote(i)
            for i in range(3)
        ]
        refs += [
            fetch_data.options(**on("kali"), **job_env(service)).remote(i)
            for i in range(3, 6)
        ]
        results = ray.get(refs)
        print(f"job 1 tasks done: {results}")

        processor = Processor.options(**on("kali"), **job_env(service)).remote()
        processed = ray.get(processor.process.remote(results[0]))
        print(f"job 1 actor done: {processed}")


def job_pool(service: str) -> None:
    """Job 2: one Pool pinned per node — items scaled on the head and on Kali."""
    with tracer.start_as_current_span("job-2-pool"):
        items = [{"item_id": i} for i in range(4)]

        def run(node: str) -> list:
            ray_remote_args = {**on(node), **job_env(service)}
            with Pool(processes=2, ray_remote_args=ray_remote_args) as pool:
                return pool.map(scale_item, items)

        scaled_head = run("head")
        print(f"job 2 head pool done: {scaled_head}")
        scaled_kali = run("kali")
        print(f"job 2 kali pool done: {scaled_kali}")


def main() -> None:
    job = sys.argv[1] if len(sys.argv) > 1 else "all"
    if job not in ("all", *JOB_SERVICES):
        sys.exit(f"usage: {sys.argv[0]} [all|{'|'.join(JOB_SERVICES)}] (got {job!r})")

    # Per-job mode: name the driver's service BEFORE ray.init — the tracing
    # hook reads OTEL_SERVICE_NAME when it installs the driver's provider.
    # (The provider can never be swapped afterwards: OpenTelemetry's
    # set_tracer_provider is once-per-process by design.)
    if job in JOB_SERVICES:
        os.environ["OTEL_SERVICE_NAME"] = JOB_SERVICES[job]
    service = os.environ.get("OTEL_SERVICE_NAME", DEFAULT_SERVICE)

    # Connect to a running cluster when one is available (``auto``), or start
    # a local single-node one otherwise. ``RAY_NODE_IP_ADDRESS`` is for
    # drivers on multi-homed hosts whose auto-detected IP does not match any
    # cluster node; it can usually be left unset. The hook installs the
    # global TracerProvider in the driver AND in every worker process, and
    # enables Ray's task/actor tracing (feature flag).
    ray.init(
        address=os.environ.get("RAY_ADDRESS", "auto"),
        _node_ip_address=os.environ.get("RAY_NODE_IP_ADDRESS"),
        _tracing_startup_hook="tracing_setup:setup_tracing",
    )

    if job in ("all", "job-1"):
        job_task_actor(service)
    if job in ("all", "job-2"):
        job_pool(service)

    print(f"Done. Open {JAEGER_UI} and look for service '{service}'.")
    ray.shutdown()


if __name__ == "__main__":
    main()
