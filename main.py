"""Ray + OpenTelemetry demo exporting traces to a local Jaeger.

Run from this directory (so workers can import ``tracing_setup``):

    python main.py

Span structure produced by one run:

    driver-root                          (driver process, custom span)
    ├── fetch_data ray.remote            (driver, Ray PRODUCER span)
    │   └── fetch_data ray.remote_worker (worker, Ray CONSUMER span)
    │       └── fetch_data.db_lookup     (worker, custom child span)
    ├── ... more fetch_data tasks ...
    └── Processor.process ray.remote     (driver, Ray PRODUCER span)
        └── Processor.process ray.remote_worker  (worker, CONSUMER span)
            └── Processor.process.transform       (worker, custom child span)

    driver-root-pool                               (driver, custom span)
    └── PoolActor.run_batch ray.remote              (driver, PRODUCER span)
        └── PoolActor.run_batch ray.remote_worker   (worker, CONSUMER span)
            └── scale_item                          (worker, custom child span, one per item in the batch)

``ray.util.multiprocessing.Pool`` is just a round-robin pool of plain
``@ray.remote`` actors (``PoolActor``) under the hood, so it is traced the
same way as the explicit task/actor calls above: no extra wiring needed. The
one wrinkle is that ``Pool.map`` batches several items into a single
``run_batch.remote()`` call (controlled by ``chunksize``), so one PRODUCER/
CONSUMER span pair can cover many logical work items — see ``scale_item``
below for how to still get a per-item child span out of that.
"""

import time

import ray
from opentelemetry import trace
from ray.util.multiprocessing import Pool

JAEGER_UI = "http://localhost:16686"

# The hook installs the global TracerProvider in the driver AND in every
# worker process. Also enables Ray's task/actor tracing (feature flag).
ray.init(_tracing_startup_hook="tracing_setup:setup_tracing")

# Note: don't keep the Tracer in a module-level global that remote functions
# capture in their closure — Tracer objects are not picklable. Get a fresh
# one inside each function (Ray workers resolve it against the global
# TracerProvider installed by the hook).
tracer = trace.get_tracer(__name__)


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


def main() -> None:
    # Root span in the driver: every Ray span created while it is active
    # (task submissions from the driver) nests beneath it.
    with tracer.start_as_current_span("driver-root"):
        refs = [fetch_data.remote(i) for i in range(5)]
        results = ray.get(refs)
        print(f"tasks done: {results}")

        processor = Processor.remote()
        processed = ray.get(processor.process.remote(results[0]))
        print(f"actor done: {processed}")

    # Separate root span for the pool-based path: `ray.util.multiprocessing.Pool`
    # is Ray's drop-in replacement for stdlib `multiprocessing.Pool`, backed by
    # a small round-robin pool of `@ray.remote` actors instead of OS processes.
    with tracer.start_as_current_span("driver-root-pool"):
        with Pool(processes=2) as pool:
            scaled = pool.map(scale_item, results)
        print(f"pool done: {scaled}")

    print(f"Done. Open {JAEGER_UI} and look for service 'ray-otel-demo'.")
    ray.shutdown()


if __name__ == "__main__":
    main()
