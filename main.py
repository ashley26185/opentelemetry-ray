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
"""

import time

import ray
from opentelemetry import trace

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

    print(f"Done. Open {JAEGER_UI} and look for service 'ray-otel-demo'.")
    ray.shutdown()


if __name__ == "__main__":
    main()
