"""OpenTelemetry startup hook for Ray.

Ray runs this hook in the driver and in every worker process (see
``ray/_private/worker.py:connect()``, which reads the hook string from GCS
and imports it as ``module:attribute``). Ray also calls
``ray.util.tracing.tracing_helper._enable_tracing()`` itself before running
the hook, so this function only needs to install the TracerProvider.

Pass it to Ray as::

    ray.init(_tracing_startup_hook="tracing_setup:setup_tracing")

The module must be importable from every worker, so run the demo from this
directory (with the directory on ``PYTHONPATH``).

``SimpleSpanProcessor`` is used deliberately: Ray never shuts the provider
down, and worker processes can be killed when the cluster stops, so batch
processing could drop the last spans of a short demo run.
"""

import os
import socket

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor


def setup_tracing() -> None:
    """Install the global TracerProvider, exporting spans to Jaeger via OTLP.

    The exporter is constructed without an explicit endpoint: it then uses
    the standard OTel env vars (``OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`` /
    ``OTEL_EXPORTER_OTLP_ENDPOINT``) and falls back to
    ``http://localhost:4318/v1/traces``, which is exactly where the Jaeger
    all-in-one container listens for OTLP/HTTP. (Note: in opentelemetry-
    exporter-otlp >= 1.36, passing ``endpoint=`` explicitly uses the URL
    as-is, so the signal path ``/v1/traces`` would have to be included.)
    """
    # ``host.name`` makes every span attributable to the node it was produced
    # on (e.g. ``kali`` vs the Mac's hostname) in Jaeger's process tags.
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": os.environ.get("OTEL_SERVICE_NAME", "ray-otel-demo"),
                "host.name": socket.gethostname(),
            }
        )
    )
    provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

