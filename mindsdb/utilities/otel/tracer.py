from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import Sampler


def setup_tracer(resource: Resource, sampler: Sampler, exporter: SpanExporter) -> None:
    """
    Setup OpenTelemetry tracing
    """
    # Set the tracer provider with the custom resource
    trace.set_tracer_provider(TracerProvider(resource=resource, sampler=sampler))

    # Replace the default span processor with the custom one
    trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(exporter))
