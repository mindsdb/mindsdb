import os

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from mindsdb.utilities import log
logger = log.getLogger(__name__)


# Check OpenTelemetry exporter type
OTEL_EXPORTER_TYPE = os.getenv("OTEL_EXPORTER_TYPE", "console")  # console or otlp

# Define OTLP endpoint. If not set, the default OTLP endpoint will be used
OTEL_OTLP_ENDPOINT = os.getenv("OTEL_OTLP_ENDPOINT", "http://localhost:4317")

# Define service name
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "mindsdb")

# The name of the environment we"re on, by default local for development, this is set differently per-env in our Helm chart values files
OTEL_SERVICE_ENVIRONMENT = os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower()

# Define service release
OTEL_SERVICE_RELEASE = os.getenv("OTEL_SERVICE_RELEASE", "local").lower()

# By default we have Open Telemetry SDK enabled on all envs, except for local which is disabled by default
#   If you want to enable Open Telemetry on local for some reason please set OTEL_SDK_FORCE_RUN to true
OTEL_SDK_DISABLED = os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true" or os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower() == "local"
OTEL_SDK_FORCE_RUN = os.getenv("OTEL_SDK_FORCE_RUN", "false").lower() == "true"

# Custom span processor to add global tags to spans


class GlobalTaggingSpanProcessor(BatchSpanProcessor):
    def on_start(self, span: Span, parent_context):
        # Add environment and release to every span
        span.set_attribute("environment", OTEL_SERVICE_ENVIRONMENT)
        span.set_attribute("release", OTEL_SERVICE_RELEASE)
        super().on_start(span, parent_context)


if not OTEL_SDK_DISABLED or OTEL_SDK_FORCE_RUN:
    logger.info("OpenTelemetry enabled")
    logger.info(f"OpenTelemetry exporter type: {OTEL_EXPORTER_TYPE}")
    logger.info(f"OpenTelemetry service name: {OTEL_SERVICE_NAME}")
    logger.info(f"OpenTelemetry service environment: {OTEL_SERVICE_ENVIRONMENT}")
    logger.info(f"OpenTelemetry service release: {OTEL_SERVICE_RELEASE}")

    # Define OpenTelemetry resources (e.g., service name)
    resource = Resource(attributes={"service.name": OTEL_SERVICE_NAME})

    # Set the tracer provider with the custom resource
    trace.set_tracer_provider(TracerProvider(resource=resource))

    # Configure the appropriate exporter based on the environment variable
    if OTEL_EXPORTER_TYPE == "otlp":
        logger.info("OpenTelemetry is using OTLP exporter")

        exporter = OTLPSpanExporter(
            endpoint=OTEL_OTLP_ENDPOINT,  # Default OTLP endpoint
            insecure=True  # Disable TLS for local testing
        )

    else:
        logger.info("OpenTelemetry is using Console exporter")

        exporter = ConsoleSpanExporter()

    # Replace the default span processor with the custom one
    trace.get_tracer_provider().add_span_processor(GlobalTaggingSpanProcessor(exporter))
