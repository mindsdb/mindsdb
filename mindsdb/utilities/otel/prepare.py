import os
import typing

from opentelemetry import trace  # noqa: F401
from opentelemetry import metrics  # noqa: F401
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter as OTLPLogExporterGRPC
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter as OTLPLogExporterHTTP
from opentelemetry.sdk._logs._internal.export import LogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as OTLPMetricExporterGRPC
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as OTLPMetricExporterHTTP
from opentelemetry.sdk.metrics.export import MetricExporter, ConsoleMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as OTLPSpanExporterGRPC
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as OTLPSpanExporterHTTP
from opentelemetry.sdk.trace.export import SpanExporter, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

from mindsdb.utilities.otel.logger import setup_logger
from mindsdb.utilities.otel.meter import setup_meter
from mindsdb.utilities.otel.tracer import setup_tracer
from mindsdb.utilities.utils import parse_csv_attributes
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Check OpenTelemetry exporter type
OTEL_EXPORTER_TYPE = os.getenv("OTEL_EXPORTER_TYPE", "console")  # console or otlp

# Define OpenTelemetry exporter protocol
OTEL_EXPORTER_PROTOCOL = os.getenv("OTEL_EXPORTER_PROTOCOL", "grpc")  # grpc or http

# Define OTLP endpoint. If not set, the default OTLP endpoint will be used
OTEL_OTLP_ENDPOINT = os.getenv("OTEL_OTLP_ENDPOINT", "http://localhost:4317")

# Define OTLP logging endpoint. If not set, the default OTLP logging endpoint will be used
OTEL_OTLP_LOGGING_ENDPOINT = os.getenv("OTEL_OTLP_LOGGING_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define OTLP tracing endpoint. If not set, the default OTLP tracing endpoint will be used
OTEL_OTLP_TRACING_ENDPOINT = os.getenv("OTEL_OTLP_TRACING_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define OTLP metrics endpoint. If not set, the default OTLP metrics endpoint will be used
OTEL_OTLP_METRICS_ENDPOINT = os.getenv("OTEL_OTLP_METRICS_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define service name
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "mindsdb")

# Define service instace ID
OTEL_SERVICE_INSTANCE_ID = os.getenv("OTEL_SERVICE_INSTANCE_ID", "mindsdb-instance")

# The name of the environment we"re on, by default local for development, this is set differently per-env in our Helm
# chart values files
OTEL_SERVICE_ENVIRONMENT = os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower()

# Define service release
OTEL_SERVICE_RELEASE = os.getenv("OTEL_SERVICE_RELEASE", "local").lower()

# Define how often to capture traces
OTEL_TRACE_SAMPLE_RATE = float(os.getenv("OTEL_TRACE_SAMPLE_RATE", "1.0"))

# Define extra attributes
OTEL_EXTRA_ATTRIBUTES = os.getenv("OTEL_EXTRA_ATTRIBUTES", "")

# Define if OpenTelemetry logging is disabled. By default, it is disabled.
OTEL_LOGGING_DISABLED = os.getenv("OTEL_LOGGING_DISABLED", "true").lower() == "true"

# Define if OpenTelemetry tracing is disabled. By default, it is enabled.
OTEL_TRACING_DISABLED = os.getenv("OTEL_TRACING_DISABLED", "false").lower() == "true"

# Define if OpenTelemetry metrics is disabled. By default, it is disabled.
OTEL_METRICS_DISABLED = os.getenv("OTEL_METRICS_DISABLED", "true").lower() == "true"


def get_otel_attributes() -> dict:
    """
    Get OpenTelemetry attributes

    Returns:
        dict: OpenTelemetry attributes
    """

    base_attributes = {
        "service.name": OTEL_SERVICE_NAME,
        "service.instance.id": OTEL_SERVICE_INSTANCE_ID,
        "environment": OTEL_SERVICE_ENVIRONMENT,
        "release": OTEL_SERVICE_RELEASE,
    }

    extra_attributes = {}
    try:
        extra_attributes = parse_csv_attributes(OTEL_EXTRA_ATTRIBUTES)
    except Exception as e:
        logger.error(f"Failed to parse OTEL_EXTRA_ATTRIBUTES: {e}")

    attributes = {**extra_attributes, **base_attributes}  # Base attributes take precedence over extra attributes

    return attributes


def get_logging_exporter() -> typing.Optional[LogExporter]:
    """
    Get OpenTelemetry logging exporter.

    Returns:
        OTLPLogExporter: OpenTelemetry logging exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":

        if OTEL_EXPORTER_PROTOCOL == "grpc":
            return OTLPLogExporterGRPC(
                endpoint=OTEL_OTLP_LOGGING_ENDPOINT,
                insecure=True
            )

        elif OTEL_EXPORTER_PROTOCOL == "http":
            return OTLPLogExporterHTTP(
                endpoint=OTEL_OTLP_LOGGING_ENDPOINT
            )

    return None


def get_span_exporter() -> SpanExporter:
    """
    Get OpenTelemetry span exporter

    Returns:
        OTLPSpanExporter: OpenTelemetry span exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":

        if OTEL_EXPORTER_PROTOCOL == "grpc":
            return OTLPSpanExporterGRPC(
                endpoint=OTEL_OTLP_TRACING_ENDPOINT,
                insecure=True
            )

        elif OTEL_EXPORTER_PROTOCOL == "http":
            return OTLPSpanExporterHTTP(
                endpoint=OTEL_OTLP_TRACING_ENDPOINT
            )

    return ConsoleSpanExporter()


def get_metrics_exporter() -> typing.Optional[MetricExporter]:
    """
    Get OpenTelemetry metrics exporter

    Returns:
        OTLPLogExporter: OpenTelemetry metrics exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":

        if OTEL_EXPORTER_PROTOCOL == "grpc":
            return OTLPMetricExporterGRPC(
                endpoint=OTEL_OTLP_METRICS_ENDPOINT,
                insecure=True
            )

        elif OTEL_EXPORTER_PROTOCOL == "http":
            return OTLPMetricExporterHTTP(
                endpoint=OTEL_OTLP_METRICS_ENDPOINT
            )

    return ConsoleMetricExporter()


logger.info("OpenTelemetry enabled")
logger.info(f"OpenTelemetry exporter type: {OTEL_EXPORTER_TYPE}")
logger.info(f"OpenTelemetry service name: {OTEL_SERVICE_NAME}")
logger.info(f"OpenTelemetry service environment: {OTEL_SERVICE_ENVIRONMENT}")
logger.info(f"OpenTelemetry service release: {OTEL_SERVICE_RELEASE}")
logger.info(f"OpenTelemetry trace sample rate: {OTEL_TRACE_SAMPLE_RATE}")
logger.info(f"OpenTelemetry extra attributes: {OTEL_EXTRA_ATTRIBUTES}")

# Define OpenTelemetry resources (e.g., service name)
attributes = get_otel_attributes()

# Define OpenTelemetry sampler
sampler = TraceIdRatioBased(OTEL_TRACE_SAMPLE_RATE)

# Define OpenTelemetry resources (e.g., service name)
resource = Resource(attributes=attributes)

if not OTEL_LOGGING_DISABLED:
    logger.info("OpenTelemetry Logging is enabled")
    setup_logger(resource, get_logging_exporter())

if not OTEL_TRACING_DISABLED:
    logger.info("OpenTelemetry Tracing is enabled")
    setup_tracer(resource, sampler, get_span_exporter())

if not OTEL_METRICS_DISABLED:
    logger.info("OpenTelemetry Metrics is enabled")
    setup_meter(resource, get_metrics_exporter())
