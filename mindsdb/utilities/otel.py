import os
import typing

from opentelemetry import trace  # noqa: F401
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics._internal.export import ConsoleMetricExporter, MetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

from mindsdb.utilities.otel_logging import setup_otel_logging
from mindsdb.utilities.otel_metrics import setup_otel_metrics
from mindsdb.utilities.otel_tracing import setup_otel_tracing
from mindsdb.utilities.utils import parse_csv_attributes
from mindsdb.utilities import log

logger = log.getLogger(__name__)

# Check OpenTelemetry exporter type
OTEL_EXPORTER_TYPE = os.getenv("OTEL_EXPORTER_TYPE", "console")  # console or otlp

# Define OTLP endpoint. If not set, the default OTLP endpoint will be used
OTEL_OTLP_ENDPOINT = os.getenv("OTEL_OTLP_ENDPOINT", "http://localhost:4317")

# Define OTLP logging endpoint. If not set, the default OTLP logging endpoint will be used
OTEL_OTLP_LOGGING_ENDPOINT = os.getenv("OTEL_OTLP_LOGGING_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define OTLP tracing endpoint. If not set, the default OTLP tracing endpoint will be used
OTEL_OTLP_TRACING_ENDPOINT = os.getenv("OTEL_OTLP_TRACING_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define OTLP metrics endpoint. If not set, the default OTLP metrics endpoint will be used
OTEL_OTLP_METRICS_ENDPOINT = os.getenv("OTEL_OTLP_METRICS_ENDPOINT", OTEL_OTLP_ENDPOINT)

# Define service name
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "mindsdb_new_test")

# DEFINE SERVICE INSTANCE ID
OTEL_SERVICE_INSTANCE_ID = os.getenv("OTEL_SERVICE_INSTANCE_ID", "mindsdb")

# The name of the environment we"re on, by default local for development, this is set differently per-env in our Helm
# chart values files
OTEL_SERVICE_ENVIRONMENT = os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower()

# Define service release
OTEL_SERVICE_RELEASE = os.getenv("OTEL_SERVICE_RELEASE", "local").lower()

# Define extra attributes
OTEL_EXTRA_ATTRIBUTES = os.getenv("OTEL_EXTRA_ATTRIBUTES", "")

# By default, we have Open Telemetry SDK enabled on all envs, except for local which is disabled by default.
OTEL_SDK_DISABLED = (os.getenv("OTEL_SDK_DISABLED", "false").lower() == "true" or
                     os.getenv("OTEL_SERVICE_ENVIRONMENT", "local").lower() == "local")

OTEL_LOGGING_DISABLED = (os.getenv("OTEL_LOGGING_DISABLED", "false").lower() == "true" or
                         OTEL_SDK_DISABLED)

OTEL_TRACING_DISABLED = (os.getenv("OTEL_TRACING_DISABLED", "false").lower() == "true" or
                         OTEL_SDK_DISABLED)

OTEL_METRICS_DISABLED = (os.getenv("OTEL_METRICS_DISABLED", "false").lower() == "true" or
                         OTEL_SDK_DISABLED)

# If you want to enable Open Telemetry on local for some reason please set OTEL_SDK_FORCE_RUN to true
OTEL_SDK_FORCE_RUN = os.getenv("OTEL_SDK_FORCE_RUN", "false").lower() == "true"


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


def get_logging_exporter() -> typing.Optional[OTLPLogExporter]:
    """
    Get OpenTelemetry logging exporter

    Returns:
        OTLPLogExporter: OpenTelemetry logging exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":
        logging_exporter = OTLPLogExporter(
            endpoint=OTEL_OTLP_LOGGING_ENDPOINT,
            insecure=True
        )
    else:
        logging_exporter = None

    return logging_exporter


def get_span_exporter() -> OTLPSpanExporter:
    """
    Get OpenTelemetry span exporter

    Returns:
        OTLPSpanExporter: OpenTelemetry span exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":
        span_exporter = OTLPSpanExporter(
            endpoint=OTEL_OTLP_TRACING_ENDPOINT,
            insecure=True
        )

    else:
        span_exporter = ConsoleSpanExporter()

    return span_exporter


def get_metrics_exporter() -> typing.Optional[MetricExporter]:
    """
    Get OpenTelemetry metrics exporter

    Returns:
        OTLPLogExporter: OpenTelemetry metrics exporter
    """

    if OTEL_EXPORTER_TYPE == "otlp":
        metrics_exporter = OTLPMetricExporter(
            endpoint=OTEL_OTLP_METRICS_ENDPOINT,
            insecure=True
        )
    else:
        metrics_exporter = ConsoleMetricExporter()

    return metrics_exporter


if not OTEL_SDK_DISABLED or OTEL_SDK_FORCE_RUN:
    logger.info("OpenTelemetry enabled")
    logger.info(f"OpenTelemetry exporter type: {OTEL_EXPORTER_TYPE}")
    logger.info(f"OpenTelemetry service name: {OTEL_SERVICE_NAME}")
    logger.info(f"OpenTelemetry service environment: {OTEL_SERVICE_ENVIRONMENT}")
    logger.info(f"OpenTelemetry service release: {OTEL_SERVICE_RELEASE}")
    logger.info(f"OpenTelemetry extra attributes: {OTEL_EXTRA_ATTRIBUTES}")

    # Define OpenTelemetry resources (e.g., service name)
    attributes = get_otel_attributes()

    # Define OpenTelemetry resources (e.g., service name)
    resource = Resource(attributes=attributes)

    if not OTEL_LOGGING_DISABLED:
        logger.info("OpenTelemetry Logging is enabled")
        setup_otel_logging(resource, get_logging_exporter())

    if not OTEL_TRACING_DISABLED:
        logger.info("OpenTelemetry Tracing is enabled")
        setup_otel_tracing(resource, get_span_exporter())

    if not OTEL_METRICS_DISABLED:
        logger.info("OpenTelemetry Metrics is enabled")
        setup_otel_metrics(resource, get_metrics_exporter())
