from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    PeriodicExportingMetricReader,
)


def setup_meter(resource: Resource, exporter: MetricExporter) -> None:
    """
    Setup OpenTelemetry metrics
    """

    metric_reader = PeriodicExportingMetricReader(exporter=exporter)
    provider = MeterProvider(resource=resource, metric_readers=[metric_reader])

    # Sets the global default meter provider
    metrics.set_meter_provider(provider)
