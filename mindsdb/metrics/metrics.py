from http import HTTPStatus
import functools
import time
import os

from prometheus_client import Histogram, Summary


INTEGRATION_HANDLER_QUERY_TIME = Summary(
    'mindsdb_integration_handler_query_seconds',
    'How long integration handlers take to answer queries',
    ('integration', 'response_type')
)

INTEGRATION_HANDLER_RESPONSE_SIZE = Summary(
    'mindsdb_integration_handler_response_size',
    'How many rows are returned by an integration handler query',
    ('integration', 'response_type')
)

_REST_API_LATENCY = Histogram(
    'mindsdb_rest_api_latency_seconds',
    'How long REST API requests take to complete, grouped by method, endpoint, and status',
    ('method', 'endpoint', 'status')
)


def api_endpoint_metrics(method: str, uri: str):
    def decorator_metrics(endpoint_func):
        @functools.wraps(endpoint_func)
        def wrapper_metrics(*args, **kwargs):
            if os.environ.get('PROMETHEUS_MULTIPROC_DIR', None) is None:
                return endpoint_func(*args, **kwargs)
            time_before_query = time.perf_counter()
            try:
                response = endpoint_func(*args, **kwargs)
            except Exception as e:
                # Still record metrics for unexpected exceptions.
                elapsed_seconds = time.perf_counter() - time_before_query
                api_latency_with_labels = _REST_API_LATENCY.labels(
                    method, uri, HTTPStatus.INTERNAL_SERVER_ERROR.value)
                api_latency_with_labels.observe(elapsed_seconds)
                raise e
            elapsed_seconds = time.perf_counter() - time_before_query
            status = response.status_code if hasattr(response, 'status_code') else HTTPStatus.OK.value
            api_latency_with_labels = _REST_API_LATENCY.labels(method, uri, status)
            api_latency_with_labels.observe(elapsed_seconds)
            return response
        return wrapper_metrics
    return decorator_metrics
