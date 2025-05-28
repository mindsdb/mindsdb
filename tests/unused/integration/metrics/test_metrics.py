import datetime
import os
import pytest
import requests
import time
from typing import Dict

from tests.integration.utils.http_test_helpers import HTTPHelperMixin
from tests.integration.conftest import HTTP_API_ROOT


def _get_metrics():
    url = HTTP_API_ROOT.rstrip("api") + "metrics"
    print(f"Getting metrics from {url}")
    resp = requests.get(url).text
    print(f"Metrics response: {resp}")
    return resp


def _wait_for_metric(name: str, labels: Dict[str, str], timeout: datetime.timedelta = None):
    if timeout is None:
        timeout = datetime.timedelta(seconds=30)
    start_time = datetime.datetime.now()
    while datetime.datetime.now() - start_time < timeout:
        metrics = _get_metrics()
        for metrics_line in metrics.split('\n'):
            if name not in metrics_line:
                continue
            # Check labels match metric.
            found = True
            for label_name, label_value in labels.items():
                if f'{label_name}="{label_value}"' not in metrics_line:
                    found = False
                    break
            if found:
                print(f"Found metric: {metrics_line}")
                metrics_value = metrics_line.split()[-1]
                return float(metrics_value)
        time.sleep(0.5)
    return -1


class TestMetrics(HTTPHelperMixin):
    @pytest.mark.skipif(("localhost" in HTTP_API_ROOT or "127.0.0.1" in HTTP_API_ROOT) and os.getenv('PROMETHEUS_MULTIPROC_DIR') is None, reason="PROMETHEUS_MULTIPROC_DIR environment variable is not set")
    def test_http_metrics(self):
        # Make an HTTP request and check for updated metrics.
        api_metric_labels = {
            'endpoint': '/util/ping_native',
            'method': 'GET',
            'status': '200'
        }

        before_metric = _wait_for_metric(
            'mindsdb_rest_api_latency_seconds_count',
            api_metric_labels,
        )
        print(f"Before metric: {before_metric}")
        _ = self.api_request('get', '/util/ping_native')
        assert _wait_for_metric(
            'mindsdb_rest_api_latency_seconds_count',
            api_metric_labels,
        ) == before_metric + 1
        # Check multiproc dir is populated.
        multiproc_dir = os.getenv('PROMETHEUS_MULTIPROC_DIR')
        # We can't check this dir if we're running against a remote env.
        if multiproc_dir is not None:
            assert os.path.isdir(multiproc_dir)
            assert len(os.listdir(os.getenv('PROMETHEUS_MULTIPROC_DIR'))) > 0
