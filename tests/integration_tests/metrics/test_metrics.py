import datetime
import os
import pytest
import requests
import time
from typing import Dict

from mindsdb.utilities.config import Config
from tests.utils.http_test_helpers import HTTPHelperMixin
# TODO: Refactor common fixtures out of conftest.
from tests.integration_tests.flows.conftest import config, mindsdb_app, config, temp_dir # noqa

# U by mindsdb_app fixture in conftest
OVERRIDE_CONFIG = {
    'tasks': {'disable': True},
    'jobs': {'disable': True}
}
API_LIST = ["http"]


def _get_metrics():
    cfg = Config()
    base_host = cfg['api']['http']['host']
    port = cfg['api']['http']['port']
    url = f'http://{base_host}:{port}/metrics'
    return requests.get(url).text


def _wait_for_metric(name: str, labels: Dict[str, str], value: str, timeout: datetime.timedelta = None):
    if timeout is None:
        timeout = datetime.timedelta(seconds=30)
    start_time = datetime.datetime.now()
    while datetime.datetime.now() - start_time < timeout:
        metrics = _get_metrics()
        print(metrics)
        for metrics_line in metrics.split('\n'):
            if name not in metrics_line:
                continue
            # Check labels match metric.
            for label_name, label_value in labels.items():
                if f'{label_name}="{label_value}"' not in metrics_line:
                    continue
                # Check value if labels match.
                metrics_value = metrics_line.split()[-1]
                return metrics_value == value
        time.sleep(0.5)
    return False


@pytest.mark.usefixtures('mindsdb_app')
class TestMetrics(HTTPHelperMixin):
    @pytest.mark.skipif(os.getenv('PROMETHEUS_MULTIPROC_DIR') is None, reason="PROMETHEUS_MULTIPROC_DIR environment variable is not set")
    def test_http_metrics(self):
        multiproc_dir = os.getenv('PROMETHEUS_MULTIPROC_DIR')
        assert os.path.isdir(multiproc_dir)
        # Make an HTTP request and check for updated metrics.
        api_metric_labels = {
            'endpoint': '/models',
            'method': 'GET',
            'status': '200'
        }
        _ = self.api_request('get', '/projects/mindsdb/models')
        assert _wait_for_metric(
            'mindsdb_rest_api_latency_seconds_count',
            api_metric_labels,
            '1.0'
        )
        # Check multiproc dir is populated.
        assert len(os.listdir(os.getenv('PROMETHEUS_MULTIPROC_DIR'))) > 0
