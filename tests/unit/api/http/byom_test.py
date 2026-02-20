import io
import os.path
from http import HTTPStatus
from textwrap import dedent

import pytest

from mindsdb.utilities.config import config


def get_file():
    return io.BytesIO(
        dedent("""
            class CustomPredictor():
                def train(self, df, target_col, args=None):
                    ...
                def predict(self, df):
                    ...
        """).encode()
    )


def test_disabled_byom(client):
    """Test disabled byom"""
    config._config["byom"]["enabled"] = False
    response = client.put(
        "/api/handlers/byom/model1",
        data={
            "code": (get_file(), "/tmp/test_module.py"),
            "modules": (io.BytesIO(b""), "req.txt"),
            "mode": "custom_function",
        },
    )
    assert response.status_code == HTTPStatus.FORBIDDEN


def test_path_traversal(client):
    """Test uploading a file"""
    config._config["byom"]["enabled"] = True
    path = "../../../../../../../../../../tmp/test_module.py"
    response = client.put(
        "/api/handlers/byom/model1",
        data={
            "code": (get_file(), path),
            "modules": (io.BytesIO(b""), "req.txt"),
            "mode": "custom_function",
        },
    )
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert not os.path.exists(path)


@pytest.mark.slow
def test_conflict(client):
    """Test that it is not possible to create two engins with the same name"""
    config._config["byom"]["enabled"] = True
    path = "test_module.py"
    response = client.put(
        "/api/handlers/byom/model1",
        data={
            "code": (get_file(), path),
            "modules": (io.BytesIO(b""), "req.txt"),
            "type": "inhouse",
        },
    )
    assert response.status_code == HTTPStatus.OK

    response = client.put(
        "/api/handlers/byom/model1",
        data={
            "code": (get_file(), path),
            "modules": (io.BytesIO(b""), "req.txt"),
            "type": "inhouse",
        },
    )

    assert response.status_code == HTTPStatus.CONFLICT
    assert not os.path.exists(path)
