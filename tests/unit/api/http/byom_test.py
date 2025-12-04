import io
import os.path
from http import HTTPStatus
from textwrap import dedent

import pytest


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


def test_path_traversal(client):
    """Test uploading a file"""
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
