import io
import os.path
from http import HTTPStatus
from textwrap import dedent


def test_path_traversal(client):
    """Test uploading a file"""
    file = io.BytesIO(
        dedent("""
        class CustomPredictor():
            def train(self, df, target_col, args=None):
               ...
            def predict(self, df):
               ...
    """).encode()
    )
    path = "../../../../../../../../../../tmp/test_module.py"
    response = client.put(
        "/api/handlers/byom/model1",
        data={
            "code": (file, path),
            "modules": (io.BytesIO(b""), "req.txt"),
            "mode": "custom_function",
        },
    )
    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert not os.path.exists(path)
