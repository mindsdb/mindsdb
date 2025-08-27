from http import HTTPStatus
import tempfile


def test_get_files_list(client):
    """Test getting list of all files"""
    response = client.get("/api/files/", follow_redirects=True)
    assert response.status_code == HTTPStatus.OK
    files_list = response.get_json()
    assert isinstance(files_list, list)


def test_put_file(client):
    """Test uploading a file"""
    file_content = b"Hello, World!"
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(file_content)
        temp_file.flush()
        temp_file.seek(0)
        temp_file.seek(0)
        data = {"file": (temp_file, "test.txt")}
        response = client.put(
            "/api/files/test.txt",
            data=data,
            content_type="multipart/form-data",
            follow_redirects=True,
        )
    assert response.status_code == HTTPStatus.OK


def test_delete_file(client):
    """Test deleting a file"""
    response = client.delete("/api/files/test.txt", follow_redirects=True)
    assert response.status_code == HTTPStatus.OK


def test_delete_nonexistent_file(client):
    """Test deleting a nonexistent file"""
    response = client.delete("/api/files/nonexistent.txt", follow_redirects=True)
    assert response.status_code == HTTPStatus.BAD_REQUEST
    data = response.get_json()
    assert "Error deleting file" in data["title"]
    assert (
        "There was an error while trying to delete file with name 'nonexistent.txt'"
        in data["detail"]
    )


def test_put_file_invalid_url(client):
    """Test uploading with an invalid URL"""
    data = {"source_type": "url", "source": "not_a_url", "file": "bad.txt"}
    response = client.put(
        "/api/files/bad.txt",
        json=data,
        content_type="application/json",
        follow_redirects=True,
    )
    assert response.status_code == 400
    data = response.get_json()
    assert "Invalid URL" in data["title"]


def test_put_file_url_upload_disabled(client, monkeypatch):
    """Test uploading from URL when URL upload is disabled"""
    # Patch config to disable URL upload
    monkeypatch.setattr(
        "mindsdb.api.http.namespaces.file.config",
        {"url_file_upload": {"enabled": False}},
    )
    data = {
        "source_type": "url",
        "source": "http://example.com/file.txt",
        "file": "remote.txt",
    }
    response = client.put(
        "/api/files/remote.txt",
        json=data,
        content_type="application/json",
        follow_redirects=True,
    )
    assert response.status_code == 400
    data = response.get_json()
    assert "URL file upload is disabled" in data["detail"]
