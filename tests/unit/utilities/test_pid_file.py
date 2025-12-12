import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from mindsdb.utilities.fs import create_pid_file, delete_pid_file


class TestCreatePidFile:
    """Tests for create_pid_file function"""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for PID files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def mock_tmp_dir(self, temp_dir):
        """Mock get_tmp_dir to return our temp directory"""
        with patch("mindsdb.utilities.fs.get_tmp_dir", return_value=temp_dir):
            yield temp_dir

    def test_does_nothing_when_use_pidfile_not_set(self, mock_tmp_dir):
        """Test that function does nothing when USE_PIDFILE env var is not '1'"""
        with patch.dict(os.environ, {"USE_PIDFILE": "0"}, clear=False):
            create_pid_file({})

        pid_file = mock_tmp_dir / "pid"
        assert not pid_file.exists()

    def test_does_nothing_when_use_pidfile_missing(self, mock_tmp_dir):
        """Test that function does nothing when USE_PIDFILE env var is missing"""
        env_copy = os.environ.copy()
        env_copy.pop("USE_PIDFILE", None)
        with patch.dict(os.environ, env_copy, clear=True):
            create_pid_file({})

        pid_file = mock_tmp_dir / "pid"
        assert not pid_file.exists()

    def test_creates_pid_file_when_not_exists(self, mock_tmp_dir):
        """Test that PID file is created when it doesn't exist"""
        config = {
            "api": {"http": {"host": "127.0.0.1", "port": 47334}},
            "auth": {"username": "mindsdb", "password": "secret"},
            "pid_file_content": {
                "http_host": "api.http.host",
                "http_port": "api.http.port",
                "username": "auth.username",
                "password": "auth.password",
            },
        }

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        pid_file = mock_tmp_dir / "pid"
        assert pid_file.exists()

        data = json.loads(pid_file.read_text())
        assert data["pid"] == os.getpid()
        assert data["http_host"] == "127.0.0.1"
        assert data["http_port"] == 47334
        assert data["username"] == "mindsdb"
        assert data["password"] == "secret"

    def test_creates_pid_file_with_empty_config(self, mock_tmp_dir):
        """Test that PID file is created with only PID number when pid_file_content is None"""
        config = {"pid_file_content": None}

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        pid_file = mock_tmp_dir / "pid"
        assert pid_file.exists()

        # Should be just a number, not JSON
        content = pid_file.read_text()
        assert content == str(os.getpid())

    def test_removes_invalid_json_pid_file(self, mock_tmp_dir):
        """Test that PID file with invalid JSON is removed and recreated"""
        pid_file = mock_tmp_dir / "pid"
        pid_file.write_text("not valid json")

        config = {"pid_file_content": None}

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        assert pid_file.exists()
        content = pid_file.read_text()
        assert int(content) == os.getpid()

    def test_removes_pid_file_with_nonexistent_process(self, mock_tmp_dir):
        """Test that PID file with non-existent process is removed and recreated"""
        pid_file = mock_tmp_dir / "pid"
        # Use a very high PID that's unlikely to exist
        old_data = {"pid": 999999999, "http_host": "old_host", "http_port": 12345}
        pid_file.write_text(json.dumps(old_data))

        config = {
            "api": {"http": {"host": "new_host", "port": 54321}},
            "pid_file_content": {"http_host": "api.http.host", "http_port": "api.http.port"},
        }

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        data = json.loads(pid_file.read_text())
        assert data["pid"] == os.getpid()
        assert data["http_host"] == "new_host"
        assert data["http_port"] == 54321

    def test_raises_exception_when_process_exists(self, mock_tmp_dir):
        """Test that exception is raised when PID file points to existing process"""
        pid_file = mock_tmp_dir / "pid"
        # Use current process PID to simulate existing process
        old_data = {"pid": os.getpid()}
        pid_file.write_text(json.dumps(old_data))

        config = {"pid_file_content": None}

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            with pytest.raises(Exception, match="Found PID file with existing process"):
                create_pid_file(config)

    def test_creates_pid_file_with_empty_pid_file_content(self, mock_tmp_dir):
        """Test that PID file is created with only PID number when pid_file_content is empty dict"""
        config = {"pid_file_content": {}}

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        pid_file = mock_tmp_dir / "pid"
        assert pid_file.exists()

        # Should be just a number, not JSON
        content = pid_file.read_text()
        assert content == str(os.getpid())

    def test_creates_pid_file_with_json_content(self, mock_tmp_dir):
        """Test that PID file is created with JSON when pid_file_content has fields"""
        config = {
            "api": {"http": {"host": "0.0.0.0", "port": 47334}},
            "pid_file_content": {
                "http_host": "api.http.host",
                "http_port": "api.http.port",
            },
        }

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        pid_file = mock_tmp_dir / "pid"
        assert pid_file.exists()

        # Should be JSON
        data = json.loads(pid_file.read_text())
        assert data["pid"] == os.getpid()
        assert data["http_host"] == "0.0.0.0"
        assert data["http_port"] == 47334

    def test_creates_pid_file_with_none_values_in_json(self, mock_tmp_dir):
        """Test that PID file JSON includes None for missing config values"""
        config = {
            "pid_file_content": {
                "http_host": "api.http.host",
                "missing_value": "path.to.missing.value",
            }
        }

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            create_pid_file(config)

        pid_file = mock_tmp_dir / "pid"
        assert pid_file.exists()

        # Should be JSON with None values for missing paths
        data = json.loads(pid_file.read_text())
        assert data["pid"] == os.getpid()
        assert data["http_host"] is None
        assert data["missing_value"] is None

    def test_raises_exception_when_process_exists_with_simple_pid(self, mock_tmp_dir):
        """Test that exception is raised when PID file contains just a number of existing process"""
        pid_file = mock_tmp_dir / "pid"
        # Use current process PID to simulate existing process (as simple number)
        pid_file.write_text(str(os.getpid()))

        config = {"pid_file_content": None}

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            with pytest.raises(Exception, match="Found PID file with existing process"):
                create_pid_file(config)


class TestDeletePidFile:
    """Tests for delete_pid_file function"""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for PID files"""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def mock_tmp_dir(self, temp_dir):
        """Mock get_tmp_dir to return our temp directory"""
        with patch("mindsdb.utilities.fs.get_tmp_dir", return_value=temp_dir):
            yield temp_dir

    def test_does_nothing_when_use_pidfile_not_set(self, mock_tmp_dir):
        """Test that function does nothing when USE_PIDFILE env var is not '1'"""
        pid_file = mock_tmp_dir / "pid"
        pid_file.write_text(json.dumps({"pid": os.getpid()}))

        with patch.dict(os.environ, {"USE_PIDFILE": "0"}, clear=False):
            delete_pid_file()

        # File should still exist
        assert pid_file.exists()

    def test_does_nothing_when_use_pidfile_missing(self, mock_tmp_dir):
        """Test that function does nothing when USE_PIDFILE env var is missing"""
        pid_file = mock_tmp_dir / "pid"
        pid_file.write_text(json.dumps({"pid": os.getpid()}))

        env_copy = os.environ.copy()
        env_copy.pop("USE_PIDFILE", None)
        with patch.dict(os.environ, env_copy, clear=True):
            delete_pid_file()

        # File should still exist
        assert pid_file.exists()

    def test_does_nothing_when_pid_file_not_exists(self, mock_tmp_dir):
        """Test that function does nothing when PID file doesn't exist"""
        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            # Should not raise any exception
            delete_pid_file()

    def test_deletes_pid_file_when_pid_matches(self, mock_tmp_dir):
        """Test that PID file is deleted when PID matches current process"""
        pid_file = mock_tmp_dir / "pid"
        pid_file.write_text(json.dumps({"pid": os.getpid()}))

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            delete_pid_file()

        assert not pid_file.exists()

    def test_does_not_delete_when_pid_mismatch(self, mock_tmp_dir):
        """Test that PID file is not deleted when PID doesn't match"""
        pid_file = mock_tmp_dir / "pid"
        # Use a different PID
        other_pid = os.getpid() + 1
        pid_file.write_text(json.dumps({"pid": other_pid}))

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            delete_pid_file()

        # File should still exist
        assert pid_file.exists()

    def test_handles_invalid_json_gracefully(self, mock_tmp_dir):
        """Test that invalid JSON in PID file is handled gracefully"""
        pid_file = mock_tmp_dir / "pid"
        pid_file.write_text("not valid json")

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            # Should not raise exception, function logs a warning and removes corrupted file
            delete_pid_file()

        # Corrupted PID file should be removed
        assert not pid_file.exists()

    def test_deletes_pid_file_with_simple_pid_when_matches(self, mock_tmp_dir):
        """Test that PID file with simple PID number is deleted when it matches current process"""
        pid_file = mock_tmp_dir / "pid"
        # Write just the PID as a number
        pid_file.write_text(str(os.getpid()))

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            delete_pid_file()

        # File should be deleted since PID matches
        assert not pid_file.exists()

    def test_does_not_delete_simple_pid_when_mismatch(self, mock_tmp_dir):
        """Test that PID file with simple PID number is not deleted when PID doesn't match"""
        pid_file = mock_tmp_dir / "pid"
        # Write a different PID as a number
        other_pid = os.getpid() + 1
        pid_file.write_text(str(other_pid))

        with patch.dict(os.environ, {"USE_PIDFILE": "1"}, clear=False):
            delete_pid_file()

        # File should NOT be deleted since PID doesn't match
        assert pid_file.exists()
