import os
import subprocess
import tempfile

import pytest
from unittest.mock import patch, MagicMock

from mindsdb.integrations.utilities.install import (
    read_dependencies,
    parse_dependencies,
    install_dependencies,
)


class TestReadDependencies:
    """Tests for the read_dependencies function."""

    def test_reads_valid_file(self, tmp_path):
        """Test that dependencies are read and stripped from a valid file."""
        req_file = tmp_path / "requirements.txt"
        req_file.write_text("  pkg1==1.0  \npkg2>=2.0\n")

        result = read_dependencies(str(req_file))

        assert result == ["pkg1==1.0", "pkg2>=2.0"]

    def test_filters_empty_lines(self, tmp_path):
        """Test that empty and whitespace-only lines are filtered out."""
        req_file = tmp_path / "requirements.txt"
        req_file.write_text("pkg1==1.0\n\n  \n\npkg2>=2.0\n")

        result = read_dependencies(str(req_file))

        assert result == ["pkg1==1.0", "pkg2>=2.0"]

    def test_file_not_found(self):
        """Test that a missing file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            read_dependencies("/nonexistent/path/requirements.txt")


class TestParseDependencies:
    """Tests for the parse_dependencies function."""

    def test_simple_dependencies(self):
        """Test that plain dependencies pass through unchanged."""
        deps = ["pkg1==1.0", "pkg2>=2.0", "pkg3"]

        result = parse_dependencies(deps)

        assert result == ["pkg1==1.0", "pkg2>=2.0", "pkg3"]

    def test_ignores_standalone_comments(self):
        """Test that lines starting with # are skipped."""
        deps = ["# this is a comment", "pkg1==1.0", "# another comment", "pkg2>=2.0"]

        result = parse_dependencies(deps)

        assert result == ["pkg1==1.0", "pkg2>=2.0"]

    def test_removes_inline_comments(self):
        """Test that inline comments are stripped from dependency lines."""
        deps = ["pkg1==1.0  # pinned version", "pkg2>=2.0 # minimum version"]

        result = parse_dependencies(deps)

        assert result == ["pkg1==1.0", "pkg2>=2.0"]

    def test_resolves_nested_requirements(self, tmp_path):
        """Test that -r directives recursively include dependencies from referenced files."""
        inner_req = tmp_path / "inner_requirements.txt"
        inner_req.write_text("inner_pkg==3.0\n")

        with patch(
            "mindsdb.integrations.utilities.install.os.path.dirname",
            return_value=str(tmp_path),
        ):
            deps = [
                "pkg1==1.0",
                f"-r {inner_req}",
            ]
            # The path resolution replaces 'mindsdb/integrations' with '..',
            # but since our path doesn't contain that prefix, it resolves relative to tmp_path.
            # We need to mock the path resolution to point to our temp file.
            with patch(
                "mindsdb.integrations.utilities.install.os.path.abspath",
                return_value=str(inner_req),
            ):
                with patch(
                    "mindsdb.integrations.utilities.install.os.path.exists",
                    return_value=True,
                ):
                    result = parse_dependencies(deps)

        assert result == ["pkg1==1.0", "inner_pkg==3.0"]

    def test_nested_file_not_found(self):
        """Test that a missing nested requirements file raises FileNotFoundError."""
        deps = ["-r mindsdb/integrations/handlers/community/nonexistent_handler/requirements.txt"]

        with pytest.raises(FileNotFoundError, match="Requirements file not found"):
            parse_dependencies(deps)

    def test_empty_list(self):
        """Test that an empty dependency list returns an empty list."""
        assert parse_dependencies([]) == []

    def test_only_comments(self):
        """Test that a list of only comments returns an empty list."""
        deps = ["# comment 1", "# comment 2"]

        result = parse_dependencies(deps)

        assert result == []


class TestInstallDependencies:
    """Tests for the install_dependencies function."""

    @patch("mindsdb.integrations.utilities.install.subprocess.Popen")
    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_successful_install(self, mock_parse, mock_popen):
        """Test that a successful pip install returns success."""
        mock_parse.return_value = ["pkg1==1.0"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.communicate.return_value = (b"", b"")
        mock_popen.return_value = mock_proc

        result = install_dependencies(["pkg1==1.0"])

        assert result["success"] is True
        assert result["error_message"] is None

    @patch("mindsdb.integrations.utilities.install.subprocess.Popen")
    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_pip_failure(self, mock_parse, mock_popen):
        """Test that a pip failure returns an error with stderr output."""
        mock_parse.return_value = ["bad_pkg"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 1
        mock_proc.communicate.return_value = (b"", b"No matching distribution found")
        mock_popen.return_value = mock_proc

        result = install_dependencies(["bad_pkg"])

        assert result["success"] is False
        assert "No matching distribution found" in result["error_message"]

    @patch("mindsdb.integrations.utilities.install.subprocess.Popen")
    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_pip_failure_with_stdout_and_stderr(self, mock_parse, mock_popen):
        """Test that both stdout and stderr are included in the error message."""
        mock_parse.return_value = ["bad_pkg"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 1
        mock_proc.communicate.return_value = (b"Some output", b"Some error")
        mock_popen.return_value = mock_proc

        result = install_dependencies(["bad_pkg"])

        assert result["success"] is False
        assert "Output: Some output" in result["error_message"]
        assert "Errors: Some error" in result["error_message"]

    @patch("mindsdb.integrations.utilities.install.subprocess.Popen")
    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_timeout_error(self, mock_parse, mock_popen):
        """Test that a subprocess timeout returns a timeout error message."""
        mock_parse.return_value = ["pkg1==1.0"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.communicate.side_effect = subprocess.TimeoutExpired(cmd="pip", timeout=1)
        mock_popen.return_value = mock_proc

        result = install_dependencies(["pkg1==1.0"])

        assert result["success"] is False
        assert "Timeout error" in result["error_message"]
        mock_proc.kill.assert_called_once()

    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_parse_error_propagates(self, mock_parse):
        """Test that a FileNotFoundError from parsing is caught and returned."""
        mock_parse.side_effect = FileNotFoundError("requirements.txt not found")

        result = install_dependencies(["-r nonexistent/requirements.txt"])

        assert result["success"] is False
        assert "file not found" in result["error_message"]

    @patch("mindsdb.integrations.utilities.install.parse_dependencies")
    def test_unknown_parse_error(self, mock_parse):
        """Test that an unexpected error from parsing is caught and returned."""
        mock_parse.side_effect = RuntimeError("unexpected")

        result = install_dependencies(["pkg1"])

        assert result["success"] is False
        assert "Unknown error parsing dependencies" in result["error_message"]
