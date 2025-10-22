import pathlib
import shutil
from unittest.mock import patch
import pytest


class TestMainCleanup:
    @pytest.fixture
    def patch_main_config(self, tmp_path, monkeypatch):
        import mindsdb.__main__ as main_mod

        monkeypatch.setattr(main_mod, "config", {"paths": {"tmp": tmp_path}})
        return tmp_path, main_mod

    @pytest.fixture
    def errors(self, caplog):
        """Capture only ERROR logs as concatenated text"""

        class ErrorCapture:
            @property
            def text(self):
                return "\n".join(
                    r.getMessage() for r in caplog.records if r.levelname == "ERROR"
                )

        caplog.clear()
        caplog.set_level("ERROR")
        return ErrorCapture()

    def test_cleans_files_and_dirs_but_keeps_tmp_path(self, patch_main_config):
        tmp_path, main_mod = patch_main_config
        (tmp_path / "a.txt").write_text("hello")
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "b.txt").write_text("world")

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists(), "tmp_path itself should not be deleted"
        assert list(tmp_path.iterdir()) == [], "All content should be removed"

    def test_empty_directory(self, patch_main_config):
        tmp_path, main_mod = patch_main_config
        main_mod.clean_mindsdb_tmp_dir()
        assert tmp_path.exists()
        assert list(tmp_path.iterdir()) == []

    def test_deeply_nested_directories(self, patch_main_config):
        tmp_path, main_mod = patch_main_config
        deep = tmp_path / "a" / "b" / "c" / "d"
        deep.mkdir(parents=True)
        (deep / "file.txt").write_text("deep")

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists()
        assert not (tmp_path / "a").exists()

    def test_symlinks_are_handled(self, patch_main_config):
        tmp_path, main_mod = patch_main_config

        external_file = tmp_path.parent / "external.txt"
        external_file.write_text("external")

        (tmp_path / "link_to_external").symlink_to(external_file)

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists()
        assert list(tmp_path.iterdir()) == []
        assert external_file.exists()

        external_file.unlink()

    def test_unlink_failure_continues_and_logs(self, patch_main_config, errors):
        tmp_path, main_mod = patch_main_config
        (tmp_path / "ok1.txt").write_text("a")
        (tmp_path / "failing_file.txt").write_text("b")
        (tmp_path / "ok2.txt").write_text("c")

        original_unlink = pathlib.Path.unlink

        def mock_unlink(self, *args, **kwargs):
            if self.name == "failing_file.txt":
                raise PermissionError("Cannot delete file")
            return original_unlink(self, *args, **kwargs)

        with patch.object(pathlib.Path, "unlink", mock_unlink):
            main_mod.clean_mindsdb_tmp_dir()

        txt = errors.text
        assert "Failed to clean" in txt
        assert "Cannot delete file" in txt

        assert not (tmp_path / "ok1.txt").exists()
        assert not (tmp_path / "ok2.txt").exists()
        assert (tmp_path / "failing_file.txt").exists()

    def test_rmtree_failure_continues_and_logs(self, patch_main_config, errors):
        tmp_path, main_mod = patch_main_config

        (tmp_path / "file.txt").write_text("content")
        (tmp_path / "failing_dir").mkdir()
        (tmp_path / "another_file.txt").write_text("more content")
        (tmp_path / "good_dir").mkdir()

        original_rmtree = shutil.rmtree

        def mock_rmtree(path, *args, **kwargs):
            if "failing_dir" in str(path):
                raise PermissionError("Cannot delete directory")
            return original_rmtree(path, *args, **kwargs)

        with patch("shutil.rmtree", mock_rmtree):
            main_mod.clean_mindsdb_tmp_dir()

        txt = errors.text
        assert "Failed to clean" in txt
        assert "Cannot delete directory" in txt

        assert not (tmp_path / "file.txt").exists()
        assert not (tmp_path / "another_file.txt").exists()
        assert not (tmp_path / "good_dir").exists()
        assert (tmp_path / "failing_dir").exists()

    def test_mixed_failures_continue_cleanup(self, patch_main_config, errors):
        tmp_path, main_mod = patch_main_config

        (tmp_path / "good_file1.txt").write_text("a")
        (tmp_path / "failing_file.txt").write_text("b")
        (tmp_path / "good_file2.txt").write_text("c")
        (tmp_path / "failing_dir").mkdir()
        (tmp_path / "good_dir").mkdir()

        original_unlink = pathlib.Path.unlink
        original_rmtree = shutil.rmtree

        def mock_unlink(self, *args, **kwargs):
            if self.name == "failing_file.txt":
                raise PermissionError("Cannot delete file")
            return original_unlink(self, *args, **kwargs)

        def mock_rmtree(path, *args, **kwargs):
            if "failing_dir" in str(path):
                raise PermissionError("Cannot delete directory")
            return original_rmtree(path, *args, **kwargs)

        with patch.object(pathlib.Path, "unlink", mock_unlink), patch(
            "shutil.rmtree", mock_rmtree
        ):
            main_mod.clean_mindsdb_tmp_dir()

        txt = errors.text
        # We should have at least two "Failed to clean" lines (file + dir)
        assert txt.count("Failed to clean") >= 2

        assert not (tmp_path / "good_file1.txt").exists()
        assert not (tmp_path / "good_file2.txt").exists()
        assert not (tmp_path / "good_dir").exists()
        assert (tmp_path / "failing_file.txt").exists()
        assert (tmp_path / "failing_dir").exists()

    def test_nonexistent_tmp_path(self, monkeypatch):
        import mindsdb.__main__ as main_mod
        from pathlib import Path

        nonexistent = Path("/tmp/nonexistent_mindsdb_test_dir_12345")
        assert not nonexistent.exists()

        monkeypatch.setattr(main_mod, "config", {"paths": {"tmp": nonexistent}})
        main_mod.clean_mindsdb_tmp_dir()
        assert not nonexistent.exists()

    def test_logger_called_with_correct_level(self, patch_main_config):
        tmp_path, main_mod = patch_main_config
        (tmp_path / "failing_file.txt").write_text("content")

        original_unlink = pathlib.Path.unlink

        def mock_unlink(self, *args, **kwargs):
            if self.name == "failing_file.txt":
                raise PermissionError("Test error")
            return original_unlink(self, *args, **kwargs)

        with patch.object(pathlib.Path, "unlink", mock_unlink), patch(
            "mindsdb.__main__.logger"
        ) as mock_logger:
            main_mod.clean_mindsdb_tmp_dir()
            assert mock_logger.error.called or mock_logger.exception.called

