import pytest
from unittest.mock import patch


@pytest.fixture
def patch_main_config(tmp_path, monkeypatch):
    import mindsdb.__main__ as main_mod

    monkeypatch.setattr(main_mod, "config", {"paths": {"tmp": tmp_path}})
    return tmp_path, main_mod


class TestMainCleanup:

    def test_cleans_files_and_dirs_but_keeps_tmp_path(self, patch_main_config):
        """Test that all content is cleaned but tmp_path itself remains"""
        tmp_path, main_mod = patch_main_config

        (tmp_path / "a.txt").write_text("hello")
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "b.txt").write_text("world")

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists()
        assert list(tmp_path.iterdir()) == []

    def test_empty_tmp_path(self, patch_main_config):
        """Test that cleaning an already empty tmp_path works without errors"""
        tmp_path, main_mod = patch_main_config

        assert list(tmp_path.iterdir()) == []
        main_mod.clean_mindsdb_tmp_dir()
        assert list(tmp_path.iterdir()) == []
        assert tmp_path.exists()

    def test_nonexistent_tmp_path(self, monkeypatch):
        """Test that cleaning a non-existent tmp_path does not raise errors"""
        import mindsdb.__main__ as main_mod

        monkeypatch.setattr(main_mod, "config", {"paths": {"tmp": "/nonexistent/path"}})

        try:
            main_mod.clean_mindsdb_tmp_dir()
        except Exception as e:
            pytest.fail(f"clean_mindsdb_tmp_dir raised an exception: {e}")

    def test_mixed_content_cleanup(self, patch_main_config):
        """Test that a mix of files and directories are cleaned properly"""
        tmp_path, main_mod = patch_main_config

        (tmp_path / "file1.txt").write_text("file1")
        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir1" / "file2.txt").write_text("file2")
        (tmp_path / "dir2").mkdir()

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists()
        assert list(tmp_path.iterdir()) == []

    def test_nested_directories_cleanup(self, patch_main_config):
        """Test that nested directories are cleaned properly"""
        tmp_path, main_mod = patch_main_config

        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir1" / "subdir1").mkdir()
        (tmp_path / "dir1" / "subdir1" / "file1.txt").write_text("file1")
        (tmp_path / "dir2").mkdir()
        (tmp_path / "dir2" / "file2.txt").write_text("file2")

        main_mod.clean_mindsdb_tmp_dir()

        assert tmp_path.exists()
        assert list(tmp_path.iterdir()) == []

    def test_rmtree_failure_handling(self, patch_main_config, monkeypatch):
        """Test that exceptions during rmtree are logged but do not stop cleanup"""
        tmp_path, main_mod = patch_main_config

        (tmp_path / "dir1").mkdir()
        (tmp_path / "dir1" / "file1.txt").write_text("file1")

        def mock_rmtree(path, ignore_errors):
            raise Exception("Simulated rmtree failure")

        monkeypatch.setattr(main_mod.shutil, "rmtree", mock_rmtree)

        main_mod.clean_mindsdb_tmp_dir()

        assert (tmp_path / "dir1").exists()
        assert (tmp_path / "dir1" / "file1.txt").exists()
