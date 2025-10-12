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

        assert tmp_path.exists(), "tmp_path itself should not be deleted"
        assert list(tmp_path.iterdir()) == [], "All content should be removed"
