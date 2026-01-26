import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from mindsdb.utilities.config import Config


class TestConfig:
    """Tests for Config class"""

    def test_invalid_mindsdb_db_con_raises_error(self):
        """Test that invalid MINDSDB_DB_CON value raises ValueError with helpful message"""
        # Reset the singleton instance before test
        Config._Config__instance = None

        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = Path(tmpdir) / "config.json"
            config_file.write_text(json.dumps({}))

            invalid_db_con = "invalid_connection_string"

            with patch.dict(
                os.environ,
                {
                    "MINDSDB_CONFIG_PATH": str(config_file),
                    "MINDSDB_STORAGE_DIR": tmpdir,
                    "MINDSDB_DB_CON": invalid_db_con,
                },
                clear=False,
            ):
                # Should raise ValueError with helpful message
                with pytest.raises(ValueError) as exc_info:
                    Config()

                error_message = str(exc_info.value)
                assert "Invalid MINDSDB_DB_CON value" in error_message
                assert invalid_db_con in error_message
