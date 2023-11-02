import importlib
import os
import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("requests")
    REQUESTS_INSTALLED = True
except ImportError:
    REQUESTS_INSTALLED = False


@pytest.mark.skipif(not REQUESTS_INSTALLED, reason="requests package is not installed")
class TestClipdropHandler(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        self.api_key = os.environ.get("CLIPDROP_KEY")
        self.local_dir = os.environ.get("LOCAL_DIR")
        self.run_sql(f"""
            CREATE DATABASE mindsdb_clipdrop
                WITH ENGINE = 'clipdrop',
                PARAMETERS = {
                "api_key": '{self.api_key}',
                "dir_to_save": '{self.local_dir}'
                };
        """)

    def test_basic_select_from(self):
        sql = 'SELECT * FROM mindsdb_clipdrop.remove_text WHERE img_url="https://static.vecteezy.com/system/resources/thumbnails/022/721/714/small/youtube-logo-for-popular-online-media-content-creation-website-and-application-free-png.png";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_clipdrop.remove_background where img_url="https://upload.wikimedia.org/wikipedia/commons/a/a5/Red_Kitten_01.jpg";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_clipdrop.sketch_to_image WHERE img_url="https://png.pngtree.com/png-vector/20230531/ourmid/pngtree-simple-kitten-drawing-with-cat-sitting-on-the-white-background-vector-png-image_6785704.png" AND text="A pink fairy tale cat";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_clipdrop.replace_background WHERE img_url="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d8/Indian_Cricket_Player.jpg/1024px-Indian_Cricket_Player.jpg" and text="A yellow background";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_clipdrop.reimagine WHERE img_url="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d8/Indian_Cricket_Player.jpg/1024px-Indian_Cricket_Player.jpg";'
        self.run_sql(sql)

        sql = 'SELECT * FROM mindsdb_clipdrop.text_to_image WHERE text="A pink fairy tale cat";'
        self.run_sql(sql)
