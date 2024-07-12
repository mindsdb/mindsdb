import unittest
import snowflake
from unittest.mock import patch, MagicMock, Mock
from collections import OrderedDict

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.handlers.s3_handler.s3_handler import S3Handler


class CursorContextManager(Mock):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestS3Handler(unittest.TestCase):

    dummy_connection_data = OrderedDict(
        aws_access_key_id='AQAXEQK89OX07YS34OP',
        aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        bucket='mindsdb-bucket',
        region_name='us-east-2',
    )

    def setUp(self):
        self.patcher = patch('duckdb.connect')
        self.mock_connect = self.patcher.start()
        self.handler = S3Handler('s3', connection_data=self.dummy_connection_data)

    def tearDown(self):
        self.patcher.stop()