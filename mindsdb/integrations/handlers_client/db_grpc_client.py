import os
import base64
import traceback
import pickle
import json

import grpc
from mindsdb.grpc.db import db_pb2_grpc
from mindsdb.grpc.db import db_pb2

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.handlers_client.base_client import BaseClient, Switcher
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class DBClientGRPC:

    def __init__(self, handler_type: str, **kwargs: dict):
        self.handler_type = handler_type
        self.handler_params = kwargs
        host = os.environ.get("MINDSDB_DB_SERVICE_HOST", None)
        port = os.environ.get("MINDSDB_DB_SERVICE_PORT", None)
        for a in ("fs_store", "file_storage"):
            if a in self.handler_params:
                del self.handler_params[a]
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = db_pb2_grpc.DBServiceStub(self.channel)

    def __del__(self):
        self.channel.close()

    @property
    def context(self):
        ctx_str = json.dumps(ctx.dump())
        return db_pb2.HandlerContext(handler_type=self.handler_type,
                                     handler_params=json.dumps(self.handler_params),
                                     context=ctx_str)

    @staticmethod
    def _to_status_response(response: db_pb2.StatusResponse):
        return StatusResponse(success=response.success,
                              error_message=response.error_message)


    @staticmethod
    def _to_response(response: db_pb2.Response):
        data = pickle.loads(response.data_frame)
        return Response(
                resp_type=response.type,
                data_frame=data,
                query=response.query,
                error_code=response.error_code,
                error_message=response.error_message,
        )


    def connect(self):
        resp = self.stub.Connect(self.context)
        logger.error("%s.connect: returns success - %s, error - %s",
                     self.__class__.__name__,
                     resp.success,
                     resp.error_message)

        return self._to_status_response(resp)

    def check_connection(self):
        resp = self.stub.CheckConnection(self.context)
        logger.error("%s.check_connection: returns success - %s, error - %s",
                     self.__class__.__name__,
                     resp.success,
                     resp.error_message)

        return self._to_status_response(resp)

    def disconnect(self):
        resp = self.stub.Disconnect(self.context)
        logger.error("%s.disconnect: returns success - %s, error - %s",
                     self.__class__.__name__,
                     resp.success,
                     resp.error_message)

        return self._to_status_response(resp)

    def native_query(self, query):
        logger.error("%s.native_query: calling for query - %s",
                     self.__class__.__name__,
                     query)
        request = db_pb2.NativeQueryContext(context=self.context, query=query)
        resp = self.stub.NativeQuery(request)
        logger.error("%s.native_query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.error("%s.native_query: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)

    def query(self, query):
        logger.error("%s.query: calling for query - %s",
                     self.__class__.__name__,
                     query)
        query = pickle.dumps(query)
        request = db_pb2.BinaryQueryContext(context=self.context, query=query)
        resp = self.stub.BinaryQuery(request)
        logger.error("%s.query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.error("%s.query: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)


    def get_tables(self):
        logger.error("%s.get_tables: calling",
                     self.__class__.__name__)
        resp = self.stub.GetTables(self.context)
        logger.error("%s.get_tables: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.error("%s.get_tables: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)

    def get_columns(self, table):
        logger.error("%s.get_columns: calling for table - %s",
                     self.__class__.__name__,
                     table)
        request = db_pb2.ColumnsContext(context=self.context, table=table)
        resp = self.stub.GetColumns(request)
        logger.error("%s.get_columns: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.error("%s.get_columns: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)


def test(fail=False):
    if fail:
        handler_type = 'mysql'
    else:
        handler_type = "postgres"
    params = {'connection_data':
              {'user': 'demo_user',
               'password': 'demo_password',
               'host': '3.220.66.106',
               'port': '5432',
               'database': 'demo',
               'dbname': 'demo'
               },
              'integration_id': 6,
              'name': 'example_db',
              }

    client = DBClientGRPC(handler_type, **params)
    res = client.check_connection()
    logger.error("response returned - %s", res)
    query = "SELECT * FROM demo_data.home_rentals LIMIT 10;"
    client.native_query(query)


if __name__ == '__main__':
    test()

    # test(fail=True)
