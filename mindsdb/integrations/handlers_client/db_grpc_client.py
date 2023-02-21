import os
import pickle
import json

import grpc
from mindsdb.grpc.db import db_pb2_grpc
from mindsdb.grpc.db import db_pb2, common_pb2

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.integrations.libs.handler_helpers import action_logger
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

        # FileController is an object
        # so it is not a good idea to send it
        # to service side as parameter
        # will create a separate instace instead
        if self.handler_type == "files":
            del self.handler_params["file_controller"]
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
    def _to_status_response(response: common_pb2.StatusResponse):
        return StatusResponse(success=response.success,
                              error_message=response.error_message)

    @staticmethod
    def _to_response(response: common_pb2.Response):
        data = pickle.loads(response.data_frame)
        return Response(
            resp_type=response.type,
            data_frame=data,
            query=response.query,
            error_code=response.error_code,
            error_message=response.error_message,
        )

    @action_logger(logger)
    def connect(self):
        resp = self.stub.Connect(self.context)
        logger.info(
            "%s.connect: returns success - %s, error - %s",
            self.__class__.__name__,
            resp.success,
            resp.error_message
        )

        return self._to_status_response(resp)

    @action_logger(logger)
    def check_connection(self):
        resp = self.stub.CheckConnection(self.context)
        logger.info(
            "%s.check_connection: returns success - %s, error - %s",
            self.__class__.__name__,
            resp.success,
            resp.error_message
        )

        return self._to_status_response(resp)

    @action_logger(logger)
    def disconnect(self):
        resp = self.stub.Disconnect(self.context)
        logger.info(
            "%s.disconnect: returns success - %s, error - %s",
            self.__class__.__name__,
            resp.success,
            resp.error_message,
        )

        return self._to_status_response(resp)

    @action_logger(logger)
    def native_query(self, query):
        request = db_pb2.NativeQueryContext(context=self.context, query=query)
        resp = self.stub.NativeQuery(request)
        logger.info("%s.native_query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.info("%s.native_query: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)

    @action_logger(logger)
    def query(self, query):
        query = pickle.dumps(query)
        request = db_pb2.BinaryQueryContext(context=self.context, query=query)
        resp = self.stub.BinaryQuery(request)
        logger.info("%s.query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.info("%s.query: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)

    @action_logger(logger)
    def get_tables(self):
        resp = self.stub.GetTables(self.context)
        logger.info("%s.get_tables: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.info("%s.get_tables: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)

    @action_logger(logger)
    def get_columns(self, table):
        request = db_pb2.ColumnsContext(context=self.context, table=table)
        resp = self.stub.GetColumns(request)
        logger.info("%s.get_columns: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        data = pickle.loads(resp.data_frame)
        logger.info("%s.get_columns: returned data(type of %s) - %s", self.__class__.__name__, type(data), data)

        return self._to_response(resp)
