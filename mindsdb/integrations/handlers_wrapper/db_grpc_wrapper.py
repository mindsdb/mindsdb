import json
import pickle
import traceback
from concurrent import futures

import grpc
from mindsdb.grpc.db import db_pb2_grpc
from mindsdb.grpc.db import db_pb2, common_pb2

from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class DBServiceServicer(db_pb2_grpc.DBServiceServicer):

    def __init__(self):
        logger.info(
            "%s.__init__: ", self.__class__.__name__
        )

    def _get_handler(self, handler_ctx: db_pb2.HandlerContext):
        ctx.load(json.loads(handler_ctx.context))
        handler_class = get_handler(handler_ctx.handler_type)
        logger.info(
            "%s._get_handler: requested instance of %s handler",
            self.__class__.__name__,
            handler_class,
        )
        handler_kwargs = json.loads(handler_ctx.handler_params)
        # Create an instance of FileController for
        # 'files' type of handler
        if handler_ctx == "files":
            handler_kwargs["file_controller"] = FileController()
        return handler_class(**handler_kwargs)

    def CheckConnection(self, request, context):

        result = None
        logger.info(
            "%s.check_connection calling", self.__class__.__name__
        )
        try:
            handler = self._get_handler(request)
            res = handler.check_connection()
            result = common_pb2.StatusResponse(success=res.success, error_message=res.error_message)
        except Exception:
            msg = traceback.format_exc()
            result = common_pb2.StatusResponse(success=False, error_message=msg)
        return result

    def Connect(self, request, context):

        result = None
        logger.info(
            "%s.connect calling", self.__class__.__name__
        )
        try:
            handler = self._get_handler(request)
            handler.connect()
            result = common_pb2.StatusResponse(success=True, error_message="")
        except Exception:
            msg = traceback.format_exc()
            result = common_pb2.StatusResponse(success=False, error_message=msg)
        return result

    def Disconnect(self, request, context):
        result = None
        logger.info(
            "%s.disconnect calling", self.__class__.__name__
        )
        try:
            handler = self._get_handler(request)
            handler.disconnect()
            result = common_pb2.StatusResponse(success=True, error_message="")
        except Exception:
            msg = traceback.format_exc()
            result = common_pb2.StatusResponse(success=False, error_message=msg)
        return result

    def NativeQuery(self, request, context):

        result = None
        query = request.query

        logger.info(
            "%s.native_query: calling 'native_query' with query - %s",
            self.__class__.__name__,
            query,
        )
        try:
            handler = self._get_handler(request.context)
            res = handler.native_query(query)
            data = pickle.dumps(res.data_frame)
            result = common_pb2.Response(
                type=res.resp_type,
                data_frame=data,
                query=res.query,
                error_code=res.error_code,
                error_message=res.error_message
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.native_query: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    def BinaryQuery(self, request, context):

        result = None
        try:
            query = pickle.loads(request.query)

            logger.info(
                "%s.query: calling 'query' with query - %s",
                self.__class__.__name__,
                query,
            )
            handler = self._get_handler(request.context)
            res = handler.query(query)
            data = pickle.dumps(res.data_frame)
            result = common_pb2.Response(
                type=res.resp_type,
                data_frame=data,
                query=res.query,
                error_code=res.error_code,
                error_message=res.error_message
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.query: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg
            )
        return result

    def GetTables(self, request, context):

        logger.info(
            "%s.get_tables: calling",
            self.__class__.__name__,
        )
        result = None
        try:
            handler = self._get_handler(request)
            res = handler.get_tables()
            data = pickle.dumps(res.data_frame)
            result = common_pb2.Response(
                type=res.resp_type,
                data_frame=data,
                query=res.query,
                error_code=res.error_code,
                error_message=res.error_message,
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.get_tables: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    def GetColumns(self, request, context):

        logger.info(
            "%s.get_columns: calling for table - %s",
            self.__class__.__name__,
            request.table,
        )
        result = None
        try:
            handler = self._get_handler(request.context)
            res = handler.get_columns(request.table)
            data = pickle.dumps(res.data_frame)
            result = common_pb2.Response(
                type=res.resp_type,
                data_frame=data,
                query=res.query,
                error_code=res.error_code,
                error_message=res.error_message,
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.get_tables: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    def run(self, **kwargs):
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 50051)
        addr = f"{host}:{port}"
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        db_pb2_grpc.add_DBServiceServicer_to_server(
            DBServiceServicer(), server)
        server.add_insecure_port(addr)
        # logger.info("staring rpc server on [::]:50051")
        server.start()
        server.wait_for_termination()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    db_pb2_grpc.add_DBServiceServicer_to_server(
        DBServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    logger.info("staring rpc server on [::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
