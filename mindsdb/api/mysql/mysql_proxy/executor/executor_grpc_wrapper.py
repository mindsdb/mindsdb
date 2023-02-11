import json
import pickle
import traceback
from concurrent import futures

import grpc
from mindsdb.grpc.executor import executor_pb2_grpc
from mindsdb.grpc.executor import executor_pb2

from mindsdb.utilities.context import context as ctx
from mindsdb.api.mysql.mysql_proxy.executor.executor import Executor
from mindsdb.api.mysql.mysql_proxy.controllers.session_controller import (
    SessionController,
)
from mindsdb.utilities.log import (
    get_log,
)

logger = get_log(logger_name="main")


class SqlServerStub:
    """This class is just an emulation of Server object,
    used by Executor.
    In 'monolithic' mode of MindsDB work the Executor takes
    some information from the sql server which. Here we emulate
    this object."""

    def __init__(self, **kwargs):
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])


class ExecutorServiceServicer(executor_pb2_grpc.ExecutorServiceServicer):

    def __init__(self):
        logger.info(
            "%s.__init__: ", self.__class__.__name__
        )
        self.executors_cache = {}
        self.sessions_cache = {}

    def run(self, **kwargs):
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 50051)
        addr = f"{host}:{port}"
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        executor_pb2_grpc.add_ExecutorServiceServicer_to_server(
            ExecutorServiceServicer(), server)
        server.add_insecure_port(addr)
        server.start()
        server.wait_for_termination()

    def _get_executor(self, params: executor_pb2.ExecutorContext):
        # We have to send context between client and server
        # here we load the context json received from the client(mindsdb)
        # to the local context instance in this Flask thread
        ctx.load(json.loads(params.context))
        exec_id = params.id
        if exec_id in self.executors_cache:
            logger.info(
                "%s: executor %s found in cache", self.__class__.__name__, exec_id
            )
            return self.executors_cache[exec_id]
        session_id = params.session_id
        if session_id in self.sessions_cache:
            logger.info(
                "%s: session %s found in cache", self.__class__.__name__, session_id
            )
            session = self.sessions_cache[session_id]
        else:
            logger.info(
                "%s: creating new session. id - %s, params - %s",
                self.__class__.__name__,
                session_id,
                params.session,
            )
            session = SessionController()
            self.sessions_cache[session_id] = session
        session_params = json.loads(params.session)
        session.database = session_params["database"]
        session.username = session_params["username"]
        session.auth = session_params["auth"]
        session.prepared_stmts = session_params["prepared_stmts"]
        session.packet_sequence_number = session_params["packet_sequence_number"]
        sqlserver = SqlServerStub(connection_id=params.connection_id)

        logger.info(
            "%s: session info - id=%s, params=%s",
            self.__class__.__name__,
            session_id,
            session.to_json(),
        )
        logger.info(
            "%s: creating new executor. id - %s, session_id - %s",
            self.__class__.__name__,
            exec_id,
            session_id,
        )
        executor = Executor(session, sqlserver)
        self.executors_cache[exec_id] = executor
        return executor

    @staticmethod
    def _prepare_response(executor):
        columns = pickle.dumps(executor.columns)
        params = pickle.dumps(executor.params)
        data = pickle.dumps(executor.data)
        state_track = executor.state_track
        server_status = executor.server_status
        is_executed = executor.is_executed
        session = json.dumps(executor.session.to_json())

        res = executor_pb2.ExecutorResponse(
            columns=columns,
            params=params,
            data=data,
            state_track=state_track,
            server_status=server_status,
            is_executed=is_executed,
            session=session,
        )
        return res

    def DeleteExecutor(self, request, context):
        # to delete executors
        exec_id = request.id
        session_id = request.session_id
        logger.info(
            "%s: removing executor instance. id - %s", self.__class__.__name__, exec_id
        )
        if exec_id is not None and exec_id in self.executors_cache:
            del self.executors_cache[exec_id]

        if session_id is not None and session_id in self.sessions_cache:
            del self.sessions_cache[session_id]

        return executor_pb2.ExecutorStatusResponse(success=True, error_message="")

    def StatementPrepare(self, request, context):
        result = None
        try:
            params = request.json
            executor = self._get_executor(params)
            sql = params.get("sql")
            executor.stmt_prepare(sql)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def StatementExecute(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            param_values = json.loads(request.param_values)
            executor.stmt_execute(param_values)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def QueryExecute(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            sql = request.sql
            executor.query_execute(sql)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def ExecuteExternal(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            sql = request.sql
            executor.execute_external(sql)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def Parse(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            sql = request.sql
            executor.parse(sql)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def DoExecute(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            executor.do_execute()
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result

    def ChangeDefaultDB(self, request, context):
        result = None
        try:
            executor = self._get_executor(request.context)
            new_db = request.new_db
            executor.change_default_db(new_db)
            result = self._prepare_response(executor)
        except Exception:
            err_msg = traceback.format_exc()
            result = executor_pb2.ExecutorResponse(error_message=err_msg)
        return result


def serve():
    host = "127.0.0.1"
    port = 50052
    addr = f"{host}:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    executor_pb2_grpc.add_ExecutorServiceServicer_to_server(
        ExecutorServiceServicer(), server)
    server.add_insecure_port(addr)
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
