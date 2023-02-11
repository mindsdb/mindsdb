import os
import json
import pickle
from uuid import uuid4

import grpc
from mindsdb.grpc.executor import executor_pb2_grpc
from mindsdb.grpc.executor import executor_pb2

from mindsdb.utilities.log import get_log
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.libs.handler_helpers import action_logger


logger = get_log("main")


class ExecutorClientGRPC:

    def __init__(self, session, sqlserver):
        self.id = f"executor_{uuid4()}"

        self.sqlserver = sqlserver
        self.session = session
        self.query = None

        # returns
        self.columns = []
        self.params = []
        self.data = None
        self.state_track = None
        self.server_status = None
        self.is_executed = False

        # additional attributes used in handling query process
        self.sql = ""
        self.sql_lower = ""

        host = os.environ.get("MINDSDB_EXECUTOR_SERVICE_HOST", None)
        port = os.environ.get("MINDSDB_EXECUTOR_SERVICE_PORT", None)
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = executor_pb2_grpc.ExecutorServiceStub(self.channel)

    @property
    def _context(self):
        logger.info("%s._context: preparing context. id - %s, session - %s", self.__class__.__name__, self.id, self.session)
        _id = self.id
        connection_id = self.sqlserver.connection_id if hasattr(self.sqlserver, "connection_id") else -1
        session_id = self.session.id
        session = json.dumps(self.session.to_json())
        context = json.dumps(ctx.dump())

        return executor_pb2.ExecutorContext(
            id=_id,
            connection_id=connection_id,
            session_id=session_id,
            session=session,
            context=context,
        )

    def __del__(self):
        self.stub.DeleteExecutor(self._context)

    def _update_attrs(self, response: executor_pb2.ExecutorResponse):
        self.columns = pickle.loads(response.columns)
        self.params = pickle.loads(response.params)
        self.data = pickle.loads(response.data)
        self.state_track = response.state_track
        self.is_executed = response.is_executed
        self.session.from_json(json.loads(response.session))
        logger.info("%s._update_attrs: got data from service - %s", self.__class__.__name__, self.data)

    @action_logger(logger)
    def stmt_prepare(self, sql):
        params = executor_pb2.ExecutionContext(context=self._context, sql=sql)
        resp = self.stub.StatementPrepare(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def stmt_execute(self, param_values):
        if self.is_executed:
            return

        params = executor_pb2.StatementExecuteContext(context=self._context, param_values=json.dumps(param_values))
        resp = self.stub.StatementExecute(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def query_execute(self, sql):
        params = executor_pb2.ExecutionContext(context=self._context, sql=sql)
        resp = self.stub.QueryExecute(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def execute_external(self, sql):
        params = executor_pb2.ExecutionContext(context=self._context, sql=sql)
        resp = self.stub.ExecuteExternal(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def parse(self, sql):
        params = executor_pb2.ExecutionContext(context=self._context, sql=sql)
        resp = self.stub.Parse(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def do_execute(self):
        if self.is_executed:
            return
        resp = self.stub.DoExecute(self._context)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)

    @action_logger(logger)
    def change_default_db(self, new_db):
        params = executor_pb2.DefaultDBContext(context=self._context, new_db=new_db)
        resp = self.stub.ChangeDefaultDB(params)
        if resp.error_message != "":
            raise Exception(resp.error_message)
        self._update_attrs(resp)
