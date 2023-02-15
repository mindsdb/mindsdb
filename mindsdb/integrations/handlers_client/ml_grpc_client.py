import json
from typing import Optional
import pickle

import grpc
from mindsdb.grpc.ml import ml_pb2_grpc
from mindsdb.grpc.ml import ml_pb2, common_pb2

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.libs.handler_helpers import action_logger
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


class MLClientGRPC:
    def __init__(self, host, port, handler_params: dict):
        self.host = host
        self.port = port
        self.handler_params = handler_params
        # have to create a handler instance
        # because Executor accesses to some handler attributes
        # directly
        self.handler = BaseMLEngineExec(**self.handler_params)
        # remove all 'object' params from dict before sending it to the serverside.
        # all of them will be created there
        for arg in (
            "handler_controller",
            "file_storage",
            "storage_factory",
            "handler_class",
        ):
            if arg in self.handler_params:
                del self.handler_params[arg]

        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.stub = ml_pb2_grpc.MLServiceStub(self.channel)

    def __del__(self):
        self.channel.close()

    @property
    def database_controller(self):
        return self.handler.database_controller

    @property
    def handler_controller(self):
        return self.handler.handler_controller

    @property
    def context(self):
        ctx_str = json.dumps(ctx.dump())
        return ml_pb2.HandlerContextML(
            handler_params=json.dumps(self.handler_params),
            context=ctx_str,
        )

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
    def native_query(self, query):
        request = ml_pb2.NativeQueryContextML(context=self.context, query=query)
        resp = self.stub.NativeQuery(request)
        logger.info("%s.native_query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)

        return self._to_response(resp)

    @action_logger(logger)
    def query(self, query):
        query = pickle.dumps(query)
        request = ml_pb2.BinaryQueryContextML(context=self.context, query=query)
        resp = self.stub.BinaryQuery(request)
        logger.info("%s.query: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)

        return self._to_response(resp)

    @action_logger(logger)
    def get_tables(self):
        resp = self.stub.GetTables(self.context)
        logger.info("%s.get_tables: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)

        return self._to_response(resp)

    @action_logger(logger)
    def get_columns(self, table):
        request = ml_pb2.ColumnsContextML(context=self.context, table=table)
        resp = self.stub.GetColumns(request)
        logger.info("%s.get_columns: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)

        return self._to_response(resp)

    @action_logger(logger)
    def predict(
        self,
        model_name: str,
        data: list,
        pred_format: str = "dict",
        project_name: str = None,
        version=None,
        params: dict = None,
    ):
        predict_params = ml_pb2.PredictParams(
            model_name=model_name,
            data=pickle.dumps(data),
            pred_format=pred_format,
            project_name=project_name,
            version=version,
            params=json.dumps(params),
        )
        request = ml_pb2.PredictContextML(context=self.context, predict_params=predict_params)
        resp = self.stub.Predict(request)

        logger.info("%s.learn: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        if resp.error_code and resp.error_message:
            raise Exception(resp.error_message)

        return self._to_response(resp).data_frame

    @action_logger(logger)
    def learn(
        self,
        model_name,
        project_name,
        data_integration_ref=None,
        fetch_data_query=None,
        problem_definition=None,
        join_learn_process=False,
        label=None,
        version=1,
        is_retrain=False,
        set_active=True,
    ):

        learn_params = ml_pb2.LearnParams(
            model_name=model_name,
            project_name=project_name,
            data_integration_ref=json.dumps(data_integration_ref),
            fetch_data_query=fetch_data_query,
            problem_definition=json.dumps(problem_definition),
            join_learn_process=join_learn_process,
            label=label,
            version=version,
            is_retrain=is_retrain,
            set_active=set_active,
        )
        request = ml_pb2.LearnContextML(context=self.context, learn_params=learn_params)
        resp = self.stub.Learn(request)

        logger.info("%s.learn: success - %s", self.__class__.__name__, resp.success)
        if not resp.success:
            logger.error("%s.learn: returned error - %s", self.__class__.__name__, resp.error_message)
            raise Exception(resp.error_message)

    @action_logger(logger)
    def update(
            self, model_name, project_name, version,
            data_integration_ref=None,
            fetch_data_query=None,
            join_learn_process=False,
            label=None,
            set_active=True,
            args: Optional[dict] = None
    ):

        update_params = ml_pb2.UpdateParams(
            model_name=model_name,
            project_name=project_name,
            data_integration_ref=json.dumps(data_integration_ref),
            fetch_data_query=fetch_data_query,
            join_learn_process=join_learn_process,
            label=label,
            version=version,
            set_active=set_active,
        )
        request = ml_pb2.UpdateContextML(context=self.context, update_params=update_params)
        resp = self.stub.Update(request)

        logger.info("%s.update: success - %s", self.__class__.__name__, resp.success)
        if not resp.success:
            logger.error("%s.update: returned error - %s", self.__class__.__name__, resp.error_message)
            raise Exception(resp.error_message)
