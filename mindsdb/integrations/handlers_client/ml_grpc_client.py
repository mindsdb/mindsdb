import json
import pickle

import grpc
from mindsdb.microservices_grpc.ml import ml_pb2_grpc
from mindsdb.microservices_grpc.ml import ml_pb2, common_pb2

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
)

# from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.libs.handler_helpers import action_logger
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class MLClientGRPC:
    def __init__(self, host, port, **handler_params):
        self.host = host
        self.port = port
        self.handler_params = handler_params
        self.integration_id = handler_params.get("integration_id")
        self.predictor_id = handler_params.get("predictor_id")
        for key in ("integration_id", "predictor_id"):
            try:
                del self.handler_params[key]
            except Exception:
                pass
        # have to create a handler instance
        # because Executor accesses to some handler attributes
        # directly
        # self.handler = BaseMLEngineExec(**self.handler_params)
        # remove all 'object' params from dict before sending it to the serverside.
        # all of them will be created there
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        self.stub = ml_pb2_grpc.MLServiceStub(self.channel)

    def __del__(self):
        if hasattr(self, "channel"):
            self.channel.close()

    @property
    def context(self):
        return ml_pb2.HandlerContextML(
            predictor_id=self.predictor_id,
            integration_id=self.integration_id,
            context=json.dumps(ctx.dump()),
            handler_params=json.dumps(self.handler_params),
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
    def predict(
        self,
        df,
        args
    ):
        request = ml_pb2.PredictCall(context=self.context, df=pickle.dumps(df), args=json.dumps(args))
        resp = self.stub.Predict(request)

        logger.info("%s.learn: returned error - %s, error_message - %s", self.__class__.__name__, resp.error_code, resp.error_message)
        if resp.error_code and resp.error_message:
            raise Exception(resp.error_message)

        return pickle.loads(resp.data_frame)

    @action_logger(logger)
    def create(
            self,
            target,
            df,
            args
    ):

        request = ml_pb2.CreateCall(context=self.context, target=target, df=pickle.dumps(df), args=json.dumps(args))
        resp = self.stub.Create(request)

        logger.info("%s.learn: success - %s", self.__class__.__name__, resp.success)
        if not resp.success:
            logger.error("%s.learn: returned error - %s", self.__class__.__name__, resp.error_message)
            raise Exception(resp.error_message)

    @action_logger(logger)
    def update(
        self,
        df,
        args
    ):

        request = ml_pb2.UpdateCall(context=self.context, df=pickle.dumps(df), args=json.dumps(args))
        resp = self.stub.Update(request)

        logger.info("%s.update: success - %s", self.__class__.__name__, resp.success)
        if not resp.success:
            logger.error("%s.update: returned error - %s", self.__class__.__name__, resp.error_message)
            raise Exception(resp.error_message)

    @action_logger(logger)
    def close(self):
        pass
