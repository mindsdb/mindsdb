import json
import pickle
import traceback
from concurrent import futures

import grpc
from mindsdb.microservices_grpc.ml import ml_pb2_grpc
from mindsdb.microservices_grpc.ml import ml_pb2, common_pb2

from mindsdb.interfaces.storage.fs import (
    FileStorage,
    FileStorageFactory,
    RESOURCE_GROUP,
)
from mindsdb.interfaces.storage.model_fs import ModelStorage, HandlerStorage

from mindsdb.integrations.libs.response import (
    RESPONSE_TYPE,
)
from mindsdb.interfaces.database.integrations import integration_controller
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class MLServiceServicer(ml_pb2_grpc.MLServiceServicer):

    def __init__(self):
        logger.info(
            "%s.__init__: ", self.__class__.__name__
        )

    def run(self, **kwargs):
        host = kwargs.get("host", "127.0.0.1")
        port = kwargs.get("port", 50052)
        addr = f"{host}:{port}"
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        ml_pb2_grpc.add_MLServiceServicer_to_server(
            MLServiceServicer(), server)
        server.add_insecure_port(addr)
        server.start()
        server.wait_for_termination()

    def _get_handler_controller(self):
        return integration_controller

    def _get_file_storage(self, integration_id):
        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_id,
            sync=True,
        )
        return fs_store

    def _get_storage_factory(self):
        return FileStorageFactory(resource_group=RESOURCE_GROUP.PREDICTOR, sync=True)

    def _get_handler(self, handler_context: ml_pb2.HandlerContextML):
        ctx.load(json.loads(handler_context.context))
        params = json.loads(handler_context.handler_params)
        logger.info(
            "%s._get_handler: create handler. params - %s",
            self.__class__.__name__,
            params,
        )
        integration_id = handler_context.integration_id
        predictor_id = handler_context.predictor_id
        _type = params.get("engine")
        del params["engine"]
        logger.info(
            "%s.get_handler: request handler of type - %s, context - %s",
            self.__class__.__name__,
            _type,
            params,
        )

        HandlerClass = get_handler(_type)
        handlerStorage = HandlerStorage(integration_id)
        modelStorage = ModelStorage(predictor_id)

        ml_handler = HandlerClass(
            engine_storage=handlerStorage,
            model_storage=modelStorage,
            **params
        )
        return ml_handler

    def Predict(self, request, context):
        result = None
        try:
            args = json.loads(request.args)
            df = pickle.loads(request.df)
            logger.info(
                "%s.Predict: args - %s",
                self.__class__.__name__,
                args,
            )

            handler = self._get_handler(request.context)
            predictions = handler.predict(df, args)
            handler.close()

            logger.info(
                "%s.Predict: got predictions - %s(type - %s)",
                self.__class__.__name__,
                predictions,
                type(predictions),
            )
            predictions = pickle.dumps(predictions)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.OK,
                data_frame=predictions,
                query=0,
                error_code=0,
                error_message="",
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.Predict: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    def Create(self, request, context):
        result = None
        try:
            args = json.loads(request.args)
            target = request.target
            df = pickle.loads(request.df)
            logger.info(
                "%s.Create: target - %s df - %s, args - %s",
                self.__class__.__name__,
                target,
                df,
                args,
            )

            handler = self._get_handler(request.context)
            handler.create(target, df=df, args=args)

            result = common_pb2.StatusResponse(success=True, error_message="")

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.Create: error - %s", self.__class__.__name__, msg)
            result = common_pb2.StatusResponse(success=False, error_message=msg)

        return result

    def Update(self, request, context):

        result = None
        try:
            args = json.loads(request.args)
            df = pickle.loads(request.df)
            logger.info(
                "%s.Update: args - %s",
                self.__class__.__name__,
                args,
            )

            handler = self._get_handler(request.context)
            handler.update(df, args)

            result = common_pb2.StatusResponse(success=True, error_message="")

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.Update: error - %s", self.__class__.__name__, msg)
            result = common_pb2.StatusResponse(success=False, error_message=msg)

        return result


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ml_pb2_grpc.add_MLServiceServicer_to_server(
        MLServiceServicer(), server)
    server.add_insecure_port('[::]:50052')
    print("staring rpc server on [::]:50052")
    logger.info("staring rpc server on [::]:50052")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
