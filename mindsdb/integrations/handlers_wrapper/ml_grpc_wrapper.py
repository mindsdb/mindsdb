import json
import pickle
import traceback
from concurrent import futures

import grpc
from mindsdb.grpc.ml import ml_pb2_grpc
from mindsdb.grpc.ml import ml_pb2, common_pb2

from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.interfaces.storage.fs import (
    FileStorage,
    FileStorageFactory,
    RESOURCE_GROUP,
)
from mindsdb.integrations.libs.response import (
    RESPONSE_TYPE,
)
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


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
        return IntegrationController()

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
        integration_id = params["integration_id"]
        _type = params.get("integration_engine") or params.get("name", None)
        logger.info(
            "%s.get_handler: request handler of type - %s, context - %s",
            self.__class__.__name__,
            _type,
            handler_context.context,
        )

        handler_class = get_handler(_type)
        params["handler_class"] = handler_class
        params["handler_controller"] = self._get_handler_controller()
        params["file_storage"] = self._get_file_storage(integration_id)
        params["storage_factory"] = self._get_storage_factory()

        logger.info(
            "%s._get_handler: create handler. params - %s",
            self.__class__.__name__,
            params,
        )

        return BaseMLEngineExec(**params)

    def GetColumns(self, request, context):
        logger.info(
            "%s.GetColumns: calling for table - %s",
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
            logger.error("%s.GetColumns: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    def GetTables(self, request, context):

        logger.info(
            "%s.GetTables: calling",
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
            logger.error("%s.GetTables: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg
            )
        return result

    def NativeQuery(self, request, context):

        result = None
        query = request.query

        logger.info(
            "%s.NativeQuery: calling 'native_query' with query - %s",
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
                error_message=res.error_message,
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.NativeQuery: error - %s", self.__class__.__name__, msg)
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
                "%s.BinaryQuery: calling 'query' with query - %s",
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
                error_message=res.error_message,
            )

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.BinaryQuery: error - %s", self.__class__.__name__, msg)
            result = common_pb2.Response(
                type=RESPONSE_TYPE.ERROR,
                data_frame=None,
                query=0,
                error_code=1,
                error_message=msg,
            )
        return result

    @staticmethod
    def _params_predict_to_dict(p_params: ml_pb2.PredictParams):
        res = {
            "model_name": p_params.model_name,
            "data": pickle.loads(p_params.data),
            "pred_format": p_params.pred_format,
            "project_name": p_params.project_name,
            "version": p_params.version if p_params.version > 0 else None,
            "params": json.loads(p_params.params),
        }
        return res

    def Predict(self, request, context):
        result = None
        try:
            params = self._params_predict_to_dict(request.predict_params)
            logger.info(
                "%s.Predict: calling 'predict' with params - %s",
                self.__class__.__name__,
                params,
            )

            handler = self._get_handler(request.context)
            predictions = handler.predict(**params)

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

    @staticmethod
    def _params_learn_to_dict(l_params: ml_pb2.LearnParams):
        res = {
            "model_name": l_params.model_name,
            "project_name": l_params.project_name,
            "data_integration_ref": json.loads(l_params.data_integration_ref),
            "fetch_data_query": l_params.fetch_data_query,
            "problem_definition": json.loads(l_params.problem_definition),
            "join_learn_process": l_params.join_learn_process,
            "label": l_params.label,
            "version": l_params.version if l_params.version > 0 else None,
            "is_retrain": l_params.is_retrain,
            "set_active": l_params.set_active,
        }
        return res

    def Learn(self, request, context):
        result = None
        try:
            params = self._params_learn_to_dict(request.learn_params)
            logger.info(
                "%s.Learn: calling 'learn' with params - %s",
                self.__class__.__name__,
                params,
            )

            handler = self._get_handler(request.context)
            handler.learn(**params)

            result = common_pb2.StatusResponse(success=True, error_message="")

        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.Learn: error - %s", self.__class__.__name__, msg)
            result = common_pb2.StatusResponse(success=False, error_message=msg)

        return result

    @staticmethod
    def _params_update_to_dict(u_params: ml_pb2.LearnParams):
        res = {
            "model_name": u_params.model_name,
            "project_name": u_params.project_name,
            "data_integration_ref": json.loads(u_params.data_integration_ref),
            "fetch_data_query": u_params.fetch_data_query,
            "join_learn_process": u_params.join_learn_process,
            "label": u_params.label,
            "version": u_params.version if u_params.version > 0 else None,
            "set_active": u_params.set_active,
        }
        return res

    def Update(self, request, context):

        result = None
        try:
            params = self._params_update_to_dict(request.update_params)
            logger.info(
                "%s.Update: calling 'update' with params - %s",
                self.__class__.__name__,
                params,
            )

            handler = self._get_handler(request.context)
            handler.update(**params)

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
    logger.info("staring rpc server on [::]:50052")
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
