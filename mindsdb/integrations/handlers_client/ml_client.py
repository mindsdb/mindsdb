""" Implementation of client to any supported MLHandler service

The module provide an opportunity to not just create an instance of MLHanlder locally
but communicate to a separate service of MLHandler using MLClient.
MLClient must provide same set of public API methods as MLHandlers do,
including calling params and returning types.

    Typical usage example:
    client = MLClient(HuggingFaceHandler,
                      as_service=True,
                      connection_data={"host": "SERVICE(OR_CONTAINER)HOST",
                                       "port": "SERVICE_PORTS"},
                      company_id=COMPANY_ID,
                      predictor_id=PREDICTOR_ID

    status_response = client.check_connection()
    print(status_response.status, status_response.error_message)

"""
import os
import base64
from typing import Optional, Dict
import traceback
import pickle

import pandas as pd

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log("main")


class MLClient(BaseClient):
    """ The client to connect to MLHanlder service

    MLClient must provide same set of public API methods as MLHandlers do,
    including calling params and returning types.

    Attributes:
        as_service: supports back compatibility with the legacy.
            if False - the MLClient will be just a thin wrapper of MLHanlder instance
            and will redirect all requests to it.
            Connects to a specified MLHandler otherwise
        handler_class: class of MLHandler which supposed to be used
        kwargs:
            connection_data - connection args for to MLHandler service if as_service=True
            predictor_id - id of the used model
            company_id - id of the company.
            Last two args needed to instantiante engine and model storages
    """
    def __init__(self, **handler_kwargs: dict):
        """ Init MLClient

        Args:
        as_service: supports back compatibility with the legacy.
            if False - the MLClient will be just a thin wrapper of MLHanlder instance
            and will redirect all requests to it.
            Connects to a specified MLHandler otherwise
        handler_class: class of MLHandler which supposed to be used
        kwargs:
            connection_data - connection args for to MLHandler service if as_service=True
            predictor_id - id of the used model
            company_id - id of the company.
            Last two args needed to instantiante engine and model storages
        """
        host = os.environ.get("MINDSDB_ML_SERVICE_HOST", None)
        port = os.environ.get("MINDSDB_ML_SERVICE_PORT", None)
        as_service = True
        if host is None or port is None:
            as_service = False
            msg = "No host/port provided to MLHandler service connect to. Working in monolithic mode."
            logger.info("%s __init__: %s", self.__class__.__name__, msg)
        else:
            self.base_url = f"http://{host}:{port}"
            logger.info("%s.__init__: ML Service base url - %s", self.__class__.__name__, self.base_url)
        super().__init__(as_service=as_service)
        # if not self.as_service:
        self.handler = BaseMLEngineExec(**handler_kwargs)
        self.handler_kwargs = handler_kwargs

        # remove all 'object' params from dict before sending it to the serverside.
        # all of them will be created there
        for arg in ("handler_controller", "file_storage", "storage_factory", "handler_class"):
            if arg in self.handler_kwargs:
                del self.handler_kwargs[arg]

    def default_json(self):
        return {
                "context": ctx.dump(),
                "handler_args": self.handler_kwargs,
                }


    # def check_connection(self) -> StatusResponse:
    #     """ Check a connection to the MLHandler.

    #     Returns:
    #         success status and error message if error occurs
    #     """
    #     logger.info("%s: calling 'check_connection'", self.__class__.__name__)
    #     status = None
    #     try:
    #         r = self._do("/check_connection")
    #         r = self._convert_response(r.json())
    #         status = StatusResponse(success=r.get("success", False), error_message=r.get("error_message", ""))
    #         logger.info("%s: ml service has replied", self.__class__.__name__)

    #     except Exception as e:
    #         status = StatusResponse(success=False, error_message=str(e))
    #         logger.error("call to ml service has finished with an error: %s", traceback.format_exc())

    #     return status

    def get_columns(self, table_name: str) -> Response:
        logger.info("%s.get_columns is calling with table_name - %s", self.__class__.__name__, table_name)
        response = None
        try:
            params = self.default_json()
            params["method_params"] = {"table_name": table_name}
            r = self._do("/get_columns", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            logger.info("%s.get_columns: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.get_columns: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())

        return response

    def get_tables(self) -> Response:
        logger.info("%s.get_tables is calling", self.__class__.__name__)
        response = None
        try:
            params = self.default_json()
            params["method_params"] = {}
            r = self._do("/get_tables", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            logger.info("%s.get_tables: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.get_tables: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())

        return response

    def learn(
        self, model_name, project_name,
        data_integration_ref=None,
        fetch_data_query=None,
        problem_definition=None,
        join_learn_process=False,
        label=None,
        version=1,
        is_retrain=False,
        set_active=True,
    ):

        calling_params = {
                "model_name": model_name,
                "project_name": project_name,
                "data_integration_ref": data_integration_ref,
                "fetch_data_query": fetch_data_query,
                "problem_definition": problem_definition,
                "join_learn_process": join_learn_process,
                "label": label,
                "version": version,
                "is_retrain": is_retrain,
                "set_active": set_active,
                }
        logger.info("%s.learn is calling with params - %s", self.__class__.__name__, calling_params)
        response = None
        try:
            params = self.default_json()
            params["method_params"] = calling_params
            r = self._do("/learn", _type="post", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            logger.info("%s.learn: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.learn: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())
        return response

    def native_query(self, query: str) -> Response:
        logger.info("%s.native_query is calling with query - %s", self.__class__.__name__, query)
        response = None
        try:
            params = self.default_json()
            params["method_params"] = {"query": query}
            r = self._do("/native_query", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            logger.info("%s.native_query: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.native_query: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())
        return response

    def predict(self, model_name: str, data: list, pred_format: str = 'dict',
                project_name: str = None, version=None, params: dict = None):

        calling_params = {
                "model_name": model_name,
                "data": data,
                "pred_format": pred_format,
                "project_name": project_name,
                "version": version,
                "params": params,
                }
        logger.info("%s.predict is calling with params - %s", self.__class__.__name__, calling_params)
        response = None
        try:
            params = self.default_json()
            params["method_params"] = calling_params
            r = self._do("/predict", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            if response.error_code != 0 and reponse.error_message is not None:
                logger.error("%s.predict: got error in ml service reply - %s", self.__class__.__name__, response.error_message)
                raise Exception(response.error_message)
            logger.info("%s.predict: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
            r = response.data_frame.to_dict(orient="records")
            logger.info("%s.predict: ml service has replied. predictions - %s(type - %s)", self.__class__.__name__, r, type(r))
            return r
        except Exception as e:
            # response = Response(error_message=str(e),
            #                     error_code=1,
            #                     resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.predict: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())
            raise e

    def query(self, query: ASTNode) -> Response:
        logger.info("%s.query is calling with query - %s", self.__class__.__name__, query)
        response = None
        try:
            query_b = pickle.dumps(query)
            query_b64 = base64.b64encode(query_b)
            s_query = query_b64.decode("utf-8")
            params = self.default_json()
            params["method_params"] = {"query": s_query}
            r = self._do("/query", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            logger.info("%s.query: ml service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            logger.error("%s.query: call to ml service has finished with an error - %s", self.__class__.__name__, traceback.format_exc())
        return response
