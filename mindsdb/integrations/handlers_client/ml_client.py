""" Implementation of client to any supported MLHandler service

The module provide an opportunity to not just create an instance of MLHanlder locally
but communicate to a separate service of MLHandler using MLClient.
MLClient must provide same set of public API methods as MLHandlers do,
including calling params and returning types.

    Typical usage example:
    client = MLClient(**handler_kwargs)
    result = client.get_tables()
    print(status_response.status, status_response.error_message)

"""
import os
import base64
import traceback
from typing import Optional

import pickle


from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers_client.base_client import BaseClient, Switcher
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.integrations.libs.handler_helpers import action_logger
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log


logger = get_log(logger_name="main")


@Switcher
class MLClient(BaseClient):
    """The client to connect to MLHanlder service

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
        """Init MLClient

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
        base_url = os.environ.get("MINDSDB_ML_SERVICE_URL", None)
        as_service = True
        if base_url is None:
            as_service = False
            msg = "No url provided to MLHandler service connect to. Working in monolithic mode."
            logger.info("%s __init__: %s", self.__class__.__name__, msg)
        else:
            self.base_url = base_url
            logger.info(
                "%s.__init__: ML Service base url - %s",
                self.__class__.__name__,
                self.base_url,
            )
        super().__init__(as_service=as_service)

        # need always instantiate handler instance
        self.handler = BaseMLEngineExec(**handler_kwargs)
        self.handler_kwargs = handler_kwargs

        # remove all 'object' params from dict before sending it to the serverside.
        # all of them will be created there
        for arg in (
            "handler_controller",
            "file_storage",
            "storage_factory",
            "handler_class",
        ):
            if arg in self.handler_kwargs:
                del self.handler_kwargs[arg]

    def _default_json(self):
        return {
            "context": ctx.dump(),
            "handler_args": self.handler_kwargs,
        }

    @action_logger(logger)
    def get_columns(self, table_name: str) -> Response:
        response = None
        try:
            params = self._default_json()
            params["method_params"] = {"table_name": table_name}
            r = self._do("/get_columns", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.get_columns: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.get_columns: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

        return response

    @action_logger(logger)
    def get_tables(self) -> Response:
        response = None
        try:
            params = self._default_json()
            params["method_params"] = {}
            r = self._do("/get_tables", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.get_tables: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.get_tables: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

        return response

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
        response = None
        try:
            params = self._default_json()
            params["method_params"] = calling_params
            r = self._do("/learn", _type="post", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.learn: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.learn: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )
        return response

    @action_logger(logger)
    def native_query(self, query: str) -> Response:
        response = None
        try:
            params = self._default_json()
            params["method_params"] = {"query": query}
            r = self._do("/native_query", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.native_query: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.native_query: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )
        return response

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

        calling_params = {
            "model_name": model_name,
            "data": data,
            "pred_format": pred_format,
            "project_name": project_name,
            "version": version,
            "params": params,
        }
        response = None
        try:
            params = self._default_json()
            params["method_params"] = calling_params
            r = self._do("/predict", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            if response.error_code != 0 and response.error_message is not None:
                logger.error(
                    "%s.predict: got error in ml service reply - %s",
                    self.__class__.__name__,
                    response.error_message,
                )
                raise Exception(response.error_message)
            logger.info(
                "%s.predict: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
            predictions = response.data_frame
            logger.info(
                "%s.predict: ml service has replied. predictions - %s(type - %s)",
                self.__class__.__name__,
                predictions,
                type(predictions),
            )
            return predictions
        except Exception as e:
            logger.error(
                "%s.predict: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )
            raise e

    @action_logger(logger)
    def query(self, query: ASTNode) -> Response:
        response = None
        try:
            query_b = pickle.dumps(query)
            query_b64 = base64.b64encode(query_b)
            s_query = query_b64.decode("utf-8")
            params = self._default_json()
            params["method_params"] = {"query": s_query}
            r = self._do("/query", json=params)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.query: ml service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.query: call to ml service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )
        return response

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
        # TODO: add implementation
        raise NotImplementedError
