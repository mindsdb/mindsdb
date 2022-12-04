"""Implementation of REST API wrapper for any supported MLHandler.
The module provide an opportunity to convert a MLHandler into REST API service
Basic usage:
    app = MLHandlerWrapper("foo handler")
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    log.info("Running dbservice: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)

"""
import traceback
import json
import base64
import pickle

from pandas import read_json, DataFrame

from flask import Flask, request

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.interfaces.storage.fs import FsStore, FileStorage, FileStorageFactory, RESOURCE_GROUP
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log

logger = get_log()


class BaseMLWrapper:
    """Base abstract class contains some general methods."""
    def __init__(self, name):
        self.app = Flask(name)
        self.__mdb_config = Config()

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)

    def _get_context(self):
        try:
            context = request.json.get("context")
        except Exception as e:
            logger.error("error getting request context: %s", e)
            context = {}
        ctx_str = json.dumps(context, sort_keys=True)
        return {"hash": hash(ctx_str), "context": context}

    def index(self):
        """ Default GET endpoint - '/'."""
        return "A ML Service Wrapper", 200

    def run(self, **kwargs):
        """ Launch internal Flask application."""
        return self.app.run(**kwargs)


class MLHandlerWrapper(BaseMLWrapper):
    def __init__(self, name):
        """ Wrapper Init.
        Args:
            name: name of the service
        """
        super().__init__(name)

        # CONVERT METHODS TO FLASK API ENDPOINTS
        check_connect_route = self.app.route("/check_connection", methods=["GET", ])
        self.check_connection = check_connect_route(self.check_connection)

        predict_route = self.app.route("/predict", methods=["GET", ])
        self.predict = predict_route(self.predict)

        query_create = self.app.route("/create", methods=["POST", ])
        self.create = query_create(self.create)

    def _get_handler_controller(self):
        return IntegrationController()

    def _get_file_storage(self, integration_id):
        fs_store = FileStorage(
            resource_group=RESOURCE_GROUP.INTEGRATION,
            resource_id=integration_id,
            sync=True,
        )
    def _get_storage_factory(self):
        return FileStorageFactory(
            resource_group=RESOURCE_GROUP.PREDICTOR,
            sync=True
        )



    def _get_handler_general_data(self) -> dict:
        """ Get common data for creating MLHandler instance from the request. """
        return {"company_id": request.json.get("company_id", None),
                "predictor_id": request.json.get("predictor_id", None),
                "type": request.json.get("type")}

    def _get_handler_class(self):
        """ Specify handler class base on common handler data.
        The method gets the data from the request json.
        """
        common_data = self._get_handler_general_data()
        logger.info("%s: got common handler args from request - %s", self.__class__.__name__, common_data)
        handler_class = define_ml_handler(common_data["type"])
        logger.info("%s: handler class for '%s' type has defined - %s", self.__class__.__name__, common_data["type"], handler_class)
        return handler_class

    def _get_create_data(self) -> dict:
        """ Get 'create' model params from json in the request."""
        logger.info("%s: getting training data from request", self.__class__.__name__)
        data = {"target": request.json.get("target"),
                "args": request.json.get("args", None)}
        df = None
        try:
            df = request.json.get("df", None)
            if df is not None:
                df = read_json(df, orient="split")
        except Exception:
            logger.error("%s: error getting training dataframe from request", self.__class__.__name__)
        data["df"] = df

        return data

    def _get_predict_data(self):
        """Get model 'predict' params from json in the request."""
        logger.info("%s: getting predict data from request", self.__class__.__name__)
        df = None
        try:
            df = request.json.get("df", None)
            if df is not None:
                df = read_json(df, orient="split")
        except Exception:
            logger.error("%s: error getting predict dataframe from request", self.__class__.__name__)
        return {"df": df}

    def _get_handler(self):
        """Create handler instance."""
        # seems that integration_id is senseless for models
        ctx.load(request.json.get("context"))
        params = request.json.get("handler_args")
        integration_id = params["integration_id"]
        _type = params["engine"]
        handler_class = get_handler(_type)
        params["handler_class"] = handler_class
        params["handler_controller"] = self._get_handler_controller()
        params["file_storage"] = self._get_file_storage(integration_id)
        params["storage_factory"] = self._get_storage_factory()
        logger.info("%s._get_handler: create handler. params - %s", self.__class__.__name__, params)
        return BaseMLEngineExec(**params)
        # common_args = self._get_handler_general_data()
        # predictor_id = common_args["predictor_id"]
        # engineStorage = HandlerStorage(integration_id)
        # modelStorage = ModelStorage(predictor_id)
        # handler = handler_class(engine_storage=engineStorage,
                                # model_storage=modelStorage)

        # return handler

    # def check_connection(self):
    #     logger.info("%s: calling 'check_connection'", self.__class__.__name__)
    #     result = StatusResponse(success=True,
    #                             error_message=None)
    #     return result.to_json(), 200

    def get_columns(self):
        logger.info("%s.get_columns has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            table_name = params.get("table_name")
            logger.info("%s.get_columns - called for table - %s", self.__class__.__name__, table_name)
            handler = self._get_handler()
            result = handler.get_columns(table_name)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'get_columns': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_tables(self):
        logger.info("%s.get_tables has called.", self.__class__.__name__)
        try:
            handler = self._get_handler()
            result = handler.get_tables()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'get_tables': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def native_query(self):
        logger.info("%s.native_query has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            query = params.get("query")
            logger.info("%s.native_query: called for query - %s", self.__class__.__name__, query)
            handler = self._get_handler()
            result = handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'native_query': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def query(self):
        logger.info("%s.query has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            query_s64 = params.get("query")
            query_b64 = query_s64.encode("utf-8")
            query_s = base64.b64decode(query_b64)
            query = pickle.loads(query_s)
            logger.info("%s.query: called for query - %s", self.__class__.__name__, query)
            handler = self._get_handler()
            result = handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'query': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def predict(self):
        """See 'predict' method in MLHandler"""
        logger.info("%s.predict has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            logger.info("%s.predict: called with params - %s", self.__class__.__name__, params)
            handler = self._get_handler()
            # predict_kwargs = self._get_predict_data()
            predictions = handler.predict(**params)
            predictions = DataFrame.from_dict(predictions, orient="columns")
            result = Response(resp_type=RESPONSE_TYPE.OK,
                              data_frame=predictions,
                              error_code=0,
                              error_message=None)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'predict': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def learn(self):
        logger.info("%s.learn has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            logger.info("%s.learn: called with params - %s", self.__class__.__name__, params)
            handler = self._get_handler()
            handler.learn(**params)
            result = Response(resp_type=RESPONSE_TYPE.OK,
                              data_frame=None,
                              error_code=0,
                              error_message=None)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s error in calling 'query': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500
