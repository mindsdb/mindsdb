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

from pandas import read_json
from flask import Flask, request

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.interfaces.storage.fs import ModelStorage, HandlerStorage
from mindsdb.integrations.libs.handler_helpers import define_ml_handler
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log

log = get_log()


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
            log.error("error getting request context: %s", e)
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
        log.info("%s: got common handler args from request - %s", self.__class__.__name__, common_data)
        handler_class = define_ml_handler(common_data["type"])
        log.info("%s: handler class for '%s' type has defined - %s", self.__class__.__name__, common_data["type"], handler_class)
        return handler_class

    def _get_create_data(self) -> dict:
        """ Get 'create' model params from json in the request."""
        log.info("%s: getting training data from request", self.__class__.__name__)
        data = {"target": request.json.get("target"),
                "args": request.json.get("args", None)}
        df = None
        try:
            df = request.json.get("df", None)
            if df is not None:
                df = read_json(df, orient="split")
        except Exception:
            log.error("%s: error getting training dataframe from request", self.__class__.__name__)
        data["df"] = df

        return data

    def _get_predict_data(self):
        """Get model 'predict' params from json in the request."""
        log.info("%s: getting predict data from request", self.__class__.__name__)
        df = None
        try:
            df = request.json.get("df", None)
            if df is not None:
                df = read_json(df, orient="split")
        except Exception:
            log.error("%s: error getting predict dataframe from request", self.__class__.__name__)
        return {"df": df}

    def _get_handler(self):
        """Create handler instance."""
        # seems that integration_id is senseless for models
        integration_id = 0
        handler_class = self._get_handler_class()
        common_args = self._get_handler_general_data()
        predictor_id = common_args["predictor_id"]
        engineStorage = HandlerStorage(integration_id)
        modelStorage = ModelStorage(predictor_id)
        handler = handler_class(engine_storage=engineStorage,
                                model_storage=modelStorage)
        return handler

    def check_connection(self):
        log.info("%s: calling 'check_connection'", self.__class__.__name__)
        result = StatusResponse(success=True,
                                error_message=None)
        return result.to_json(), 200

    def create(self):
        """See 'create' method in MLHandler"""
        try:
            handler = self._get_handler()
            create_kwargs = self._get_create_data()
            handler.create(**create_kwargs)
            status = StatusResponse(success=True)
            return status.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s error in calling 'create': %s", self.__class__.__name__, msg)
            status = StatusResponse(success=False, error_message=msg)
            return status.to_json(), 500

    def predict(self):
        """See 'predict' method in MLHandler"""
        try:
            handler = self._get_handler()
            predict_kwargs = self._get_predict_data()
            predictions = handler.predict(**predict_kwargs)
            result = Response(resp_type=RESPONSE_TYPE.OK,
                              data_frame=predictions,
                              error_code=0,
                              error_message=None)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s error in calling 'predict': %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500
