import traceback
import pickle
import json
from flask import Flask, request

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage.fs import FsStore
from mindsdb.interfaces.model.model_controller import ModelController
from mindsdb.utilities.with_kwargs_wrapper import WithKWArgsWrapper

from mindsdb.integrations.libs.handler_helpers import define_ml_handler
from mindsdb.utilities.log import log

class BaseMLWrapper:
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
        return "A ML Service Wrapper", 200

    def run(self, **kwargs):
        return self.app.run(**kwargs)


class MLHandlerWrapper(BaseMLWrapper):
    def __init__(self,  name, _type):
        super().__init__(name)
        handler_class = define_ml_handler(_type)
        if handler_class is None:
            raise Exception(f"ML of '{_type}' type is not supported")
        self.handler = handler_class(
                name,
                fs_store=WithKWArgsWrapper(FsStore(), company_id=None),
                model_controller=WithKWArgsWrapper(ModelController(), company_id=None),
        )

        # CONVERT METHODS TO FLASK API ENDPOINTS
        connect_route = self.app.route("/connect", methods = ["GET", ])
        self.connect = connect_route(self.connect)

        check_connect_route = self.app.route("/check_connection", methods = ["GET", ])
        self.check_connection = check_connect_route(self.check_connection)

        get_tables_route = self.app.route("/get_tables", methods = ["GET", ])
        self.get_tables = get_tables_route(self.get_tables)

        native_query_route = self.app.route("/native_query", methods = ["POST", "PUT"])
        self.native_query = native_query_route(self.native_query)

        query_route = self.app.route("/query", methods = ["POST", "PUT"])
        self.query = query_route(self.query)

        query_route = self.app.route("/predict", methods = ["GET", ])
        self.query = query_route(self.query)

    def check_connection(self):
        log.info("%s: calling 'check_connection'", self.__class__.__name__)
        try:
           result = self.handler.check_connection()
           return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s: in 'check_connection' - %s", self.__class__.__name__, msg)
            result = StatusResponse(success=False,
                                    error_message=msg)
            return result.to_json(), 500

    def connect(self):
        return self.check_connection()

    def get_tables(self):
        log.info("%s: calling 'get_tables'", self.__class__.__name__)
        try:
           result = self.handler.get_tables()
           return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s: in 'get_tables' - %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_columns(self):
        table = request.args.get("table")
        log.info("%s: calling 'get_columns of %s table'", self.__class__.__name__, table)
        try:
           result = self.handler.get_columns(table)
           return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s: in 'get_columns' - %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def native_query(self):
        query = request.json.get("query")
        log.info("%s: calling 'native_query' with query string - %s", self.__class__.__name__, query)
        try:
            result = self.handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s: in 'native_query' - %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def query(self):
        s_query = request.get_data()
        log.info("%s: calling 'query' with query object - %s", self.__class__.__name__, s_query)
        try:
            query = pickle.loads(s_query)
            result = handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.error("%s: in 'native_query' - %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def predict(self):
        model_name = request.json.get("model_name")
        data = request.json.get("data")
        pred_format = request.json.get("pred_format")

        log.info("%s: calling 'predict':\nmodel - %s\ndata - %s, format - %s",
                 self.__class__.__name__,
                 model_name,
                 data,
                 pred_format)
        try:
            result = handler.predict(model_name, data, pred_format)
            return result.to_json(), 200
        except Exception as e:
            msg = traceback.format_exc()
            log.error("%s: in 'predict' - %s", self.__class__.__name__, msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500
