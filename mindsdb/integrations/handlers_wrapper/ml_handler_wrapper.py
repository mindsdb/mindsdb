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
import base64
import pickle

from pandas import DataFrame

from flask import Flask, request

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.libs.ml_exec_base import BaseMLEngineExec
from mindsdb.interfaces.storage.fs import (
    FileStorage,
    FileStorageFactory,
    RESOURCE_GROUP,
)
from mindsdb.interfaces.database.integrations import IntegrationController
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import get_log

logger = get_log(logger_name="main")


class BaseMLWrapper:
    """Base abstract class contains some general methods."""

    def __init__(self, name):
        self.app = Flask(name)
        self.__mdb_config = Config()

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)

    def index(self):
        """Default GET endpoint - '/'."""
        return "A ML Service Wrapper", 200

    def run(self, **kwargs):
        """Launch internal Flask application."""
        return self.app.run(**kwargs)


class MLHandlerWrapper(BaseMLWrapper):
    def __init__(self, name):
        """Wrapper Init.
        Args:
            name: name of the service
        """
        super().__init__(name)

        # CONVERT METHODS TO FLASK API ENDPOINTS
        # check_connect_route = self.app.route("/check_connection", methods=["GET", ])
        # self.check_connection = check_connect_route(self.check_connection)

        predict_route = self.app.route(
            "/predict",
            methods=[
                "GET",
            ],
        )
        self.predict = predict_route(self.predict)

        learn_route = self.app.route(
            "/learn",
            methods=[
                "POST",
            ],
        )
        self.learn = learn_route(self.learn)

        get_columns_route = self.app.route(
            "/get_columns",
            methods=[
                "GET",
            ],
        )
        self.get_columns = get_columns_route(self.get_columns)

        get_tables_route = self.app.route(
            "/get_tables",
            methods=[
                "GET",
            ],
        )
        self.get_tables = get_tables_route(self.get_tables)

        native_query_route = self.app.route(
            "/native_query",
            methods=[
                "GET",
            ],
        )
        self.native_query = native_query_route(self.native_query)

        query_route = self.app.route(
            "/query",
            methods=[
                "GET",
            ],
        )
        self.query = query_route(self.query)

        update_route = self.app.route(
            "/update",
            methods=[
                "PUT",
            ],
        )
        self.update = update_route(self.update)

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

    def _get_handler(self):
        """Create handler instance."""
        ctx.load(request.json.get("context"))
        params = request.json.get("handler_args")

        integration_id = params["integration_id"]
        _type = params["integration_engine"] or params.get("name", None)
        logger.info(
            "%s.get_handler: request handler of type - %s",
            self.__class__.__name__,
            _type,
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

    def get_columns(self):
        logger.info("%s.get_columns has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            table_name = params.get("table_name")
            logger.info(
                "%s.get_columns - called for table - %s",
                self.__class__.__name__,
                table_name,
            )
            handler = self._get_handler()
            result = handler.get_columns(table_name)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'get_columns': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def get_tables(self):
        logger.info("%s.get_tables has called.", self.__class__.__name__)
        try:
            handler = self._get_handler()
            result = handler.get_tables()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'get_tables': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def native_query(self):
        logger.info("%s.native_query has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            query = params.get("query")
            logger.info(
                "%s.native_query: called for query - %s", self.__class__.__name__, query
            )
            handler = self._get_handler()
            result = handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'native_query': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def query(self):
        logger.info("%s.query has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            query_s64 = params.get("query")
            query_b64 = query_s64.encode("utf-8")
            query_s = base64.b64decode(query_b64)
            query = pickle.loads(query_s)
            logger.info(
                "%s.query: called for query - %s", self.__class__.__name__, query
            )
            handler = self._get_handler()
            result = handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'query': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def predict(self):
        """See 'predict' method in MLHandler"""
        logger.info("%s.predict has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            logger.info(
                "%s.predict: called with params - %s", self.__class__.__name__, params
            )
            handler = self._get_handler()
            predictions = handler.predict(**params)
            logger.info(
                "%s.predict: got predictions - %s(type - %s)",
                self.__class__.__name__,
                predictions,
                type(predictions),
            )
            result = Response(
                resp_type=RESPONSE_TYPE.OK,
                data_frame=DataFrame(predictions),
                error_code=0,
                error_message=None,
            )
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'predict': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def learn(self):
        logger.info("%s.learn has called.", self.__class__.__name__)
        try:
            params = request.json.get("method_params")
            logger.info(
                "%s.learn: called with params - %s", self.__class__.__name__, params
            )
            handler = self._get_handler()
            handler.learn(**params)
            result = Response(
                resp_type=RESPONSE_TYPE.OK,
                data_frame=None,
                error_code=0,
                error_message=None,
            )
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s error in calling 'query': %s", self.__class__.__name__, msg
            )
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def update(self):
        logger.error("%s.update - NOT IMPLEMENTED")
        result = Response(
            resp_type=RESPONSE_TYPE.ERROR,
            error_code=1,
            error_message="'update' not implemented on serverside"
        )
        return result.to_json(), 500
