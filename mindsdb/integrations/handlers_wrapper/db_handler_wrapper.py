"""Implementation of REST API wrapper for any supported DBHandler.
The module provide an opportunity to convert a DBHandler into REST API service
Basic usage:
    app = DBHandlerWrapper(
            connection_data={
                    "host": "mysql_db",
                    "port": "3306",
                    "user": "root",
                    "password": "supersecret",
                    "database": "test",
                    "ssl": false}
            name="foo"
            type="mysql"}
    )
    port = int(os.environ.get('PORT', 5001))
    host = os.environ.get('HOST', '0.0.0.0')
    log.info("Running dbservice: host=%s, port=%s", host, port)
    app.run(debug=True, host=host, port=port)

"""
import json
import pickle
import traceback
import base64

from flask import Flask, request
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.interfaces.file.file_controller import FileController
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log

logger = get_log(logger_name="main")


class BaseDBWrapper:
    """Base abstract class contains some general methods."""

    def __init__(self, **kwargs):
        name = kwargs.get("name", self.__class__.__name__)
        self.app = Flask(name)

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)
        self.after_request = self.app.after_request(self.after_request)
        self.before_request = self.app.before_request(self.before_request)
        logger.info(
            "%s: base params and route have been initialized", self.__class__.__name__
        )

    def index(self):
        """Default GET endpoint - '/'."""
        return "A DB Service Wrapper", 200

    def run(self, **kwargs):
        """Launch internal Flask application."""
        self.app.run(**kwargs)

    def get_handler(self, _json):
        ctx.load(request.json.get("context"))
        handler_class = get_handler(_json["handler_type"])
        logger.info(
            "%s.get_handler: requested instance of %s handler",
            self.__class__.__name__,
            handler_class,
        )
        handler_kwargs = _json["handler_kwargs"]
        # Create an instance of FileController for
        # 'files' type of handler
        if _json["handler_type"] == "files":
            handler_kwargs["file_controller"] = FileController()
        return handler_class(**handler_kwargs)

    def before_request(self):
        logger.info("%s [%s %s]: params - %s", self.__class__.__name__, request.method, request.full_path, request.json)

    def after_request(self, response):
        logger.info("%s [%s %s] - %s: result - %s", self.__class__.__name__, request.method, request.full_path, response.status, response.json)
        return response


class DBHandlerWrapper(BaseDBWrapper):
    """A REST API wrapper for DBHandler.
    General meaning: DBHandlerWrapper(DBHandler) = DBHandler + REST API
    DBHandler which capable communicate with the caller via REST
    """

    def __init__(self, **kwargs):
        """Wrapper Init.
        Args:
            connection_data: dict contains all required connection info to a specific database
            name: DBHandler instance name
            type: type of the Handler (mysql, postgres, etc)
        """
        super().__init__(**kwargs)

        # CONVERT METHODS TO FLASK API ENDPOINTS
        connect_route = self.app.route(
            "/connect",
            methods=[
                "GET",
            ],
        )
        self.connect = connect_route(self.connect)

        disconnect_route = self.app.route(
            "/connect",
            methods=[
                "GET",
            ],
        )
        self.disconnect = disconnect_route(self.disconnect)

        check_connection_route = self.app.route(
            "/check_connection",
            methods=[
                "GET",
            ],
        )
        self.check_connection = check_connection_route(self.check_connection)

        get_tables_route = self.app.route(
            "/get_tables",
            methods=[
                "GET",
            ],
        )
        self.get_tables = get_tables_route(self.get_tables)

        get_columns_route = self.app.route(
            "/get_columns",
            methods=[
                "GET",
            ],
        )
        self.get_columns = get_columns_route(self.get_columns)

        native_query_route = self.app.route("/native_query", methods=["POST", "PUT"])
        self.native_query = native_query_route(self.native_query)

        query_route = self.app.route(
            "/query",
            methods=[
                "GET",
            ],
        )
        self.query = query_route(self.query)
        logger.info(
            "%s: additional params and routes have been initialized",
            self.__class__.__name__,
        )

    def connect(self):
        try:
            handler = self.get_handler(request.json)
            handler.connect()
            return {"status": "OK"}, 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            return {"status": "FAIL", "error": msg}, 500

    def disconnect(self):
        try:
            handler = self.get_handler(request.json)
            handler.disconnect()
            return {"status": "OK"}, 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            return {"status": "FAIL", "error": msg}, 500

    def check_connection(self):
        """Check connection to the database server."""
        try:
            handler = self.get_handler(request.json)
            result = handler.check_connection()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(
                "%s.check_connection: error - %s", self.__class__.__name__, msg
            )
            result = StatusResponse(success=False, error_message=msg)
            return result.to_json(), 500

    def native_query(self):
        """Execute received string query."""
        query = request.json.get("query")
        try:
            handler = self.get_handler(request.json)
            result = handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.native_query: error - %s", self.__class__.__name__, msg)
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def query(self):
        """Execute received query object"""
        try:
            # Have received json with context and
            # serialized query object
            # it is not possible to send json and data separately
            # so need use base64 to encode serialized object string back in bytes
            _json_bytes = request.get_data()
            _json = json.loads(_json_bytes)
            logger.debug("%s.query: json decoded - %s", self.__class__.__name__, _json)
            b64_query_bytes = _json["query"].encode("utf-8")
            s_query = base64.b64decode(b64_query_bytes)
            query = pickle.loads(s_query)
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.query: error - %s", self.__class__.__name__, msg)
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

        logger.debug(
            "%s.query: with unpickle query - %s", self.__class__.__name__, query
        )
        try:
            handler = self.get_handler(_json)
            result = handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def get_tables(self):
        try:
            handler = self.get_handler(request.json)
            result = handler.get_tables()
            logger.error("%s.get_tables: result - %s", self.__class__.__name__, result.to_json())
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.get_tables: error - %s", self.__class__.__name__, msg)
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500

    def get_columns(self):
        try:
            table = request.json.get("table")
            logger.info(
                "%s.get_columns: calling for table - %s", self.__class__.__name__, table
            )
            handler = self.get_handler(request.json)
            result = handler.get_columns(table)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error("%s.get_columns: error - %s", self.__class__.__name__, msg)
            result = Response(
                resp_type=RESPONSE_TYPE.ERROR, error_code=1, error_message=msg
            )
            return result.to_json(), 500
