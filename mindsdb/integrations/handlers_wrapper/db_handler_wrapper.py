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
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.handler_helpers import get_handler
# from mindsdb.utilities.log import logger
# import logging
# logger = logging.getLogger("mindsdb.main")
from mindsdb.utilities.config import Config
from mindsdb.utilities.log import (
    initialize_log,
    get_log
)
Config()
initialize_log(logger_name="main")
logger = get_log("main")


class BaseDBWrapper:
    """Base abstract class contains some general methods."""

    def __init__(self, **kwargs):
        name = kwargs.get("name", self.__class__.__name__)
        # _type = kwargs.get("type")
        # handler_class = get_handler(_type)
        # self.handler = handler_class(**kwargs)
        self.app = Flask(name)

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)
        logger.info("%s: base params and route have been initialized", self.__class__.__name__)

    def index(self):
        """ Default GET endpoint - '/'."""
        return "A DB Service Wrapper", 200

    def run(self, **kwargs):
        """ Launch internal Flask application."""
        self.app.run(**kwargs)

    def get_handler(self, _json):
        handler_class = get_handler(_json["handler_type"])
        logger.info("%s.get_handler: requested instance of %s handler", self.__class__.__name__, handler_class)
        return handler_class(**_json["handler_kwargs"])



class DBHandlerWrapper(BaseDBWrapper):
    """ A REST API wrapper for DBHandler.
    General meaning: DBHandlerWrapper(DBHandler) = DBHandler + REST API
    DBHandler which capable communicate with the caller via REST
    """
    def __init__(self, **kwargs):
        """ Wrapper Init.
        Args:
            connection_data: dict contains all required connection info to a specific database
            name: DBHandler instance name
            type: type of the Handler (mysql, postgres, etc)
        """
        super().__init__(**kwargs)

        # CONVERT METHODS TO FLASK API ENDPOINTS
        connect_route = self.app.route("/connect", methods=["GET", ])
        self.connect = connect_route(self.connect)

        check_connection_route = self.app.route("/check_connection", methods=["GET", ])
        self.check_connection = check_connection_route(self.check_connection)

        get_tables_route = self.app.route("/get_tables", methods=["GET", ])
        self.get_tables = get_tables_route(self.get_tables)

        get_columns_route = self.app.route("/get_columns", methods=["GET", ])
        self.get_columns = get_columns_route(self.get_columns)

        native_query_route = self.app.route("/native_query", methods=["POST", "PUT"])
        self.native_query = native_query_route(self.native_query)

        query_route = self.app.route("/query", methods=["GET", ])
        self.query = query_route(self.query)
        logger.info("%s: additional params and routes have been initialized", self.__class__.__name__)

    def connect(self):
        try:
            handler = self.get_handler(request.json)
            handler.connect()
            return {"status": "OK"}, 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            return {"status": "FAIL", "error": msg}, 500

    def check_connection(self):
        """Check connection to the database server."""
        logger.info("%s: calling 'check_connection'", self.__class__.__name__)
        try:
            handler = self.get_handler(request.json)
            result = handler.check_connection()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = StatusResponse(success=False,
                                    error_message=msg)
            return result.to_json(), 500

    def native_query(self):
        """Execute received string query."""
        query = request.json.get("query")
        logger.info("%s: calling 'native_query' with query - %s", self.__class__.__name__, query)
        try:
            handler = self.get_handler(request.json)
            result = handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def query(self):
        """Execute received query object"""
        logger.info("%s: calling 'query'", self.__class__.__name__)
        logger.info("%s: calling 'query', raw data - %s", self.__class__.__name__, request.get_data())
        # _json = json.loads(request.get_data())
        # logger.info("%s: calling 'query', data in json - %s", self.__class__.__name__, _json)
        try:
            # Have received json with context and
            # serialized query object
            # it is not possible to send json and data separately
            # so need use base64 to encode serialized object string back in bytes
            _json_bytes = request.get_data()
            # query = pickle.loads(s_query)
            _json = json.loads(_json_bytes)
            logger.info("%s: json decoded - %s", self.__class__.__name__, _json)
            b64_query_bytes = _json["query"].encode("utf-8")
            s_query = base64.b64decode(b64_query_bytes)
            query = pickle.loads(s_query)
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

        logger.info("%s: calling 'query' with query - %s", self.__class__.__name__, query)
        try:
            # logger.info("%s: calling 'query' with json(text) - %s", self.__class__.__name__, request.body)
            # handler = self.get_handler(request.json)
            handler = self.get_handler(_json)
            result = handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_tables(self):
        logger.info("%s: calling 'get_tables'", self.__class__.__name__)
        try:
            handler = self.get_handler(request.json)
            result = handler.get_tables()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_columns(self):
        logger.info("%s: calling 'get_columns' for table - %s", self.__class__.__name__, table)
        try:
            table = request.json.get("table")
            handler = self.get_handler(request.json)
            result = handler.get_columns(table)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500
