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
import pickle
import traceback
from flask import Flask, request
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.handler_helpers import define_handler
from mindsdb.utilities import log


class BaseDBWrapper:
    """Base abstract class contains some general methods."""

    def __init__(self, **kwargs):
        name = kwargs.get("name")
        _type = kwargs.get("type")
        handler_class = define_handler(_type)
        self.handler = handler_class(**kwargs)
        self.app = Flask(name)

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)
        log.logger.info("%s: base params and route have been initialized", self.__class__.__name__)

    def index(self):
        """ Default GET endpoint - '/'."""
        return "A DB Service Wrapper", 200

    def run(self, **kwargs):
        """ Launch internal Flask application."""
        self.app.run(**kwargs)


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
        log.logger.info("%s: additional params and routes have been initialized", self.__class__.__name__)

    def connect(self):
        try:
            self.handler.connect()
            return {"status": "OK"}, 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            return {"status": "FAIL", "error": msg}, 500

    def check_connection(self):
        """Check connection to the database server."""
        log.logger.info("%s: calling 'check_connection'", self.__class__.__name__)
        try:
            result = self.handler.check_connection()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            result = StatusResponse(success=False,
                                    error_message=msg)
            return result.to_json(), 500

    def native_query(self):
        """Execute received string query."""
        query = request.json.get("query")
        log.logger.info("%s: calling 'native_query' with query - %s", self.__class__.__name__, query)
        try:
            result = self.handler.native_query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def query(self):
        """Execute received query object"""
        s_query = request.get_data()
        query = pickle.loads(s_query)
        log.logger.info("%s: calling 'query' with query - %s", self.__class__.__name__, query)
        try:
            result = self.handler.query(query)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_tables(self):
        log.logger.info("%s: calling 'get_tables'", self.__class__.__name__)
        try:
            result = self.handler.get_tables()
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500

    def get_columns(self):
        table = request.json.get("table")
        try:
            log.logger.info("%s: calling 'get_columns' for table - %s", self.__class__.__name__, table)
            result = self.handler.get_columns(table)
            return result.to_json(), 200
        except Exception:
            msg = traceback.format_exc()
            log.logger.error(msg)
            result = Response(resp_type=RESPONSE_TYPE.ERROR,
                              error_code=1,
                              error_message=msg)
            return result.to_json(), 500
