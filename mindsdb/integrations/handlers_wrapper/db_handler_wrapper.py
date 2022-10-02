import json
import pickle
from flask import Flask, request
from mindsdb.integrations.handlers_wrapper.helper import define_handler
from mindsdb.utilities.log import log

class BaseDBWrapper:
    def __init__(self, **kwargs):
        name = kwargs.get("name")
        _type = kwargs.get("type")
        handler_class = define_handler(_type)
        self.handler = handler_class(**kwargs)
        self.app = Flask(name)

        # self.index becomes a flask API endpoint
        default_router = self.app.route("/")
        self.index = default_router(self.index)

    def index(self):
        return "A DB Service Wrapper", 200

    def run(self, **kwargs):
        self.app.run(**kwargs)


class DBHandlerWrapper(BaseDBWrapper):
    def __init__(self,  **kwargs):
        super().__init__(**kwargs)

        # CONVERT METHODS TO FLASK API ENDPOINTS
        connect_route = self.app.route("/connect", methods = ["GET", ])
        self.connect = connect_route(self.connect)

        check_connection_route = self.app.route("/check_connection", methods = ["GET", ])
        self.check_connection = check_connection_route(self.check_connection)

        get_tables_route = self.app.route("/get_tables", methods = ["GET", ])
        self.get_tables = get_tables_route(self.get_tables)

        get_columns_route = self.app.route("/get_columns", methods = ["GET", ])
        self.get_columns = get_columns_route(self.get_columns)

        native_query_route = self.app.route("/native_query", methods = ["POST", "PUT"])
        self.native_query = native_query_route(self.native_query)

        query_route = self.app.route("/query", methods = ["GET", ])
        self.query = query_route(self.query)

    def connect(self):
        try:
            self.handler.connect()
            return {"status": "OK"}, 200
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500

    def check_connection(self):
        try:
            result =  self.handler.check_connection()
            return {"success": result.success}
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500

    def native_query(self):
        query = request.json.get("query")
        try:
            result = self.handler.native_query(query)
            return {"query": result.query, "data": result.data_frame.to_json(orient="split")}, 200
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500

    def query(self, query):
        s_query = request.data("query")
        query = pickle.loads(s_query)
        try:
            result = self.handler.query(query)
            return result, 200
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500

    def get_tables(self):
        try:
            result = self.handler.get_tables()
            return {"query": result.query, "data": result.data_frame.to_json(orient="split")}, 200
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500

    def get_columns(self):
        table = request.json.get("table")
        try:
            log.debug("get_columns: table - %s", table)
            result = self.handler.get_columns(table)
            log.debug("get_columns: result - %s", result.data_frame)
            return {"query": result.query, "data": result.data_frame.to_json(orient="split")}, 200
        except Exception as e:
            return {"status": "FAIL" ,"error": str(e)}, 500
