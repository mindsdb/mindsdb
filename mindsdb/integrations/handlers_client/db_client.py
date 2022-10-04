import pickle

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers_client.base import BaseClient
from mindsdb.integrations.libs.handler_helpers import define_handler as define_db_handler


class DBServiceClient(BaseClient):
    def __init__(self, handler_type, as_service=False, **kwargs):
        super().__init__(as_service=as_service)
        connection_data = kwargs.get("connection_data", None)
        if connection_data is None:
            raise Exception("No connection data provided.")
        host = kwargs.get("host", None)
        port = kwargs.get("port", None)
        if self.as_service and (host is None or port is None):
            raise Exception("No host/port provided to DBHandler service connect to.")

        self.base_url = f"http://{host}:{port}"
        if not self.as_service:
            handler_class = define_db_handler(handler_type)
            self.handler = handler_class(handler_class.name, **kwargs)

    def connect(self):
        try:
            r = self._do("/connect", json={"context": self.context})
            if r.status_code == 200 and r.json()["status"] is True:
                return True
        except Exception:
            # do some logging
            pass

        return False

    def check_connection(self):
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        status = None
        try:
            r = self._do("/check_connection")
            r = r.json()
            status = StatusResponse(success=r.get("success", False), error_message=r.get("error_message", ""))

        except Exception as e:
            # do some logging
            status = StatusResponse(success=False, error_message=str(e))

        return status

    def native_query(self, query: str):
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in db
        :return: returns the records from the current recordset
        """

        response = None
        try:
            r = self._do("/native_query", json={"context": self.context, "query": query})
            r = r.json()
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

        return response

    def query(self, query):
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        s_query = pickle.dumps(query)
        response = None

        try:
            r = self._do("/query", json={"data": s_query})
            r = r.json()
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

        return response

    def get_tables(self):
        """
        List all tabels in the database without the system data.
        """

        response = None

        try:
            r = self._do("/get_tables", json={"context": self.context})
            r = r.json()
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

        return response

    def get_columns(self, table_name):
        response = None

        try:
            r = self._do("/get_columns", json={"context": self.context, "table": table_name})
            r = r.json()
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

        return response
