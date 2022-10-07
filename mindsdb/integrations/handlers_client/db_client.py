import traceback
import pickle

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.handler_helpers import define_handler as define_db_handler
from mindsdb.utilities.log import log

class DBServiceClient(BaseClient):
    def __init__(self, handler_type, as_service=False, **kwargs):
        super().__init__(as_service=as_service)
        connection_data = kwargs.get("connection_data", None)
        if connection_data is None:
            raise Exception("No connection data provided.")
        host = connection_data.get("host", None)
        port = connection_data.get("port", None)
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
        log.info("%s: calling 'check_connection'", self.__class__.__name__)
        status = None
        try:
            r = self._do("/check_connection")
            r = self._convert_response(r.json())
            status = StatusResponse(success=r.get("success", False), error_message=r.get("error_message", ""))
            log.info("%s: db service has replied", self.__class__.__name__)

        except Exception as e:
            # do some logging
            status = StatusResponse(success=False, error_message=str(e))
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return status

    def native_query(self, query: str):
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in db
        :return: returns the records from the current recordset
        """

        response = None
        log.info("%s: calling 'native_query' for query - %s", self.__class__.__name__, query)
        try:
            r = self._do("/native_query", _type="post", json={"query": query})
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

            log.error("call to db service has finished with an error: %s", traceback.format_exc())
        return response

    def query(self, query):
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        s_query = pickle.dumps(query)
        response = None

        log.info("%s: calling 'query' for query - %s", self.__class__.__name__, query)
        try:
            r = self._do("/query", data=s_query)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def get_tables(self):
        """
        List all tabels in the database without the system data.
        """

        response = None
        log.info("%s: calling 'get_tables'", self.__class__.__name__)

        try:
            r = self._do("/get_tables")
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def get_columns(self, table_name):
        response = None

        log.info("%s: calling 'get_columns' for table - %s", self.__class__.__name__, table_name)
        try:
            r = self._do("/get_columns", json={"table": table_name})
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))

            log.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response
