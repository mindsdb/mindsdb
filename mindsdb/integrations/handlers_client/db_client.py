""" Implementation of client to any supported DBHandler service

The module provide an opportunity to not just create an instance of DBHanlder locallly
but communicate to a separate service of DBHandler using DBServiceClient.
DBServiceClient must provide same set of public API methods as DBHandlers do,
including calling params and returning types.

    Typical usage example:
    client = DBServiceClient("mysql",
                             as_service=True,
                             connection_data={"host": "SERVICE(OR_CONTAINER)HOST",
                                              "port": "SERVICE_PORTS"}
                             )
    status_response = client.check_connection()
    print(status_response.status, status_response.error_message)

"""
import traceback
import pickle

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.handler_helpers import define_handler as define_db_handler
from mindsdb.utilities import log


class DBServiceClient(BaseClient):
    """The client to connect to DBHanlder service

    DBServiceClient must provide same set of public API methods as DBHandlers do,
    including calling params and returning types.

    Attributes:
        as_service: supports back compatibility with the legacy.
            if False - the DBServiceClient will be just a thin wrapper of DBHanlder instance
            and will redirect all requests to it.
            Connects to a specified DBHandler otherwise
        handler_type: type of DBHandler if as_service=False or DBHandler service type otherwise
        kwargs: connection args for db if as_service=False or to DBHandler service otherwise
    """
    def __init__(self, handler_type: str, as_service: bool = False, **kwargs: dict):
        """Init DBServiceClient

        Args:
            as_service: supports back compatibility with the legacy.
                if False - the DBServiceClient will be just a thin wrapper of DBHanlder instance and will redirect all requests to it.
                    Connects to a specified DBHandler otherwise
            handler_type: type of DBHandler if as_service=False or DBHandler service type otherwise
            kwargs: dict connection args for db if as_service=False or to DBHandler service otherwise
        """
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

    def connect(self) -> bool:
        """Establish a connection.

        Returns: True if the connection success, False otherwise
        """
        try:
            r = self._do("/connect", json={"context": self.context})
            if r.status_code == 200 and r.json()["status"] is True:
                return True
        except Exception:
            # do some logging
            pass

        return False

    def check_connection(self) -> StatusResponse:
        """
        Check a connection to the DBHandler.

        Returns:
            success status and error message if error occurs
        """
        log.logger.info("%s: calling 'check_connection'", self.__class__.__name__)
        status = None
        try:
            r = self._do("/check_connection")
            r = self._convert_response(r.json())
            status = StatusResponse(success=r.get("success", False), error_message=r.get("error_message", ""))
            log.logger.info("%s: db service has replied", self.__class__.__name__)

        except Exception as e:
            # do some logging
            status = StatusResponse(success=False, error_message=str(e))
            log.logger.error("call to db service has finished with an error: %s", traceback.format_exc())

        return status

    def native_query(self, query: str) -> Response:
        """Send SQL query to DBHandler service and wait a response

        Args:
            query: query string

        Returns:
            A records from the current recordset in case of success
            An error message and error code if case of fail
        """

        response = None
        log.logger.info("%s: calling 'native_query' for query - %s", self.__class__.__name__, query)
        try:
            r = self._do("/native_query", _type="post", json={"query": query})
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.logger.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

            log.logger.error("call to db service has finished with an error: %s", traceback.format_exc())
        return response

    def query(self, query: ASTNode) -> Response:
        """Serializes query object, send it to DBHandler service and waits a response.

        Args:
            query: query object

        Returns:
            A records from the current recordset in case of success
            An error message and error code if case of fail
        """
        s_query = pickle.dumps(query)
        response = None

        log.logger.info("%s: calling 'query' for query - %s", self.__class__.__name__, query)
        try:
            r = self._do("/query", data=s_query)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.logger.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            # do some logging
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.logger.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def get_tables(self) -> Response:
        """List all tabels in the database without the system data.

        Returns:
            A list of all records in the database in case of success
            An error message and error code if case of fail
        """

        response = None
        log.logger.info("%s: calling 'get_tables'", self.__class__.__name__)

        try:
            r = self._do("/get_tables")
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.logger.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)

        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.logger.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def get_columns(self, table_name: str) -> Response:
        """List all columns of the specific tables

        Args:
            table_name: table
        Returns:
            A list of all columns in the table in case of success
            An error message and error code if case of fail
        """
        response = None

        log.logger.info("%s: calling 'get_columns' for table - %s", self.__class__.__name__, table_name)
        try:
            r = self._do("/get_columns", json={"table": table_name})
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))

            log.logger.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.logger.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response
