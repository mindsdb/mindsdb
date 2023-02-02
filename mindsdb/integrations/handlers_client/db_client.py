""" Implementation of client to any supported DBHandler service

The module provide an opportunity to not just create an instance of DBHanlder locallly
but communicate to a separate service of DBHandler using DBServiceClient.
DBServiceClient must provide same set of public API methods as DBHandlers do,
including calling params and returning types.

    Typical usage example:
    client = DBServiceClient(handler_type, **hanlder_kwargs)
    status_response = client.check_connection()
    print(status_response.status, status_response.error_message)

"""
import os
import base64
import traceback
import pickle
import json

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.handlers_client.base_client import BaseClient, Switcher
from mindsdb.integrations.libs.handler_helpers import get_handler
from mindsdb.utilities.context import context as ctx
from mindsdb.utilities.log import get_log

logger = get_log(logger_name="main")


@Switcher
class DBServiceClient(BaseClient):
    """The client to connect to DBHanlder service

    DBServiceClient must provide same set of public API methods as DBHandlers do,
    including calling params and returning types.

    Attributes:
            Connects to a specified DBHandler otherwise
        handler_type: type of DBHandler if as_service=False or DBHandler service type otherwise
        handler_kwargs: dict of handler arguments, which depends from handler_type might be very various
    """

    def __init__(self, handler_type: str, **kwargs: dict):
        """Init DBServiceClient

        Args:
            as_service: supports back compatibility with the legacy.
                if False - the DBServiceClient will be just a thin wrapper of DBHanlder instance and will redirect all requests to it.
                    Connects to a specified DBHandler otherwise
            handler_type: type of DBHandler if as_service=False or DBHandler service type otherwise
            kwargs: dict connection args for db if as_service=False or to DBHandler service otherwise
        """
        connection_data = kwargs.get("connection_data", None)
        if connection_data is None:
            raise Exception("No connection data provided.")
        base_url = os.environ.get("MINDSDB_DB_SERVICE_URL", None)
        as_service = True
        if base_url is None:
            as_service = False
            logger.info(
                "%s.__init__: no url to DBService have provided. Handler all db request locally",
                self.__class__.__name__,
            )
        else:
            self.base_url = base_url
            self.handler_kwargs = kwargs
            # no clue why it has provided!!!!!
            for a in ("fs_store", "file_storage"):
                if a in self.handler_kwargs:
                    del self.handler_kwargs[a]
            logger.info(
                "%s.__init__: DBService url - %s",
                self.__class__.__name__,
                self.base_url,
            )
        super().__init__(as_service=as_service)

        # need always instantiate handler instance
        self.handler_type = handler_type
        handler_class = get_handler(self.handler_type)
        self.handler = handler_class(**kwargs)

        # FileController is not json serializable
        # so need to remove it from json params for
        # sending to DB Service
        if self.handler_type == "files" and as_service:
            del self.handler_kwargs["file_controller"]

    def _default_json(self):
        return {
            "context": ctx.dump(),
            "handler_kwargs": self.handler_kwargs,
            "handler_type": self.handler_type,
        }

    # def _context(self):
    #     context = {
    #         "handler_type": self.handler_type,
    #         "handler_kwargs": self.handler_kwargs,
    #     }
    #     return context

    def connect(self):
        """Establish a connection.

        Returns: True if the connection success, False otherwise
        """
        logger.info("%s.connect: called", self.__class__.__name__)
        try:
            r = self._do("/connect", json=self._default_json())
            if r.status_code == 200 and r.json()["status"] is True:
                return True
        except Exception:
            logger.error(
                "%s.connect: call to db service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

        return False

    def disconnect(self):
        logger.info("%s.disconnect: called", self.__class__.__name__)
        try:
            r = self._do("/disconnect", json=self._default_json())
            if r.status_code == 200 and r.json()["status"] is True:
                return True
        except Exception:
            logger.error(
                "%s.disconnect: call to db service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

    def check_connection(self) -> StatusResponse:
        """
        Check a connection to the DBHandler.

        Returns:
            success status and error message if error occurs
        """
        logger.info("%s: calling 'check_connection'", self.__class__.__name__)
        status = None
        try:
            r = self._do("/check_connection", json=self._default_json())
            r = self._convert_response(r.json())
            status = StatusResponse(
                success=r.get("success", False),
                error_message=r.get("error_message", ""),
            )
            logger.info("%s: db service has replied", self.__class__.__name__)

        except Exception as e:
            # do some logging
            status = StatusResponse(success=False, error_message=str(e))
            logger.error(
                "call to db service has finished with an error: %s",
                traceback.format_exc(),
            )

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
        logger.info(
            "%s: calling 'native_query' for query - %s", self.__class__.__name__, query
        )
        try:
            _json = self._default_json()
            _json["query"] = query
            r = self._do("/native_query", _type="post", json=_json)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s: db service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )

        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )

            logger.error(
                "call to db service has finished with an error: %s",
                traceback.format_exc(),
            )
        return response

    def query(self, query: ASTNode) -> Response:
        """Serializes query object, send it to DBHandler service and waits a response.

        Args:
            query: query object

        Returns:
            A records from the current recordset in case of success
            An error message and error code if case of fail
        """
        # Need to send json with context and
        # serialized query object
        # it is not possible to send json and data separately
        # so need use base64 to decode serialized object into string
        s_query = pickle.dumps(query)
        b64_s_query = base64.b64encode(s_query)
        b64_s_query_str = b64_s_query.decode("utf-8")
        response = None

        _json = self._default_json()
        _json["query"] = b64_s_query_str
        logger.info(
            "%s: calling 'query' for query - %s, json - %s",
            self.__class__.__name__,
            query,
            _json,
        )
        try:
            r = self._do("/query", data=json.dumps(_json))
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.query: db service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )

        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.query:call to db service has finished with an error: %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

        return response

    def get_tables(self) -> Response:
        """List all tabels in the database without the system data.

        Returns:
            A list of all records in the database in case of success
            An error message and error code if case of fail
        """

        response = None
        logger.info("%s: calling 'get_tables'", self.__class__.__name__)

        try:
            r = self._do("/get_tables", json=self._default_json())
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )
            logger.info(
                "%s.get_tables: db service has replied. error_code - %s. data - %s",
                self.__class__.__name__,
                response.error_code,
                response.data_frame,
            )

        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.get_tables: call to db service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

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

        logger.info(
            "%s: calling 'get_columns' for table - %s",
            self.__class__.__name__,
            table_name,
        )
        try:
            _json = self._default_json()
            _json["table"] = table_name
            r = self._do("/get_columns", json=_json)
            r = self._convert_response(r.json())
            response = Response(
                data_frame=r.get("data_frame", None),
                resp_type=r.get("resp_type"),
                error_code=r.get("error_code", 0),
                error_message=r.get("error_message", None),
                query=r.get("query"),
            )

            logger.info(
                "%s.get_columns: db service has replied. error_code - %s",
                self.__class__.__name__,
                response.error_code,
            )
        except Exception as e:
            response = Response(
                error_message=str(e), error_code=1, resp_type=RESPONSE_TYPE.ERROR
            )
            logger.error(
                "%s.get_columns: call to db service has finished with an error - %s",
                self.__class__.__name__,
                traceback.format_exc(),
            )

        return response
