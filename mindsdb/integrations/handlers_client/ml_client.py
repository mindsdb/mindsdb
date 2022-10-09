import traceback
import pickle
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.handler_helpers import define_ml_handler
from mindsdb.utilities.log import log


class MLClient(BaseClient):
    def __init__(self, handler_type: str, as_service: bool=False, **kwargs: dict):
        super().__init__(as_service=as_service)
        connection_data = kwargs.get("connection_data", None)
        if connection_data is None:
            raise Exception("No connection data provided.")
        host = connection_data.get("host", None)
        port = connection_data.get("port", None)
        if self.as_service and (host is None or port is None):
            msg = "No host/port provided to MLHandler service connect to."
            log.error("%s __init__: %s", self.__class__.__name__, msg)
            raise Exception(msg)
        self.base_url = f"http://{host}:{port}"
        if not self.as_service:
            handler_class = define_ml_handler(handler_type)
            self.handler = handler_class(handler_class.name, **kwargs)

    def check_connection(self) -> StatusResponse:
        """
        Check a connection to the MLHandler.

        Returns:
            success status and error message if error occurs
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

    def get_tables(self) -> Response:
        """List all existing models.

        Returns:
            List all existing models.
            An error message and error code if case of fail
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
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def native_query(self, query: str) -> Response:
        """Send SQL query to MLHandler service and wait a response

        Args:
            query: query string

        Returns:
            A records from the current recordset in case of success
            An error message and error code if case of fail
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
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)

            log.error("call to db service has finished with an error: %s", traceback.format_exc())
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
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def get_columns(self, table_name: str) -> Response:
        """List all columns of the specific model

        Args:
            table_name: model
        Returns:
            A list of all columns in the model in case of success
            An error message and error code if case of fail
        """
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
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response

    def predict(self, model_name: str, data: list, pred_format: str = 'dict') -> Response:
        """Send prediction request to MLHandler and wait a response.
        Args:
            model_name: name of the model
            data: list of input data for prediction
            pred_format: prediction result format (dict|TBD)
        Returns:
            A list of prediction results in case of success
            An error message and error code if case of fail
        """
        response = None
        params = {"model_name": model_name, "data": data, "pred_format": pred_format}
        log.info("%s: calling 'predict':\n model - %s\ndata - %s\nformat - %s",
                 self.__class__.__name__, model_name, data, pred_format)
        try:
            response = self._do("/predict", json=params)
            r = self._convert_response(r.json())
            response = Response(data_frame=r.get("data_frame", None),
                                resp_type=r.get("resp_type"),
                                error_code=r.get("error_code", 0),
                                error_message=r.get("error_message", None),
                                query=r.get("query"))
            log.info("%s: db service has replied. error_code - %s", self.__class__.__name__, response.error_code)
            response = {"status": "FAIL", "error": str(e)}
        except Exception as e:
            response = Response(error_message=str(e),
                                error_code=1,
                                resp_type=RESPONSE_TYPE.ERROR)
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return response
