from typing import Optional, Dict
import traceback
import pandas as pd
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.handler_helpers import define_ml_handler
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import log


class MLClient(BaseClient):
    def __init__(self, handler_class: BaseMLEngine, as_service: bool=False, **kwargs: dict):
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
            self.handler = handler_class(**kwargs)

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
            status = StatusResponse(success=False, error_message=str(e))
            log.error("call to db service has finished with an error: %s", traceback.format_exc())

        return status

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if df is not None:
            df = df.to_json(orient="split")
        log.info("%s calling 'create': df(size) - %s, target - %s, args - %s",
                    self.__class__.__name__,
                    df.size if df is not None else None,
                    target,
                    args)
        params = {"target": target, "args": args, "df": df}
        r = self._do("/create", _type="post", json=params)
        r = self._convert_response(r.json())
        err = r.get("error_message", None)
        if err:
            log.error("%s 'create' call to ml service has finished with an error - %s", self.__class__.__name__, err)
            # ml_exec_base::learn_process must handle it properly
            raise Exception(err)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """Send prediction request to MLHandler and wait a response.
        Args:
            df: list of input data for prediction
            args: don't have a clue what is it
        Returns:
            DataFrame of prediction results
        """
        if df is not None:
            df = df.to_json(orient="split")
        params = {"df": df, "args": args}
        log.info("%s: calling 'predict':\n df(size) - %s\nargs - %s\n",
                 self.__class__.__name__,
                 df.size if df is not None else None,
                 args)
        r = self._do("/predict", json=params)
        r = self._convert_response(r.json())
        # response = Response(data_frame=r.get("data_frame", None),
        #                     resp_type=r.get("resp_type"),
        #                     error_code=r.get("error_code", 0),
        #                     error_message=r.get("error_message", None),
        #                     query=r.get("query"))
        log.info("%s: db service has replied. error_code - %s, err_msg",
                    self.__class__.__name__,
                    r.get("error_code", 0),
                    r.get("error_message", None))
        return r.get("data_frame", None)
