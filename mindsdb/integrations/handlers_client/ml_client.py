""" Implementation of client to any supported MLHandler service

The module provide an opportunity to not just create an instance of MLHanlder locally
but communicate to a separate service of MLHandler using MLClient.
MLClient must provide same set of public API methods as MLHandlers do,
including calling params and returning types.

    Typical usage example:
    client = MLClient(HuggingFaceHandler,
                      as_service=True,
                      connection_data={"host": "SERVICE(OR_CONTAINER)HOST",
                                       "port": "SERVICE_PORTS"},
                      company_id=COMPANY_ID,
                      predictor_id=PREDICTOR_ID

    status_response = client.check_connection()
    print(status_response.status, status_response.error_message)

"""
from typing import Optional, Dict
import traceback

import pandas as pd

from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.handlers_client.base_client import BaseClient
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.log import get_log


log = get_log()


class MLClient(BaseClient):
    """ The client to connect to MLHanlder service

    MLClient must provide same set of public API methods as MLHandlers do,
    including calling params and returning types.

    Attributes:
        as_service: supports back compatibility with the legacy.
            if False - the MLClient will be just a thin wrapper of MLHanlder instance
            and will redirect all requests to it.
            Connects to a specified MLHandler otherwise
        handler_class: class of MLHandler which supposed to be used
        kwargs:
            connection_data - connection args for to MLHandler service if as_service=True
            predictor_id - id of the used model
            company_id - id of the company.
            Last two args needed to instantiante engine and model storages
    """
    def __init__(self, handler_class: BaseMLEngine, as_service: bool = False, **kwargs: dict):
        """ Init MLClient

        Args:
        as_service: supports back compatibility with the legacy.
            if False - the MLClient will be just a thin wrapper of MLHanlder instance
            and will redirect all requests to it.
            Connects to a specified MLHandler otherwise
        handler_class: class of MLHandler which supposed to be used
        kwargs:
            connection_data - connection args for to MLHandler service if as_service=True
            predictor_id - id of the used model
            company_id - id of the company.
            Last two args needed to instantiante engine and model storages
        """
        super().__init__(as_service=as_service)
        connection_data = kwargs.get("connection_data", None)
        if connection_data is None:
            raise Exception("No connection data provided.")

        company_id = kwargs.get("company_id", -1)
        predictor_id = kwargs.get("predictor_id", -1)
        if self.as_service and (company_id == -1 or predictor_id == -1):
            raise Exception("company_id and/or predictor_id are not provided.")

        host = connection_data.get("host", None)
        port = connection_data.get("port", None)
        if self.as_service and (host is None or port is None):
            msg = "No host/port provided to MLHandler service connect to."
            log.error("%s __init__: %s", self.__class__.__name__, msg)
            raise Exception(msg)
        self.base_url = f"http://{host}:{port}"
        if not self.as_service:
            self.handler = handler_class(**kwargs)
        self.model_info = {"company_id": company_id,
                           "predictor_id": predictor_id,
                           "type": "huggingface"}

    def check_connection(self) -> StatusResponse:
        """ Check a connection to the MLHandler.

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
        """Send 'create' request to the MLService.
        Args:
            target: train (prediction) target of a model
            df: train dataframe
            args: additional training args [TBD]
        Returns:
            None
        Raises:
            Any network errors
            Errors, received from the MLService
        """
        if df is not None:
            df_str = df.to_json(orient="split", index=False)
        else:
            df_str = df
        log.info("%s calling 'create': df(size) - %s, target - %s, args - %s",
                 self.__class__.__name__,
                 len(df) if df is not None else 0,
                 target,
                 args)
        params = {"target": target, "args": args, "df": df_str}
        params.update(self.model_info)
        r = self._do("/create", _type="post", json=params)
        r = self._convert_response(r.json())
        err = r.get("error_message", None)
        log.info("%s 'predict': db service has replied. error_code - %s, err_msg - %s",
                 self.__class__.__name__,
                 r.get("error_code", 0),
                 err)
        if err:
            log.error("%s 'create' call to ml service has finished with an error - %s", self.__class__.__name__, err)

            # ml_exec_base::learn_process must handle it properly
            # that is why no needs to catch any exceptions here for now
            raise Exception(err)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """Send prediction request to MLHandler and wait a response.
        Args:
            df: list of input data for prediction
            args: don't have a clue what is it
        Returns:
            DataFrame of prediction results
        Raises:
            Any network error
        """
        if df is not None:
            df_str = df.to_json(orient="split", index=False)
        else:
            df_str = df
        params = {"df": df_str, "args": args}
        params.update(self.model_info)
        log.info("%s: calling 'predict':\n df(size) - %s\nargs - %s\n",
                 self.__class__.__name__,
                 len(df) if df is not None else 0,
                 args)
        r = self._do("/predict", json=params)
        r = self._convert_response(r.json())
        log.info("%s 'predict': db service has replied. error_code - %s, err_msg - %s",
                 self.__class__.__name__,
                 r.get("error_code", 0),
                 r.get("error_message", None))
        return r.get("data_frame", None)
