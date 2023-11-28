import pandas as pd
from typing import Dict

from openbb import obb

from mindsdb.integrations.handlers.openbb_handler.openbb_tables import OpenBBtable, create_table_class
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
)
from mindsdb.utilities import log
from mindsdb_sql import parse_sql

logger = log.getLogger(__name__)


class OpenBBHandler(APIHandler):
    """A class for handling connections and interactions with the OpenBB Platform.

    Attributes:
        PAT (str): OpenBB's personal access token. Sign up here: https://my.openbb.co
        is_connected (bool): Whether or not the user is connected to their OpenBB account.

    """

    def __init__(self, name: str = None, **kwargs):
        """Registers all API tables and prepares the handler for an API connection.

        Args:
            name: (str): The handler name to use
        """
        super().__init__(name)
        self.PAT = None

        args = kwargs.get("connection_data", {})
        if "PAT" in args:
            self.PAT = args["PAT"]

        self.is_connected = False

        self.obb = obb
        from openbb_provider import standard_models

        self._register_table("openbb_fetcher", OpenBBtable(self))
        stock_table_class = create_table_class(
            params_metadata=standard_models.stock_historical.StockHistoricalQueryParams.model_json_schema(),
            response_metadata=standard_models.stock_historical.StockHistoricalData.model_json_schema(),
            obb_function=self.obb.stocks.load
        )
        self._register_table("stock_historical", stock_table_class(self))

        crypto_table_class = create_table_class(
            params_metadata=standard_models.crypto_historical.CryptoHistoricalQueryParams.model_json_schema(),
            response_metadata=standard_models.crypto_historical.CryptoHistoricalData.model_json_schema(),
            obb_function=self.obb.crypto.load
        )
        self._register_table("crypto_historical", crypto_table_class(self))

    def connect(self) -> bool:
        """Connects with OpenBB account through personal access token (PAT).

        Returns none.
        """
        self.is_connected = False
        obb.account.login(pat=self.PAT)

        # Check if PAT utilized is valid
        # if obb.user.profile.active:
        self.is_connected = True
        return True

    def check_connection(self) -> StatusResponse:
        """Checks connection to OpenBB accounting by checking the validity of the PAT.

        Returns StatusResponse indicating whether or not the handler is connected.
        """

        response = StatusResponse(False)

        try:
            if self.connect():
                response.success = True

        except Exception as e:
            logger.error(f"Error connecting to OpenBB Platform: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response

    def _process_cols_names(self, cols: list) -> list:
        new_cols = []
        for element in cols:
            # If the element is a tuple, we want to merge the elements together
            if isinstance(element, tuple):
                # If there's more than one element we want to merge them together
                if len(element) > 1:
                    # Prevents the case where there's a multi column index and the index is a date
                    # in that instance we will have ('date', '') and this avoids having a column named 'date_'
                    new_element = "_".join(map(str, element)).rstrip("_")
                    new_cols.append(new_element)
                else:
                    new_cols.append(element[0])
            else:
                new_cols.append(element)
        return new_cols

    def _openbb_fetcher(self, params: Dict = None) -> pd.DataFrame:
        """Gets aggregate trade data for a symbol based on given parameters

        Returns results as a pandas DataFrame.

        Args:
            params (Dict): Trade data params (symbol, interval, limit, start_time, end_time)
        """
        self.connect()

        try:
            if params is None:
                logger.error("At least cmd needs to be added!")
                raise Exception("At least cmd needs to be added!")

            # Get the OpenBB command to get the data from
            cmd = params.pop("cmd")

            # Ensure that the cmd provided is a valid OpenBB command
            available_cmds = [f"obb{cmd}" for cmd in list(obb.coverage.commands.keys())]
            if cmd not in available_cmds:
                logger.error(f"The command provided is not supported by OpenBB! Choose one of the following: {', '.join(available_cmds)}")
                raise Exception(f"The command provided is not supported by OpenBB! Choose one of the following: {', '.join(available_cmds)}")

            args = ""
            # If there are parameters create arguments as a string
            if params:
                for arg, val in params.items():
                    args += f"{arg}={val},"

                # Remove the additional ',' added at the end
                if args:
                    args = args[:-1]

            # Recreate the OpenBB command with the arguments
            openbb_cmd = f"{cmd}({args})"

            # Execute the OpenBB command and return the OBBject
            openbb_object = eval(openbb_cmd)

            # Transform the OBBject into a pandas DataFrame
            data = openbb_object.to_df()

            # Check if index is a datetime, if it is we want that as a column
            if isinstance(data.index, pd.DatetimeIndex):
                data.reset_index(inplace=True)

            # Process column names
            data.columns = self._process_cols_names(data.columns)

        except Exception as e:
            logger.error(f"Error accessing data from OpenBB: {e}!")
            raise Exception(f"Error accessing data from OpenBB: {e}!")

        return data

    def native_query(self, query: str = None) -> Response:
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)

    def call_openbb_api(
        self, method_name: str = None, params: Dict = None
    ) -> pd.DataFrame:
        """Calls the OpenBB Platform method with the given params.

        Returns results as a pandas DataFrame.

        Args:
            method_name (str): Method name to call (e.g. klines)
            params (Dict): Params to pass to the API call
        """
        if method_name == "openbb_fetcher":
            return self._openbb_fetcher(params)
        raise NotImplementedError(
            "Method name {} not supported by OpenBB Platform Handler".format(
                method_name
            )
        )
