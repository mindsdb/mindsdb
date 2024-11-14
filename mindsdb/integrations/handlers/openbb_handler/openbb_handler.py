from functools import reduce
from openbb_core.app.static.app_factory import create_app

from mindsdb.integrations.handlers.openbb_handler.openbb_tables import create_table_class
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log
from mindsdb.integrations.handlers.openbb_handler.openbb_tables import OpenBBtable

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

        # Initialize OpenBB
        # pylint: disable=import-outside-toplevel
        from openbb.package.__extensions__ import Extensions
        self.obb = create_app(Extensions)

        for cmd in list(self.obb.coverage.command_model.keys()):

            openbb_params = self.obb.coverage.command_model[cmd]["openbb"]["QueryParams"]
            openbb_data = self.obb.coverage.command_model[cmd]["openbb"]["Data"]

            # Creates the default data retrieval function for the given command
            # e.g. obb.equity.price.historical, obb.equity.fa.income
            # Note: Even though openbb_params just contains the standard fields that are
            # common across vendors users are able to select any of the fields from the vendor
            # as well. However, some of them might have no effect on the data if the vendor
            # doesn't support it. Regardless, the endpoint won't crash.
            table_class = create_table_class(
                params_metadata=openbb_params,
                response_metadata=openbb_data,
                obb_function=reduce(getattr, cmd[1:].split('.'), self.obb),
                func_docs=f"https://docs.openbb.co/platform/reference/{cmd[1:].replace('.', '/')}"
            )
            self._register_table(cmd.replace('.', '_')[1:], table_class(self))

            # Creates the data retrieval function for each provider
            # e.g. obb.equity.price.historical_polygon, obb.equity.price.historical_intrinio
            for provider in list(self.obb.coverage.command_model[cmd].keys()):

                # Skip the openbb provider since we already created it and it will look like obb.equity.price.historical
                if provider == "openbb":
                    continue

                provider_extra_params = self.obb.coverage.command_model[cmd][provider]["QueryParams"]
                combined_params = provider_extra_params.copy()  # create a copy to avoid modifying the original
                combined_params["fields"] = {**openbb_params["fields"], **provider_extra_params["fields"]}  # merge the fields

                provider_extra_data = self.obb.coverage.command_model[cmd][provider]["Data"]
                combined_data = provider_extra_data.copy()  # create a copy to avoid modifying the original
                combined_data["fields"] = {**openbb_data["fields"], **provider_extra_data["fields"]}  # merge the fields

                table_class = create_table_class(
                    params_metadata=combined_params,
                    response_metadata=combined_data,
                    obb_function=reduce(getattr, cmd[1:].split('.'), self.obb),
                    func_docs=f"https://docs.openbb.co/platform/reference/{cmd[1:].replace('.', '/')}",
                    provider=provider
                )
                self._register_table(f"{cmd.replace('.', '_')[1:]}_{provider}", table_class(self))

        obb_table = OpenBBtable(self)
        self._register_table("openbb_fetcher", obb_table)

    def connect(self) -> bool:
        """Connects with OpenBB account through personal access token (PAT).

        Returns none.
        """
        self.is_connected = False
        self.obb.account.login(pat=self.PAT)

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
