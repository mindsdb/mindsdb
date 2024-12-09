from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.sharepoint_handler.sharepoint_api import (
    SharepointAPI,
)
from mindsdb.integrations.handlers.sharepoint_handler.sharepoint_tables import (
    ListsTable,
    SitesTable,
    ListItemsTable,
    SiteColumnsTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.utilities import log
from collections import OrderedDict

logger = log.getLogger(__name__)


class SharepointHandler(APIHandler):
    """
    The Sharepoint handler implementation.
    """

    name = "sharepoint"

    def __init__(self, name: str, **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        if not (
            self.connection_data["clientId"]
            and (
                self.connection_data["tenantId"]
                and self.connection_data["clientSecret"]
            )
        ):
            raise Exception(
                "client params and tenant id is required for Sharepoint connection!"
            )

        self.connection = None
        self.is_connected = False
        self._client = None
        lists_data = ListsTable(self)
        self._register_table("lists", lists_data)

        sites_data = SitesTable(self)
        self._register_table("sites", sites_data)

        site_columns_data = SiteColumnsTable(self)
        self._register_table("siteColumns", site_columns_data)

        list_items_data = ListItemsTable(self)
        self._register_table("listItems", list_items_data)

    def connect(self):
        """
        Set up the context connection required by the handler.
        Returns
        -------
        StatusResponse
            connection object
        """
        if self.is_connected is True:
            return self.connection
        self.connection = SharepointAPI(
            tenant_id=self.connection_data["tenantId"],
            client_id=self.connection_data["clientId"],
            client_secret=self.connection_data["clientSecret"],
        )
        self.connection.get_bearer_token()
        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)

        try:
            connection = self.connect()
            response.success = connection.check_bearer_token_validity()
        except Exception as e:
            logger.error("Error connecting to Sharepoint! " + str(e))
            response.error_message = str(e)

        self.is_connected = response.success

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw query.
        Parameters
        ----------
        query : str
            query in a native format
        Returns
        -------
        StatusResponse
            Request status
        """
        ast = parse_sql(query)
        return self.query(ast)


connection_args = OrderedDict(
    clientId={
        "type": ARG_TYPE.STR,
        "description": "Client Id of the App",
        "required": True,
        "label": "Client ID",
    },
    clientSecret={
        "type": ARG_TYPE.PWD,
        "description": "Client Secret of the App",
        "required": True,
        "label": "Client Secret",
    },
    tenantId={
        "type": ARG_TYPE.STR,
        "description": "Tenant Id of the tenant of the App",
        "required": True,
        "label": "Tenant ID",
    },
)

connection_args_example = OrderedDict(
    clientId="xxxx-xxxx-xxxx-xxxx",
    clientSecret="<secret>",
    tenantId="xxxx-xxxx-xxxx-xxxx",
)
