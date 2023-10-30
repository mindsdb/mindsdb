import os
import pandas as pd

from eventbrite.client import Client
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

from .eventbrite_tables import UserInfoTable
from .eventbrite_tables import OrganizationInfoTable
from .eventbrite_tables import CategoryInfoTable
from .eventbrite_tables import SubcategoryInfoTable
from .eventbrite_tables import FormatTable
from .eventbrite_tables import EventDetailsTable
from .eventbrite_tables import ListEventsTable


class EventbriteHandler(APIHandler):
    """A class for handling connections and interactions with the Eventbrite API.

    Attributes:
        api (Client): The `Client` object for accessing Eventbrite API.
    """

    def __init__(self, name=None, **kwargs):
        super().__init__(name)

        args = kwargs.get("connection_data", {})
        self.connection_args = {}
        handler_config = Config().get("eventbrite_handler", {})

        # Set up connection arguments
        for k in ["access_token"]:
            if k in args:
                self.connection_args[k] = args[k]
            elif f"EVENTBRITE_{k.upper()}" in os.environ:
                self.connection_args[k] = os.environ[f"EVENTBRITE_{k.upper()}"]
            elif k in handler_config:
                self.connection_args[k] = handler_config[k]

        self.api = None
        self.is_connected = False

        userInfoTable = UserInfoTable(self)
        self._register_table("userInfoTable", userInfoTable)

        organizationInfoTable = OrganizationInfoTable(self)
        self._register_table("organizationInfoTable", organizationInfoTable)

        categoryInfoTable = CategoryInfoTable(self)
        self._register_table("categoryInfoTable", categoryInfoTable)

        subcategoryInfoTable = SubcategoryInfoTable(self)
        self._register_table("subcategoryInfoTable", subcategoryInfoTable)

        formatInfoTable = FormatTable(self)
        self._register_table("formatInfoTable", formatInfoTable)

        eventDetailsTable = EventDetailsTable(self)
        self._register_table("eventDetailsTable", eventDetailsTable)

        listEventsTable = ListEventsTable(self)
        self._register_table("listEventsTable", listEventsTable)

    def connect(self):
        """Initialize the Eventbrite API Client."""
        if self.is_connected:
            return self.api

        self.api = Client(access_token=self.connection_args["access_token"])

        self.is_connected = True
        return self.api

    def check_connection(self) -> StatusResponse:
        """Check the connection to the Eventbrite API."""
        response = StatusResponse(False)

        try:
            api = self.connect()
            me = api.get_current_user()
            log.logger.info(f"Connected to Eventbrite as {me['name']}")
            response.success = True

        except Exception as e:
            response.error_message = f"Error connecting to Eventbrite API: {e}"
            log.logger.error(response.error_message)

        if not response.success and self.is_connected:
            self.is_connected = False

        return response
