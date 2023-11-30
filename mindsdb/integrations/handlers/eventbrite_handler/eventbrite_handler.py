import os

from eventbrite.client import Client
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

from .eventbrite_tables import EventbriteUserTable
from .eventbrite_tables import EventbriteOrganizationTable
from .eventbrite_tables import EventbriteCategoryTable
from .eventbrite_tables import EventbriteSubcategoryTable
from .eventbrite_tables import EventbriteFormatTable
from .eventbrite_tables import EventbriteEventDetailsTable
from .eventbrite_tables import EventbriteEventsTable

logger = log.getLogger(__name__)


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

        userTable = EventbriteUserTable(self)
        self._register_table("user", userTable)

        organizationTable = EventbriteOrganizationTable(self)
        self._register_table("organization", organizationTable)

        categoryTable = EventbriteCategoryTable(self)
        self._register_table("category", categoryTable)

        subcategoryTable = EventbriteSubcategoryTable(self)
        self._register_table("subcategory", subcategoryTable)

        formatTable = EventbriteFormatTable(self)
        self._register_table("formats", formatTable)

        eventDetailsTable = EventbriteEventDetailsTable(self)
        self._register_table("event_details", eventDetailsTable)

        eventsTable = EventbriteEventsTable(self)
        self._register_table("events", eventsTable)

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
            logger.info(f"Connected to Eventbrite as {me['name']}")
            response.success = True

        except Exception as e:
            response.error_message = f"Error connecting to Eventbrite API: {e}"
            logger.error(response.error_message)

        if not response.success and self.is_connected:
            self.is_connected = False

        return response
