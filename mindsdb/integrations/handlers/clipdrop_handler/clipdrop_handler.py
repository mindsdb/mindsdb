from collections import OrderedDict

from mindsdb.integrations.handlers.clipdrop_handler.clipdrop_tables import (
    RemoveTextTable,
    RemoveBackgroundTable,
    SketchToImageTable,
    TextToImageTable,
    ReplaceBackgroundTable,
    ReimagineTable
)
from mindsdb.integrations.handlers.clipdrop_handler.clipdrop import ClipdropClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql


logger = get_log("integrations.clipdrop_handler")


class ClipdropHandler(APIHandler):
    """The Clipdrop handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Clipdrop handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs
        self.client = None
        self.is_connected = False

        remove_text_data = RemoveTextTable(self)
        self._register_table("remove_text", remove_text_data)

        remove_bq_data = RemoveBackgroundTable(self)
        self._register_table("remove_background", remove_bq_data)

        sk_image_data = SketchToImageTable(self)
        self._register_table("sketch_to_image", sk_image_data)

        txt_img_data = TextToImageTable(self)
        self._register_table("text_to_image", txt_img_data)

        replace_bg_data = ReplaceBackgroundTable(self)
        self._register_table("replace_background", replace_bg_data)

        reimagine_data = ReimagineTable(self)
        self._register_table("reimagine", reimagine_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        # FYI - No way currently to establish a connection with the given key
        # without making a requests that is chargeable
        # TODO - Find a way to solve FYI
        self.client = ClipdropClient(self.connection_data.get("api_key"),
                                     self.connection_data.get("dir_to_save"))
        self.is_connected = True
        return StatusResponse(True)

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        # FYI - No way currently to establish a connection with the given key
        # without making a requests that is chargeable
        # TODO - Find a way to solve FYI
        self.is_connected = True
        return StatusResponse(True)

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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)


connection_args = OrderedDict(
    api_key={
        "type": ARG_TYPE.STR,
        "description": "Clipdrop API key",
        "required": True,
        "label": "api_key",
    },
    dir_to_save={
        "type": ARG_TYPE.STR,
        "description": "The local directory to save Clipdrop API response",
        "required": True,
        "label": "dir_to_save",
    }
)

connection_args_example = OrderedDict(
    api_key="api_key",
    dir_to_save="/Users/sam/Downloads"
)
