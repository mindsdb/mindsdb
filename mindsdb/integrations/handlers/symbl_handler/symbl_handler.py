import symbl
from mindsdb.integrations.handlers.symbl_handler.symbl_tables import (
    GetConversationTable,
    GetMessagesTable,
    GetTopicsTable,
    GetQuestionsTable,
    GetAnalyticsTable,
    GetActionItemsTable,
    GetFollowUpsTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql


logger = log.getLogger(__name__)


class SymblHandler(APIHandler):
    """The Symbl handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the Symbl handler.

        Parameters
        ----------
        name : str
            name of a handler instance
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.credentials = {"app_id": self.connection_data.get("app_id"), "app_secret": self.connection_data.get("app_secret")}
        self.kwargs = kwargs
        self.is_connected = False

        conversation_id_data = GetConversationTable(self)
        self._register_table("get_conversation_id", conversation_id_data)

        messages_data = GetMessagesTable(self)
        self._register_table("get_messages", messages_data)

        topics_data = GetTopicsTable(self)
        self._register_table("get_topics", topics_data)

        question_data = GetQuestionsTable(self)
        self._register_table("get_questions", question_data)

        analytics_data = GetAnalyticsTable(self)
        self._register_table("get_analytics", analytics_data)

        ai_data = GetActionItemsTable(self)
        self._register_table("get_action_items", ai_data)

        follow_up_data = GetFollowUpsTable(self)
        self._register_table("get_follow_ups", follow_up_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        try:
            symbl.AuthenticationToken.get_access_token(self.credentials)
            resp.success = True
            self.is_connected = True
        except Exception as ex:
            resp.success = False
            resp.error_message = ex
            self.is_connected = False
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        return self.connect()

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
