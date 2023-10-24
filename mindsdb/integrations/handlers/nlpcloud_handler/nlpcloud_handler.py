import nlpcloud
from collections import OrderedDict

from mindsdb.integrations.handlers.nlpcloud_handler.nlpcloud_tables import (
    NLPCloudTranslationTable,
    NLPCloudSummarizationTable,
    NLPCloudSentimentTable,
    NLPCloudLangDetectionTable,
    NLPCloudNERTable
)
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

from mindsdb.utilities.log import get_log
from mindsdb_sql import parse_sql


logger = get_log("integrations.nlpcloud_handler")


class NLPCloudHandler(APIHandler):
    """The NLPCloud handler implementation"""

    def __init__(self, name: str, **kwargs):
        """Initialize the NLPCloud handler.

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

        translation_data = NLPCloudTranslationTable(self)
        self._register_table("translation", translation_data)

        summarization_data = NLPCloudSummarizationTable(self)
        self._register_table("summarization", summarization_data)

        sentiment_data = NLPCloudSentimentTable(self)
        self._register_table("sentiment_analysis", sentiment_data)

        lang_detect_data = NLPCloudLangDetectionTable(self)
        self._register_table("language_detection", lang_detect_data)

        ner_data = NLPCloudNERTable(self)
        self._register_table("named_entity_recognition", ner_data)

    def connect(self) -> StatusResponse:
        """Set up the connection required by the handler.

        Returns
        -------
        StatusResponse
            connection object
        """
        resp = StatusResponse(False)
        self.client = nlpcloud.Client(model=self.connection_data.get("model"), token=self.connection_data.get("token"), gpu=self.connection_data.get("gpu", False), lang=self.connection_data.get("lang", ""))
        try:
            self.client.langdetection("hello mindsdb!!")
            resp.success = True
            return resp
        except Exception as ex:
            resp.success = False
            resp.error_message = ex
        self.is_connected = True
        return resp

    def check_connection(self) -> StatusResponse:
        """Check connection to the handler.

        Returns
        -------
        StatusResponse
            Status confirmation
        """
        if self.is_connected:
            return StatusResponse(True)
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
        ast = parse_sql(query, dialect="mindsdb")
        return self.query(ast)


connection_args = OrderedDict(
    token={
        "type": ARG_TYPE.STR,
        "description": "NLPCloud API Toekn",
        "required": True,
        "label": "token",
    },
    model={
        "type": ARG_TYPE.STR,
        "description": "NLPCloud model",
        "required": True,
        "label": "model",
    },
    gpu={
        "type": ARG_TYPE.BOOL,
        "description": "NLPCloud use gpu",
        "required": False,
        "label": "gpu",
    },
    lang={
        "type": ARG_TYPE.STR,
        "description": "NLPCloud language",
        "required": False,
        "label": "lang",
    }
)

connection_args_example = OrderedDict(
    token="token",
    model="model",
    gpu=False,
    lang="en"
)
