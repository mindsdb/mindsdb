from collections import OrderedDict
from typing import List, Optional

from qdrant_client import QdrantClient
import pandas as pd

from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.libs.response import HandlerResponse
from mindsdb.integrations.libs.response import HandlerResponse as Response
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.vectordatabase_handler import (
    FilterCondition,
    FilterOperator,
    TableField,
    VectorStoreHandler,
)
from mindsdb.utilities import log


class QdrantHanlder(VectorStoreHandler):
    """This handler handles connection and execution of the Qdrant statements."""

    name = "qdrant"

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
