from typing import Text, Dict, Callable

from mindsdb.utilities import log
from mindsdb_sql import parse_sql

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb.integrations.libs.api_handler import APIHandler

logger = log.getLogger(__name__)


class MSOneDriveHandler(APIHandler):
    """
    This handler handles the connection and execution of SQL statements on Microsoft One Drive.
    """

    name = 'one_drive'
