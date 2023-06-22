import mediawikiapi

from mindsdb.integrations.handlers.mediawiki_handler.mediawiki_tables import PagesTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)

from mindsdb.utilities import log
from mindsdb_sql import parse_sql


class MediaWikiHandler(APIHandler):
    """
    The MediaWiki handler implementation.
    """

    name = 'mediawiki'