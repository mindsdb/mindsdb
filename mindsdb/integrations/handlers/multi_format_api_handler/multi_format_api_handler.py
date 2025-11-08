"""
Multi-Format API Handler for MindsDB.
Fetches and parses data from web APIs/pages in multiple formats (XML, JSON, CSV).
"""

import requests
from typing import Optional, Dict
import logging

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse

from .multi_format_table import MultiFormatAPITable
from .__about__ import __version__ as version

logger = logging.getLogger(__name__)


class MultiFormatAPIHandler(APIHandler):
    """
    Handler for fetching and parsing multi-format data from web APIs/pages.

    Supports:
    - JSON (application/json)
    - XML (application/xml, text/xml, RSS/Atom feeds)
    - CSV (text/csv, text/plain)

    Automatically detects format from Content-Type header or URL extension.
    """

    name = 'multi_format_api'

    def __init__(self, name: str, **kwargs):
        """
        Initialize handler.

        Args:
            name: Handler instance name
            **kwargs: Additional arguments including connection_data
        """
        super().__init__(name)

        self.connection_args = kwargs.get('connection_data', {})
        self.is_connected = False

        # Register the generic data table
        data_table = MultiFormatAPITable(self)
        self._register_table('data', data_table)

    def connect(self) -> StatusResponse:
        """
        Test connection by checking if requests library is available.

        Returns:
            StatusResponse with connection status
        """
        try:
            # Test that we can make requests
            # If headers are provided in connection args, validate them
            headers = self.connection_args.get('headers', {})
            if headers and not isinstance(headers, dict):
                raise ValueError("Headers must be a dictionary")

            self.is_connected = True
            return StatusResponse(success=True)

        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return StatusResponse(success=False, error_message=str(e))

    def check_connection(self) -> StatusResponse:
        """
        Check if connection is alive.

        Returns:
            StatusResponse with connection status
        """
        return self.connect()

    def native_query(self, query: str) -> StatusResponse:
        """
        Execute native query. Not applicable for this handler.

        Args:
            query: Query string

        Returns:
            StatusResponse indicating not implemented
        """
        return StatusResponse(
            success=False,
            error_message="Native queries are not supported. Use SELECT statements instead."
        )
