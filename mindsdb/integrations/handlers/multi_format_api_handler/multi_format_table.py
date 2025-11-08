"""
Multi-Format API Table implementation.
Handles fetching and parsing data from web APIs/pages in multiple formats.
"""

import requests
import pandas as pd
from typing import List, Optional
import logging

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition

from .format_parsers import parse_response

logger = logging.getLogger(__name__)


class MultiFormatAPITable(APIResource):
    """
    Generic API table that fetches and parses data from URLs.
    Supports JSON, XML, and CSV formats with automatic detection.
    """

    def list(
        self,
        conditions: Optional[List[FilterCondition]] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Fetch data from URL and parse according to detected format.

        Args:
            conditions: SQL WHERE conditions (must include 'url')
            limit: Maximum number of rows to return
            **kwargs: Additional arguments

        Returns:
            pandas DataFrame with parsed data
        """
        # Extract URL from conditions
        url = None
        headers = {}
        timeout = 30

        if conditions:
            for condition in conditions:
                column = condition.column.lower()

                if column == 'url':
                    url = condition.value
                    condition.applied = True
                elif column == 'headers':
                    # Allow custom headers as JSON string
                    import json
                    try:
                        headers = json.loads(condition.value)
                    except:
                        logger.warning(f"Invalid headers JSON: {condition.value}")
                    condition.applied = True
                elif column == 'timeout':
                    try:
                        timeout = int(condition.value)
                    except:
                        logger.warning(f"Invalid timeout value: {condition.value}")
                    condition.applied = True

        if not url:
            raise ValueError(
                "URL must be specified in WHERE clause. "
                "Example: SELECT * FROM multi_format.data WHERE url='https://example.com/api/data'"
            )

        # Fetch data from URL
        logger.info(f"Fetching data from: {url}")

        try:
            # Get default headers from connection args if available
            connection_headers = self.handler.connection_args.get('headers', {})
            if connection_headers:
                headers = {**connection_headers, **headers}

            # Add default User-Agent if not provided
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'MindsDB Multi-Format API Handler/1.0'

            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise ValueError(f"Failed to fetch data from {url}: {e}")

        # Parse response based on detected format
        try:
            df = parse_response(response, url)
        except ValueError as e:
            logger.error(f"Parsing failed: {e}")
            raise

        # Apply limit if specified
        if limit and len(df) > limit:
            df = df.head(limit)

        logger.info(f"Successfully parsed {len(df)} rows from {url}")
        return df

    def get_columns(self) -> List[str]:
        """
        Return column list. Since columns are dynamic based on URL content,
        we return a generic set. Actual columns come from the parsed data.

        Returns:
            List of column names
        """
        return ['data']
