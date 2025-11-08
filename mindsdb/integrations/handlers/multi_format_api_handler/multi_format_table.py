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
        timeout = self.handler.connection_args.get('timeout', 30)
        max_content_size_mb = self.handler.connection_args.get('max_content_size', 100)

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
                elif column == 'max_content_size':
                    try:
                        max_content_size_mb = float(condition.value)
                    except:
                        logger.warning(f"Invalid max_content_size value: {condition.value}")
                    condition.applied = True

        # Use connection-level URL if query-level not provided
        if not url:
            url = self.handler.connection_args.get('url')

        if not url:
            raise ValueError(
                "URL must be specified either in connection configuration or WHERE clause. "
                "Connection config: CREATE DATABASE ... WITH ENGINE='multi_format_api', PARAMETERS={'url': '...'} "
                "OR Query: SELECT * FROM handler.data WHERE url='https://example.com/api/data'"
            )

        # Fetch data from URL
        logger.info(f"Fetching data from: {url}")

        # Convert MB to bytes
        max_content_size_bytes = max_content_size_mb * 1024 * 1024

        try:
            # Get default headers from connection args if available
            connection_headers = self.handler.connection_args.get('headers', {})
            if connection_headers:
                headers = {**connection_headers, **headers}

            # Add default User-Agent if not provided
            if 'User-Agent' not in headers:
                headers['User-Agent'] = 'MindsDB Multi-Format API Handler/1.0'

            # Stage 1: HEAD request to check Content-Length (fail fast)
            try:
                head_response = requests.head(url, headers=headers, timeout=min(10, timeout), allow_redirects=True)
                content_length = head_response.headers.get('Content-Length')

                if content_length:
                    content_length = int(content_length)
                    if content_length > max_content_size_bytes:
                        raise ValueError(
                            f"Content size ({content_length / 1024 / 1024:.2f} MB) exceeds maximum allowed "
                            f"size ({max_content_size_mb} MB). Increase max_content_size parameter if needed."
                        )
                    logger.info(f"Content-Length: {content_length / 1024 / 1024:.2f} MB")
            except requests.exceptions.RequestException as e:
                # HEAD request failed, proceed with GET but monitor size
                logger.warning(f"HEAD request failed: {e}. Proceeding with GET request.")

            # Stage 2: GET request with streaming and size monitoring
            response = requests.get(url, headers=headers, timeout=timeout, stream=True)
            response.raise_for_status()

            # Download in chunks and monitor size
            content_chunks = []
            bytes_downloaded = 0
            chunk_size = 8192  # 8KB chunks

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive chunks
                    bytes_downloaded += len(chunk)

                    if bytes_downloaded > max_content_size_bytes:
                        response.close()
                        raise ValueError(
                            f"Content size exceeded {max_content_size_mb} MB during download. "
                            f"Downloaded {bytes_downloaded / 1024 / 1024:.2f} MB before stopping. "
                            f"Increase max_content_size parameter if needed."
                        )

                    content_chunks.append(chunk)

            # Combine chunks and decode
            content_bytes = b''.join(content_chunks)
            logger.info(f"Successfully downloaded {bytes_downloaded / 1024 / 1024:.2f} MB from {url}")

            # Create a mock response object for parse_response
            class MockResponse:
                def __init__(self, content, headers):
                    self.text = content
                    self.headers = headers
                    self.content = content.encode('utf-8') if isinstance(content, str) else content

            # Decode content
            try:
                content_text = content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # Try other encodings
                try:
                    content_text = content_bytes.decode('latin-1')
                except UnicodeDecodeError:
                    content_text = content_bytes.decode('utf-8', errors='replace')

            mock_response = MockResponse(content_text, response.headers)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise ValueError(f"Failed to fetch data from {url}: {e}")

        # Parse response based on detected format
        try:
            df = parse_response(mock_response, url)
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
