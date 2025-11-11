"""
Format detection and parsing utilities for multiple data formats.
Supports JSON, XML, and CSV content from web APIs/pages.
"""

import io
import json
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Any
import pandas as pd
import logging
import re
from html import unescape

logger = logging.getLogger(__name__)


def detect_format(response, url: str) -> Optional[str]:
    """
    Detect format from Content-Type header or URL extension.

    Args:
        response: requests.Response object
        url: URL string

    Returns:
        Format string ('json', 'xml', 'csv') or None if cannot detect
    """
    # Try Content-Type header first
    content_type = response.headers.get('Content-Type', '').lower()

    if 'application/json' in content_type or 'application/javascript' in content_type:
        return 'json'
    elif 'application/xml' in content_type or 'text/xml' in content_type:
        return 'xml'
    elif 'text/csv' in content_type:
        return 'csv'
    elif 'text/plain' in content_type:
        # Plain text could be CSV, try to detect
        if ',' in response.text[:1000]:  # Check first 1000 chars
            return 'csv'

    # Fallback to URL extension
    url_lower = url.lower()
    if url_lower.endswith('.json') or '/json' in url_lower:
        return 'json'
    elif url_lower.endswith('.xml') or '/xml' in url_lower or 'feed' in url_lower or 'rss' in url_lower:
        return 'xml'
    elif url_lower.endswith('.csv'):
        return 'csv'

    # Try to auto-detect from content
    content = response.text.strip()
    if content:
        first_char = content[0]
        if first_char in ['{', '[']:
            return 'json'
        elif first_char == '<':
            return 'xml'

    return None


def parse_json(content: str) -> pd.DataFrame:
    """
    Parse JSON content and convert to DataFrame.
    Handles nested structures using json_normalize.

    Args:
        content: JSON string

    Returns:
        pandas DataFrame
    """
    try:
        data = json.loads(content)

        if isinstance(data, list):
            if len(data) == 0:
                return pd.DataFrame()
            # List of objects
            return pd.json_normalize(data)
        elif isinstance(data, dict):
            # Check if dict contains a list that should be the main data
            # Common patterns: {"data": [...], "results": [...], "items": [...]}
            for key in ['data', 'results', 'items', 'records', 'rows', 'entries']:
                if key in data and isinstance(data[key], list):
                    logger.info(f"Extracting list from '{key}' field")
                    return pd.json_normalize(data[key])

            # Single object or nested structure
            return pd.json_normalize(data)
        else:
            # Primitive type, wrap in DataFrame
            return pd.DataFrame({'value': [data]})

    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
        raise ValueError(f"Invalid JSON content: {e}")


def parse_xml(content: str) -> pd.DataFrame:
    """
    Parse XML content and convert to DataFrame.
    Handles common XML structures and RSS/Atom feeds.

    Args:
        content: XML string

    Returns:
        pandas DataFrame
    """
    try:
        root = ET.fromstring(content)
        records = []

        # Handle RSS/Atom feeds
        if root.tag.endswith('rss') or root.tag.endswith('feed'):
            # RSS feed
            for item in root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                record = _xml_element_to_dict(item)
                records.append(record)
        else:
            # Generic XML structure
            # If root has multiple children of the same type, treat them as records
            children = list(root)
            if children:
                # Group by tag name
                tag_counts = {}
                for child in children:
                    tag = child.tag
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1

                # Find the most common tag (likely the record type)
                if tag_counts:
                    most_common_tag = max(tag_counts, key=tag_counts.get)
                    if tag_counts[most_common_tag] > 1:
                        # Multiple elements of same type - treat as records
                        for child in root.findall(f'.//{most_common_tag}'):
                            record = _xml_element_to_dict(child)
                            records.append(record)
                    else:
                        # Different tags - treat each child as a record
                        for child in children:
                            record = _xml_element_to_dict(child)
                            record['_tag'] = child.tag
                            records.append(record)
                else:
                    # No children, convert root element
                    records.append(_xml_element_to_dict(root))
            else:
                # Root has no children, just text content
                records.append({'content': root.text or ''})

        if not records:
            # Empty XML or no parseable structure
            return pd.DataFrame({'root_tag': [root.tag], 'content': [root.text or '']})

        return pd.DataFrame(records)

    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        raise ValueError(f"Invalid XML content: {e}")


def _clean_cdata_content(text: str) -> str:
    """
    Clean CDATA content by stripping HTML tags and HTML entities, and trimming whitespace.

    Args:
        text: Raw text content that may contain HTML tags and entities

    Returns:
        Cleaned text content
    """
    if not text:
        return ''

    # Strip leading/trailing whitespace (common in CDATA sections)
    text = text.strip()

    # Remove HTML tags (e.g., <a href="...">URL</a> -> URL)
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities (e.g., &amp; -> &, &lt; -> <)
    text = unescape(text)

    # Remove any remaining excessive whitespace
    text = ' '.join(text.split())

    return text


def _xml_element_to_dict(element: ET.Element) -> Dict[str, Any]:
    """
    Convert XML element to dictionary.

    Args:
        element: XML Element

    Returns:
        Dictionary representation
    """
    result = {}

    # Add attributes
    if element.attrib:
        for key, value in element.attrib.items():
            result[f'@{key}'] = value

    # Add text content
    if element.text and element.text.strip():
        result['text'] = _clean_cdata_content(element.text)

    # Add child elements
    for child in element:
        # Remove namespace from tag
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag

        if len(list(child)) == 0:
            # Leaf node - just get text and clean it
            result[tag] = _clean_cdata_content(child.text or '')
        else:
            # Has children - recursively convert
            child_dict = _xml_element_to_dict(child)
            if tag in result:
                # Duplicate tag - convert to list
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict

    return result


def parse_csv(content: str) -> pd.DataFrame:
    """
    Parse CSV content and convert to DataFrame.

    Args:
        content: CSV string

    Returns:
        pandas DataFrame
    """
    try:
        # Use StringIO to read CSV from string
        return pd.read_csv(io.StringIO(content))
    except Exception as e:
        logger.error(f"CSV parsing error: {e}")
        raise ValueError(f"Invalid CSV content: {e}")


def parse_response(response, url: str) -> pd.DataFrame:
    """
    Auto-detect format and parse response to DataFrame.

    Args:
        response: requests.Response object
        url: URL string

    Returns:
        pandas DataFrame
    """
    format_type = detect_format(response, url)

    if format_type == 'json':
        logger.info("Detected JSON format")
        return parse_json(response.text)
    elif format_type == 'xml':
        logger.info("Detected XML format")
        return parse_xml(response.text)
    elif format_type == 'csv':
        logger.info("Detected CSV format")
        return parse_csv(response.text)
    else:
        # Try JSON as default fallback
        logger.warning(f"Could not detect format, trying JSON as fallback")
        try:
            return parse_json(response.text)
        except ValueError:
            # Try XML as second fallback
            try:
                logger.warning("JSON failed, trying XML as fallback")
                return parse_xml(response.text)
            except ValueError:
                raise ValueError(
                    f"Unable to detect or parse format for URL: {url}. "
                    f"Content-Type: {response.headers.get('Content-Type', 'unknown')}"
                )
