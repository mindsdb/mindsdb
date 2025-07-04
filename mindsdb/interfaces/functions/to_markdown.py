from io import BytesIO
import os
from typing import Union
from urllib.parse import urlparse
import xml.etree.ElementTree as ET

from aipdf import ocr
import mimetypes
import requests


class ToMarkdown:
    """
    Extracts the content of documents of various formats in markdown format.
    """

    def __init__(self):
        """
        Initializes the ToMarkdown class.
        """

    def call(self, file_path_or_url: str, **kwargs) -> str:
        """
        Converts a file to markdown.
        """
        file_extension = self._get_file_extension(file_path_or_url)
        file_content = self._get_file_content(file_path_or_url)

        if file_extension == ".pdf":
            return self._pdf_to_markdown(file_content, **kwargs)

        elif file_extension in (".xml", ".nessus"):
            return self._xml_to_markdown(file_content, **kwargs)

        else:
            raise ValueError(f"Unsupported file type: {file_extension}.")

    def _get_file_content(self, file_path_or_url: str) -> BytesIO:
        """
        Retrieves the content of a file.
        """
        parsed_url = urlparse(file_path_or_url)
        if parsed_url.scheme in ("http", "https"):
            response = requests.get(file_path_or_url)
            if response.status_code == 200:
                return BytesIO(response.content)
            else:
                raise RuntimeError(f"Unable to retrieve file from URL: {file_path_or_url}")
        else:
            with open(file_path_or_url, "rb") as file:
                return BytesIO(file.read())

    def _get_file_extension(self, file_path_or_url: str) -> str:
        """
        Retrieves the file extension from a file path or URL.
        """
        parsed_url = urlparse(file_path_or_url)
        if parsed_url.scheme in ("http", "https"):
            try:
                # Make a HEAD request to get headers without downloading the file.
                response = requests.head(file_path_or_url, allow_redirects=True)
                content_type = response.headers.get("Content-Type", "")
                if content_type:
                    ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
                    if ext:
                        return ext

                # Fallback to extracting extension from the URL path
                ext = os.path.splitext(parsed_url.path)[1]
                if ext:
                    return ext
            except requests.RequestException:
                raise RuntimeError(f"Unable to retrieve file extension from URL: {file_path_or_url}")
        else:
            return os.path.splitext(file_path_or_url)[1]

    def _pdf_to_markdown(self, file_content: Union[requests.Response, BytesIO], **kwargs) -> str:
        """
        Converts a PDF file to markdown.
        """
        markdown_pages = ocr(file_content, **kwargs)
        return "\n\n---\n\n".join(markdown_pages)

    def _xml_to_markdown(self, file_content: Union[requests.Response, BytesIO], **kwargs) -> str:
        """
        Converts an XML (or Nessus) file to markdown.
        """

        def parse_element(element: ET.Element, depth: int = 0) -> str:
            """
            Recursively parses an XML element and converts it to markdown.
            """
            markdown = []
            heading = "#" * (depth + 1)

            markdown.append(f"{heading} {element.tag}")

            for key, val in element.attrib.items():
                markdown.append(f"- **{key}**: {val}")

            text = (element.text or "").strip()
            if text:
                markdown.append(f"\n{text}\n")

            for child in element:
                markdown.append(parse_element(child, depth + 1))

            return "\n".join(markdown)

        root = ET.fromstring(file_content.read().decode("utf-8"))
        markdown_content = parse_element(root)
        return markdown_content
