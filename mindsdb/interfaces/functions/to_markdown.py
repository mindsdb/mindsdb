from io import BytesIO
import os
from typing import Union
from urllib.parse import urlparse

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

        if file_extension == '.pdf':
            return self._pdf_to_markdown(file_content, **kwargs)
        else:
            raise ValueError(f"Unsupported file type: {file_extension}.")

    def _get_file_content(self, file_path_or_url: str) -> str:
        """
        Retrieves the content of a file.
        """
        parsed_url = urlparse(file_path_or_url)
        if parsed_url.scheme in ('http', 'https'):
            response = requests.get(file_path_or_url)
            if response.status_code == 200:
                return response
            else:
                raise RuntimeError(f'Unable to retrieve file from URL: {file_path_or_url}')
        else:
            with open(file_path_or_url, 'rb') as file:
                return BytesIO(file.read())

    def _get_file_extension(self, file_path_or_url: str) -> str:
        """
        Retrieves the file extension from a file path or URL.
        """
        parsed_url = urlparse(file_path_or_url)
        if parsed_url.scheme in ('http', 'https'):
            try:
                # Make a HEAD request to get headers without downloading the file.
                response = requests.head(file_path_or_url, allow_redirects=True)
                content_type = response.headers.get('Content-Type', '')
                if content_type:
                    ext = mimetypes.guess_extension(content_type.split(';')[0].strip())
                    if ext:
                        return ext

                # Fallback to extracting extension from the URL path
                ext = os.path.splitext(parsed_url.path)[1]
                if ext:
                    return ext
            except requests.RequestException:
                raise RuntimeError(f'Unable to retrieve file extension from URL: {file_path_or_url}')
        else:
            return os.path.splitext(file_path_or_url)[1]

    def _pdf_to_markdown(self, file_content: Union[requests.Response, bytes], **kwargs) -> str:
        """
        Converts a PDF file to markdown.
        """
        if isinstance(file_content, requests.Response):
            file_content = BytesIO(file_content.content)

        markdown_pages = ocr(file_content, **kwargs)
        return "\n\n---\n\n".join(markdown_pages)
