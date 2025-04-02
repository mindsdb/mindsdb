import base64
from io import BytesIO
import os
from typing import Union
from urllib.parse import urlparse

import fitz  # PyMuPDF
from markitdown import MarkItDown
import mimetypes
from openai import OpenAI
import requests


class ToMarkdown:
    """
    Extracts the content of documents of various formats in markdown format.
    """
    def __init__(self, use_llm: bool, llm_client: OpenAI = None, llm_model: str = None):
        """
        Initializes the ToMarkdown class.
        """
        # If use_llm is True, llm_client and llm_model must be provided.
        if use_llm and (llm_client is None or llm_model is None):
            raise ValueError('LLM client and model must be provided when use_llm is True.')

        # If use_llm is False, set llm_client and llm_model to None even if they are provided.
        if not use_llm:
            llm_client = None
            llm_model = None

        # Only OpenAI is supported for now.
        # TODO: Add support for other LLMs.
        if llm_client is not None and not isinstance(llm_client, OpenAI):
            raise ValueError('Only OpenAI models are supported at the moment.')

        self.use_llm = use_llm
        self.llm_client = llm_client
        self.llm_model = llm_model

    def call(self, file_path_or_url: str) -> str:
        """
        Converts a file to markdown.
        """
        file_extension = self._get_file_extension(file_path_or_url)
        file = self._get_file_content(file_path_or_url)

        if file_extension == '.pdf':
            return self._pdf_to_markdown(file)
        elif file_extension in ['.jpg', '.jpeg', '.png', '.gif']:
            return self._image_to_markdown(file)
        else:
            return self._other_to_markdown(file)

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

    def _pdf_to_markdown(self, file_content: Union[requests.Response, bytes]) -> str:
        """
        Converts a PDF file to markdown.
        """
        if self.llm_client is None:
            return self._pdf_to_markdown_no_llm(file_content)
        else:
            return self._pdf_to_markdown_llm(file_content)

    def _pdf_to_markdown_llm(self, file_content: Union[requests.Response, BytesIO]) -> str:
        """
        Converts a PDF file to markdown using LLM.
        The LLM is used mainly for the purpose of generating descriptions of any images in the PDF.
        """
        if isinstance(file_content, requests.Response):
            file_content = BytesIO(file_content.content)

        document = fitz.open(stream=file_content, filetype="pdf")

        markdown_content = []
        for page_num in range(len(document)):
            page = document.load_page(page_num)

            # Get text blocks with coordinates.
            page_content = []
            blocks = page.get_text("blocks")
            for block in blocks:
                x0, y0, x1, y1, text, _, _ = block
                if text.strip():  # Skip empty or whitespace blocks.
                    page_content.append((y0, text.strip()))

            # Extract images from the page.
            image_list = page.get_images(full=True)
            for img_index, img in enumerate(image_list):
                xref = img[0]
                base_image = document.extract_image(xref)
                image_bytes = base_image["image"]

                # Use actual image y-coordinate if available.
                y0 = float(base_image.get("y", 0))
                image_description = self._generate_image_description(image_bytes)
                page_content.append((y0, f"![{image_description}](image_{page_num + 1}_{img_index + 1}.png)"))

            # Sort the content by y0 coordinate
            page_content.sort(key=lambda x: x[0])

            # Add sorted content to the markdown
            for _, text in page_content:
                markdown_content.append(text)
            markdown_content.append("\n")

        document.close()

        return "\n".join(markdown_content)

    def _generate_image_description(self, image_bytes: bytes) -> str:
        """
        Generates a description of the image using LLM.
        """
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")

        response = self.llm_client.chat.completions.create(
            model=self.llm_model,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Describe this image"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_base64}"}},
                    ],
                }
            ],
        )
        description = response.choices[0].message.content
        return description

    def _pdf_to_markdown_no_llm(self, file_content: Union[requests.Response, BytesIO]) -> str:
        """
        Converts a PDF file to markdown without using LLM.
        """
        md = MarkItDown(enable_plugins=True)
        result = md.convert(file_content)
        return result.markdown

    def _image_to_markdown(self, file_content: Union[requests.Response, BytesIO]) -> str:
        """
        Converts images to markdown.
        """
        if not self.use_llm or self.llm_client is None:
            raise ValueError('LLM client must be enabled to convert images to markdown.')

        md = MarkItDown(llm_client=self.llm_client, llm_model=self.llm_model, enable_plugins=True)
        result = md.convert(file_content)
        return result.markdown

    def _other_to_markdown(self, file_content: Union[requests.Response, BytesIO]) -> str:
        """
        Converts other file formats to markdown.
        """
        md = MarkItDown(enable_plugins=True)
        result = md.convert(file_content)
        return result.markdown
