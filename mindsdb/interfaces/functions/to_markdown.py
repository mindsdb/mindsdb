import base64
from io import BytesIO

import fitz  # PyMuPDF
from markitdown import MarkItDown
from openai import OpenAI
import requests


from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import pdf_to_markdown


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
        # Only OpenAI is supported for now.
        if llm_client is not None and not isinstance(llm_client, OpenAI):
            raise ValueError('Only OpenAI models are supported at the moment.')
        # TODO: Add support for other LLMs?
        self.llm_client = llm_client
        self.llm_model = llm_model

    def call(self, file_path_or_url: str) -> str:
        """
        Converts a file to markdown.
        """
        if file_path_or_url.endswith('.pdf'):
            return self._pdf_to_markdown(file_path_or_url)
        elif file_path_or_url.endswith(('.png', '.jpg', '.jpeg', '.gif')):
            return self._image_to_markdown(file_path_or_url)
        else:
            return self._other_to_markdown(file_path_or_url)

    def _pdf_to_markdown(self, file_path_or_url: str) -> str:
        """
        Converts a PDF file to markdown.
        """
        file_content = self._get_file_content(file_path_or_url)

        if self.llm_client is None:
            return pdf_to_markdown(file_content)
        else:
            return self._pdf_to_markdown_llm(file_content)

    def _pdf_to_markdown_llm(self, file_content: bytes) -> str:
        """
        Converts a PDF file to markdown using LLM.
        The LLM is used mainly for the purpose of generating descriptions of any images in the PDF.
        """
        pdf_stream = BytesIO(file_content)
        document = fitz.open(stream=pdf_stream, filetype="pdf")

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

    def _pdf_to_markdown_no_llm(self, file_content: bytes) -> str:
        """
        Converts a PDF file to markdown without using LLM.
        This is done using one of the helper functions used for the web handler.
        """
        return pdf_to_markdown(file_content)

    def _get_file_content(self, file_path_or_url: str) -> str:
        """
        Retrieves the content of a file.
        """
        if file_path_or_url.startswith('http'):
            response = requests.get(file_path_or_url)
            if response.status_code == 200:
                return response.content
            else:
                raise RuntimeError(f'Unable to retrieve file from URL: {file_path_or_url}')
        else:
            with open(file_path_or_url, 'rb') as file:
                return file.read()

    def _image_to_markdown(self, file_path_or_url: str) -> str:
        """
        Converts images to markdown.
        """
        if self.llm_client is None:
            raise RuntimeError('LLM client is not initialized.')

        md = MarkItDown(llm_client=self.llm_client, llm_model="gpt-4o")
        result = md.convert(file_path_or_url)
        return result.markdown

    def _other_to_markdown(self, file_path_or_url: str) -> str:
        """
        Converts other file formats to markdown.
        """
        md = MarkItDown(enable_plugins=True)
        result = md.convert(file_path_or_url)
        return result.markdown
