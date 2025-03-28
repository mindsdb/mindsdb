from markitdown import MarkItDown
from openai import OpenAI
import requests

from mindsdb.integrations.handlers.web_handler.urlcrawl_helpers import pdf_to_markdown


class ToMarkdown:
    """
    Extracts the content of documents of various formats in markdown format.
    """
    def __init__(self, llm_client: OpenAI = None, llm_model: str = None):
        """
        Initializes the ToMarkdown class.
        """
        # Only OpenAI is supported for now.
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
            pass

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
