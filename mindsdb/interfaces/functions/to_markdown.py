
class ToMarkdown:
    """
    Extracts the content of documents of various formats in markdown format.
    """

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
        pass

    def _image_to_markdown(self, file_path_or_url: str) -> str:
        """
        Converts images to markdown.
        """
        pass

    def _other_to_markdown(self, file_path_or_url: str) -> str:
        """
        Converts other file formats to markdown.
        """
        pass
