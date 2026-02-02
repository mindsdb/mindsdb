"""Custom document types to replace langchain dependencies"""


class SimpleDocument:
    """Simple document class to replace LangchainDocument

    This class provides a minimal interface compatible with langchain's Document
    class, with page_content and metadata attributes.
    """

    def __init__(self, page_content: str, metadata: dict = None):
        """
        Initialize a SimpleDocument

        Args:
            page_content: The text content of the document
            metadata: Optional dictionary containing document metadata
        """
        self.page_content = page_content
        self.metadata = metadata or {}
