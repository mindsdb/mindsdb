import os
from openai import AzureOpenAI
from mindsdb.integrations.handlers.azure_openai_handler.constants import DEFAULT_API_VERSION

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class AzureOpenAIHandler(OpenAIHandler):
    name = 'azure_openai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key_name = getattr(self, 'api_key_name', self.name)
        self.api_version = None

    @staticmethod
    def _get_client(api_key: str, base_url: str, org: str = None) -> AzureOpenAI:
        """
        Create an Azure OpenAI client using the expected OpenAIHandler signature.

        Args:
            api_key (str): Azure API key
            base_url (str): Azure endpoint (i.e. https://...azure.com/)
            org (str, optional): Ignored for Azure

        Returns:
            AzureOpenAI: Initialized Azure OpenAI client
        """
        api_version = os.getenv('AZURE_OPENAI_API_VERSION', DEFAULT_API_VERSION)
        return AzureOpenAI(
            api_key=api_key,
            azure_endpoint=base_url,
            api_version=api_version
        )
