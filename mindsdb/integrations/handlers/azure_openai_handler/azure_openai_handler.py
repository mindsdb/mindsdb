import os
from openai import AzureOpenAI
from azure.core.credentials import AzureKeyCredential
from constants import DEFAULT_API_VERSION

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class AzureOpenAIHandler(OpenAIHandler):
    name = 'azure_openai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key_name = getattr(self, 'api_key_name', self.name)
        self.api_version = None  # determined at runtime

    @staticmethod
    def _get_client(api_key: str, base_url: str, org: str = None, version: str = None) -> AzureOpenAI:
        return AzureOpenAI(
            credential=AzureKeyCredential(api_key),
            endpoint=base_url,
            api_version=version or DEFAULT_API_VERSION
        )

    def create_engine(self, connection_args: dict) -> None:
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get('azure_openai_api_key') or os.environ.get('AZURE_OPENAI_API_KEY')
        api_base = connection_args.get('api_base') or os.environ.get('AZURE_OPENAI_API_BASE')
        api_version = connection_args.get('api_version') or os.environ.get('AZURE_OPENAI_API_VERSION', DEFAULT_API_VERSION)

        if not all([api_key, api_base, api_version]):
            raise Exception("Azure OpenAI requires `azure_openai_api_key` and `api_base`.")
