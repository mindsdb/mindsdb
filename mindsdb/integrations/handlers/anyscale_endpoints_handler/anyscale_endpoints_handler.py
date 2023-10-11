import os
import contextlib
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler, OPENAI_API_BASE


CHAT_MODELS = (
    'meta-llama/Llama-2-7b-chat-hf',
    'meta-llama/Llama-2-13b-chat-hf',
    'meta-llama/Llama-2-70b-chat-hf',
    'codellama/CodeLlama-34b-Instruct-hf',
)
ANYSCALE_API_BASE = 'https://api.endpoints.anyscale.com/v1'


class AnyscaleEndpointsHandler(OpenAIHandler):
    name = 'anyscale_endpoints'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.chat_completion_models = CHAT_MODELS
        self.default_model = CHAT_MODELS[0]
        self.base_api = ANYSCALE_API_BASE
        self.default_mode = 'default'  # can also be 'conversational' or 'conversational-full'
        self.supported_modes = ['default', 'conversational', 'conversational-full']
        self.rate_limit = 25  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100
        self.supported_ft_models = CHAT_MODELS  # base models compatible with fine-tuning

    @staticmethod
    @contextlib.contextmanager
    def overwritten_base_api(key='OPENAI_API_BASE'):
        """ Temporarily updates the API base env var to point towards the Anyscale URL. """
        old_base = os.environ.get(key, OPENAI_API_BASE)
        os.environ[key] = ANYSCALE_API_BASE
        try:
            yield  # enter
        finally:
            os.environ[key] = old_base  # exit

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        with self.overwritten_base_api():
            return super().predict(df, args)

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        if 'api_key' in args:
            del args['api_key']

        if attribute == 'args':
            return pd.DataFrame(args.items(), columns=['key', 'value'])
        elif attribute == 'metadata':
            # TODO: reimplement commented from huggingface's model cards (e.g. https://huggingface.co/meta-llama/Llama-2-7b-chat-hf)  # noqa
            # api_key = get_api_key('openai', args, self.engine_storage)
            # meta = openai.Model.retrieve(model_name, api_key=api_key)
            # return pd.DataFrame(meta.items(), columns=['key', 'value'])
            model_name = args.get('model_name', self.default_model)
            return pd.DataFrame({'model_name': [model_name]})
        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])
