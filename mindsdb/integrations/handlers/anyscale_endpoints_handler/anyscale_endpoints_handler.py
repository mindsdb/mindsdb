import os
import json
import contextlib
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.openai_handler.constants import OPENAI_API_BASE


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
        self.all_models = list(CHAT_MODELS)
        self.chat_completion_models = CHAT_MODELS
        self.supported_ft_models = CHAT_MODELS  # base models compatible with fine-tuning
        self.default_model = CHAT_MODELS[0]
        self.base_api = ANYSCALE_API_BASE
        self.default_mode = 'default'  # can also be 'conversational' or 'conversational-full'
        self.supported_modes = ['default', 'conversational', 'conversational-full']
        self.rate_limit = 25  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100

    @staticmethod
    @contextlib.contextmanager
    def _anyscale_base_api(key='OPENAI_API_BASE'):
        """ Temporarily updates the API base env var to point towards the Anyscale URL. """
        old_base = os.environ.get(key, OPENAI_API_BASE)
        os.environ[key] = ANYSCALE_API_BASE
        try:
            yield  # enter
        finally:
            os.environ[key] = old_base  # exit

    def create(self, target, args=None, **kwargs):
        with self._anyscale_base_api():
            return super().create(target, args, **kwargs)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        with self._anyscale_base_api():
            return super().predict(df, args)

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        with self._anyscale_base_api():
            return super().finetune(df, args)

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        if 'api_key' in args:
            del args['api_key']

        if attribute == 'args':
            return pd.DataFrame(args.items(), columns=['key', 'value'])
        elif attribute == 'metadata':
            # we opt for the URL because some models require completing a form to access their artifacts
            model_name = args.get('model_name', self.default_model)
            model_card_url = 'https://huggingface.co/' + model_name
            return pd.DataFrame({'model_name': [model_name], 'model_card': [model_card_url]})
        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])

    @staticmethod
    def _check_ft_cols(df, cols):
        for col in ['role', 'content']:
            if col not in set(df.columns):
                raise Exception(f"To fine-tune this model, format your select data query to have a `role` column and a `content` column.")  # noqa

    def _prepare_ft_jsonl(self, df, temp_storage_path, temp_filename, _, test_size=0.2):
        """
            df: has exactly two columns, `role` and `content`. Rows contain >= 1 chats in long (stacked) format.
            For more details, check `FineTuning -> Data Format` in the Anyscale API reference.
        """
        # 1. aggregate each chat sequence into one row
        chats = []
        chat = []
        for i, row in df.iterrows():
            if row['role'] == 'system' and len(chat) > 0:
                chats.append({'messages': chat})
                chat = []
            event = {'role': row['role'], 'content': row['content']}
            chat.append(event)
        chats.append({'messages': chat})
        series = pd.Series(chats)
        train = series.iloc[:int(len(series)*(1-test_size))]
        val = series.iloc[-int(len(series)*test_size)-1:]

        # 2. write as jsonl
        file_names = {
            'train': f'{temp_filename}_prepared_train.jsonl',
            'val': f'{temp_filename}_prepared_valid.jsonl',
        }
        train.to_json(os.path.join(temp_storage_path, file_names['train']), orient='records', lines=True)
        val.to_json(os.path.join(temp_storage_path, file_names['val']), orient='records', lines=True)

        # 3. validate
        self._validate_jsonl(os.path.join(temp_storage_path, file_names['train']))
        self._validate_jsonl(os.path.join(temp_storage_path, file_names['val']))
        return file_names

    @staticmethod
    def _get_ft_model_type(model_name: str):
        for base_model in CHAT_MODELS:
            if base_model.lower() in model_name.lower():
                return base_model
        raise Exception(f'Model {model_name} cannot be finetuned.')

    @staticmethod
    def _add_extra_ft_params(ft_params, using_args):
        extra_params = {
            'hyperparameters': {
                'n_epochs': using_args.get('n_epochs', None),
                'context_length': using_args.get('context_length', None),
            }
        }
        return {**ft_params, **extra_params}

    @staticmethod
    def _validate_jsonl(jsonl_path):
        """ Borrowed from Anyscale docs. We may want something customized in the future though. """
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            items = [json.loads(line) for line in f]

        def check_data_for_format_errors(items: list):
            for line_num, batch in enumerate(items):
                prefix = f"Error in line #{line_num + 1}: "
                if not isinstance(batch, dict):
                    raise Exception(f"{prefix}Each line in the provided data should be a dictionary")

                if "messages" not in batch:
                    raise Exception(f"{prefix}Each line in the provided data should have a 'messages' key")

                if not isinstance(batch["messages"], list):
                    raise Exception(f"{prefix}Each line in the provided data should have a 'messages' key with a list of messages")  # noqa

                messages = batch["messages"]
                if not any(message.get("role", None) == "assistant" for message in messages):
                    raise Exception(f"{prefix}Each message list should have at least one message with role 'assistant'")  # noqa

                for message_num, message in enumerate(messages):
                    prefix = f"Error in line #{line_num + 1}, message #{message_num + 1}: "
                    if "role" not in message or "content" not in message:
                        raise Exception(f"{prefix}Each message should have a 'role' and 'content' key")

                    if any(k not in ("role", "content", "name") for k in message):
                        raise Exception(f"{prefix}Each message should only have 'role', 'content', and 'name' keys, any other key is not allowed")  # noqa

                    if message.get("role", None) not in ("system", "user", "assistant"):
                        raise Exception(f"{prefix}Each message should have a valid role (system, user, or assistant)")  # noqa

        try:
            check_data_for_format_errors(items)
        except Exception as e:
            raise Exception(f"Fine-tuning data format is not valid. Got: {e}")
