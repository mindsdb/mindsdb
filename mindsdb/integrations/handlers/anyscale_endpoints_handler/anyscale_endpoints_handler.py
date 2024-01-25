import os
import openai
import json
import contextlib
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from mindsdb.integrations.handlers.openai_handler.constants import OPENAI_API_BASE
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.utilities import log

logger = log.getLogger(__name__)


ANYSCALE_API_BASE = 'https://api.endpoints.anyscale.com/v1'


class AnyscaleEndpointsHandler(OpenAIHandler):
    name = 'anyscale_endpoints'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_models = []
        self.chat_completion_models = []
        self.supported_ft_models = []
        self.default_model = 'meta-llama/Llama-2-7b-chat-hf'
        self.base_api = ANYSCALE_API_BASE
        self.default_mode = 'default'  # can also be 'conversational' or 'conversational-full'
        self.supported_modes = ['default', 'conversational', 'conversational-full']
        self.rate_limit = 25  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100

    @staticmethod
    @contextlib.contextmanager
    def _anyscale_base_api(args: dict, key='OPENAI_API_BASE'):
        """ Temporarily updates the API base env var to point towards the Anyscale URL. """
        old_base = os.environ.get(key, OPENAI_API_BASE)
        os.environ[key] = ANYSCALE_API_BASE
        oai_key = args['using'].get('openai_api_key', None)  # remove this arg override once #7496 is fixed

        try:
            if 'api_key' in args['using']:
                args['using']['openai_api_key'] = args['using']['api_key']
            yield  # enter
        finally:
            # exit
            os.environ[key] = old_base
            args['using']['openai_api_key'] = oai_key

    def create(self, target, args=None, **kwargs):
        with self._anyscale_base_api(args):
            # load base and fine-tuned models, then hand over
            self._set_models(args.get('using', {}))
            _args = self.model_storage.json_get('args')
            base_models = self.chat_completion_models
            self.chat_completion_models = _args.get('chat_completion_models', base_models) if _args else base_models
            super().create(target, args, **kwargs)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        with self._anyscale_base_api(args):
            # load base and fine-tuned models, then hand over
            self._set_models(args.get('using', {}))
            _args = self.model_storage.json_get('args')
            base_models = self.chat_completion_models
            self.chat_completion_models = _args.get('chat_completion_models', base_models) if _args else base_models
            return super().predict(df, args)

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        with self._anyscale_base_api(args):
            self._set_models(args.get('using', {}))
            super().finetune(df, args)
            # rewrite chat_completion_models to include the newly fine-tuned model
            args = self.model_storage.json_get('args')
            args['chat_completion_models'] = list(self.chat_completion_models) + [args['model_name']]
            self.model_storage.json_set('args', args)

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

    def _set_models(self, args):
        api_key = get_api_key('anyscale_endpoints', args, self.engine_storage, strict=False)
        if api_key is None:
            api_key = get_api_key('openai', args, self.engine_storage)
        client = self._get_client(api_key)
        self.all_models = [m.id for m in client.models.list()]
        self.chat_completion_models = [m.id for m in client.models.list() if m.rayllm_metadata['engine_config']['model_type'] == 'text-generation']  # noqa
        self.supported_ft_models = self.chat_completion_models  # base models compatible with fine-tuning

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
        def _is_valid(chat):
            """ Check if chat is valid according to Anyscale criteria."""
            roles = [m['role'] for m in chat]
            transitions = {None: ['system', 'user'], 'system': ['user'], 'user': ['assistant'], 'assistant': ['user']}

            # check base condition
            if not ('user' in roles and 'assistant' in roles):
                return False

            # check order is valid
            state = None
            for role in roles:
                if role not in transitions[state]:
                    return False
                else:
                    state = role

            # chat is valid, return
            return True

        # 1. aggregate each chat sequence into one row
        chats = []
        chat = []
        for i, row in df.iterrows():
            if row['role'] == 'system' and len(chat) > 0:
                if _is_valid(chat):
                    chats.append({'messages': chat})
                chat = []
            event = {'role': row['role'], 'content': row['content']}
            chat.append(event)

        if _is_valid(chat):
            chats.append({'messages': chat})

        series = pd.Series(chats)
        if len(series) < 20 * 2:
            raise Exception("Dataset is too small to finetune. Please include at least 40 samples (complete chats).")
        val_size = max(20, int(len(series) * test_size))  # at least 20 samples required by Anyscale
        train = series.iloc[:-val_size]
        val = series.iloc[-val_size:]

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

    def _get_ft_model_type(self, model_name: str):
        for base_model in self.chat_completion_models:
            if base_model.lower() in model_name.lower():
                return base_model
        logger.warning(f'Cannot recognize model {model_name}. Finetuning may fail.')
        return model_name.lower()

    @staticmethod
    def _add_extra_ft_params(ft_params, using_args):
        hyperparameters = {}
        # we populate separately because keys with `None` break the API
        for key in ('n_epochs', 'context_length'):
            if using_args.get(key, None):
                hyperparameters[key] = using_args[key]
        if hyperparameters:
            return {**ft_params, **{'hyperparameters': hyperparameters}}
        else:
            return ft_params

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

    @staticmethod
    def _get_client(api_key, base_url=ANYSCALE_API_BASE, org=None):
        return openai.OpenAI(api_key=api_key, base_url=base_url, organization=org)
