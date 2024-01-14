import ast
import pandas as pd
from typing import Optional, Dict, List

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.integrations.handlers.litellm_handler.settings import CompletionParameters

from litellm import completion, batch_completion

logger = log.getLogger(__name__)

class LiteLLMHandler(BaseMLEngine):
    """
    LiteLLMHandler is a MindsDB handler for litellm - https://docs.litellm.ai/docs/
    """

    name = "litellm"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception("Litellm engine requires a USING clause. See settings.py for more info on supported args.")

    def create(self, target: str, df: pd.DataFrame = None, args: Optional[Dict] = None):
        """
        Dispatch is validating args and storing args in model_storage
        """
        input_args = args["using"] if "using" in args else {}
        ml_engine_args = self.engine_storage.get_connection_args()
        input_args.update({k: v for k, v in ml_engine_args.items()})
        export_args = CompletionParameters(**input_args).dict()
        self.model_storage.json_set("args", export_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is getting args from model_storage, validating args and running completion
        """
        input_args = self.model_storage.json_get("args")
        args = CompletionParameters(**input_args).dict()

        self._build_messages(args, df)

        args.pop('prompt_template', None)

        if len(args['messages']) > 1:
            responses = batch_completion(**args)
            return pd.DataFrame({"result": [response.choices[0].message.content for response in responses]})

        response = completion(**args)
        return pd.DataFrame({"result": [response.choices[0].message.content]})

    def _build_messages(self, args: dict, df: pd.DataFrame):
        prompt_kwargs = df.iloc[0].to_dict()
        self._process_prompt_template(args, prompt_kwargs)
        self._process_mock_response(args, prompt_kwargs)
        self._validate_messages_arg(prompt_kwargs)
        self._process_messages_arg(args, df, prompt_kwargs)

    def _process_prompt_template(self, args: dict, prompt_kwargs: dict):
        if "prompt_template" in prompt_kwargs:
            logger.info("Using 'prompt_template' passed in SELECT Predict query. "
                        "Note this will overwrite a 'prompt_template' passed in create MODEL query.")
            args['prompt_template'] = prompt_kwargs.pop('prompt_template')

    def _process_mock_response(self, args: dict, prompt_kwargs: dict):
        if 'mock_response' in prompt_kwargs:
            args['mock_response'] = prompt_kwargs.pop('mock_response')

    def _validate_messages_arg(self, prompt_kwargs: dict):
        if 'messages' in prompt_kwargs and len(prompt_kwargs) > 1:
            raise Exception("If 'messages' is passed in SELECT Predict query, no other args can be passed in.")
        
    def _process_messages_arg(self, args: dict, df: pd.DataFrame, prompt_kwargs: dict):
        if 'messages' in prompt_kwargs:
            logger.info("Using messages passed in SELECT Predict query. 'prompt_template' will be ignored.")
            args['messages'] = ast.literal_eval(df['messages'].iloc[0])
        else:
            self._process_prompt_template(args, prompt_kwargs)
            if len(prompt_kwargs) <= 2:
                args['messages'] = self._prompt_to_messages(args.get('prompt_template', ''), **prompt_kwargs) \
                    if args.get('prompt_template') else self._prompt_to_messages(df.iloc[0][0])
            else:
                args['messages'] = self._prompt_to_messages(args['prompt_template'], **prompt_kwargs)

    def _prompt_to_messages(self, prompt: str, **kwargs) -> List[Dict]:
        if kwargs:
            prompt = prompt.format(**kwargs)
        return [{"content": prompt, "role": "user"}]

    def _process_prompt_template(self, args: dict, prompt_kwargs: dict):
        if "prompt_template" in prompt_kwargs:
            logger.info("Using 'prompt_template' passed in SELECT Predict query. "
                        "Note this will overwrite a 'prompt_template' passed in create MODEL query.")
            args['prompt_template'] = prompt_kwargs.pop('prompt_template')

    def _process_mock_response(self, args: dict, prompt_kwargs: dict):
        if 'mock_response' in prompt_kwargs:
            args['mock_response'] = prompt_kwargs.pop('mock_response')

    def _validate_messages_arg(self, prompt_kwargs: dict):
        if 'messages' in prompt_kwargs and len(prompt_kwargs) > 1:
            raise Exception("If 'messages' is passed in SELECT Predict query, no other args can be passed in.")
    # END: ed8c6549bwf9

    def _prompt_to_messages(self, prompt: str, **kwargs) -> List[Dict]:
        if kwargs:
            prompt = prompt.format(**kwargs)
        return [{"content": prompt, "role": "user"}]

    def _process_prompt_template(self, args: dict, prompt_kwargs: dict):
        if "prompt_template" in prompt_kwargs:
            logger.info("Using 'prompt_template' passed in SELECT Predict query. "
                        "Note this will overwrite a 'prompt_template' passed in create MODEL query.")
            args['prompt_template'] = prompt_kwargs.pop('prompt_template')

    def _process_mock_response(self, args: dict, prompt_kwargs: dict):
        if 'mock_response' in prompt_kwargs:
            args['mock_response'] = prompt_kwargs.pop('mock_response')

    def _validate_messages_arg(self, prompt_kwargs: dict):
        if 'messages' in prompt_kwargs and len(prompt_kwargs) > 1:
            raise Exception("If 'messages' is passed in SELECT Predict query, no other args can be passed in.")

    def _prompt_to_messages(self, prompt: str, **kwargs) -> List[Dict]:
        if kwargs:
            prompt = prompt.format(**kwargs)
        return [{"content": prompt, "role": "user"}]

    def _process_prompt_template(self, args: dict, prompt_kwargs: dict):
        if "prompt_template" in prompt_kwargs:
            logger.info("Using 'prompt_template' passed in SELECT Predict query. "
                        "Note this will overwrite a 'prompt_template' passed in create MODEL query.")
            args['prompt_template'] = prompt_kwargs.pop('prompt_template')

    def _process_mock_response(self, args: dict, prompt_kwargs: dict):
        if 'mock_response' in prompt_kwargs:
            args['mock_response'] = prompt_kwargs.pop('mock_response')

    def _validate_messages_arg(self, prompt_kwargs: dict):
        if 'messages' in prompt_kwargs and len(prompt_kwargs) > 1:
            raise Exception("If 'messages' is passed in SELECT Predict query, no other args can be passed in.")

    def _prompt_to_messages(self, prompt: str, **kwargs) -> List[Dict]:
        if kwargs:
            prompt = prompt.format(**kwargs)
        return [{"content": prompt, "role": "user"}]
