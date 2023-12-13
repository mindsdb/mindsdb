import ast
from typing import Dict, Optional, List

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.handlers.litellm_handler.settings import CompletionParameters

from litellm import completion

# these require no additional arguments

logger = log.getLogger(__name__)


# todo add support for prompt templates and corresponding kwargs for formatting

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
            raise Exception(
                "RAG engine requires a USING clause! Refer to its documentation for more details."
            )

    def create(
        self,
        target: str,
        df: pd.DataFrame = None,
        args: Optional[Dict] = None,
    ):
        """
        Dispatch is validating args and storing args in model_storage
        """
        # get api key from user input on create ML_ENGINE or create MODEL
        input_args = args["using"]

        # get api key from engine_storage
        ml_engine_args = self.engine_storage.get_connection_args()

        # for a model created with USING, only get api for that specific llm type
        input_args.update({k: v for k, v in ml_engine_args.items()})

        export_args = CompletionParameters(**input_args).dict()

        self.model_storage.json_set("args", export_args)

    def update(self, args) -> None:

        """
        Dispatch is updating args, validating args and storing args in model_storage
        """

        # get current model args
        current_model_args = self.model_storage.json_get("args")

        # update current args with new args
        current_model_args.update(args)

        # validate updated args are valid
        CompletionParameters(**current_model_args)

        # if valid, update model args
        self.model_storage.json_set("args", current_model_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is getting args from model_storage, validating args and running completion
        """

        input_args = self.model_storage.json_get("args")

        # validate args
        args = CompletionParameters(**input_args).dict()

        prompt_kwargs = df.iloc[0].to_dict()

        # if args['messages'] is empty, convert prompt to messages
        if not args['messages']:
            # if prompt_template is passed in, use that

            if len(prompt_kwargs) == 1:
                args['messages'] = self._prompt_to_messages(args['prompt_template'], **prompt_kwargs) \
                    if args['prompt_template'] else self._prompt_to_messages(df.iloc[0][0])

            elif len(prompt_kwargs) > 1:
                try:
                    args['messages'] = self._prompt_to_messages(args['prompt_template'], **prompt_kwargs)
                except KeyError as e:
                    raise Exception(
                        f"{e}: Please pass in either a prompt_template on create MODEL or "
                        f"a single where clause in predict query."
                        f""
                    )

        # if user passes in messages, use those instead
        elif 'messages' in prompt_kwargs:
            args['messages']: List = ast.literal_eval(df['messages'].iloc[0])

        else:
            raise Exception(
                "Please pass in either a prompt_template on create MODEL or a single input column on predict."
            )

        # remove prompt_template from args
        args.pop('prompt_template', None)

        # run completion
        response = completion(**args)

        return pd.DataFrame({"result": [response.choices[0].message.content]})

    @staticmethod
    def _prompt_to_messages(prompt: str, **kwargs) -> List[Dict]:
        """
        Convert a prompt to a list of messages
        """

        if kwargs:
            # if kwargs are passed in, format the prompt with kwargs
            prompt = prompt.format(**kwargs)

        return [{"content": prompt, "role": "user"}]
