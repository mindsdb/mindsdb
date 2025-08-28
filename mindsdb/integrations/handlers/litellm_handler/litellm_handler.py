import ast
from typing import Dict, Optional, List


from litellm import completion, batch_completion, embedding, acompletion, supports_response_schema

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.handlers.litellm_handler.settings import CompletionParameters


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

    @classmethod
    def prepare_arguments(cls, provider, model_name, args):
        if provider == "snowflake" and "snowflake_account_id" in args:
            args["api_base"] = (
                f"https://{args['snowflake_account_id']}.snowflakecomputing.com/api/v2/cortex/inference:complete"
            )
        if provider == "google":
            provider = "gemini"
        if "base_url" in args:
            args["api_base"] = args.pop("base_url")

        model_name = f"{provider}/{model_name}"
        return model_name, args

    @classmethod
    def embeddings(cls, provider: str, model: str, messages: List[str], args: dict) -> List[list]:
        model, args = cls.prepare_arguments(provider, model, args)
        response = embedding(model=model, input=messages, **args)
        return [rec["embedding"] for rec in response.data]

    @classmethod
    async def acompletion(cls, provider: str, model: str, messages: List[dict], args: dict):
        model, args = cls.prepare_arguments(provider, model, args)
        return await acompletion(model=model, messages=messages, stream=False, **args)

    @classmethod
    def completion(cls, provider: str, model: str, messages: List[dict], args: dict):
        model, args = cls.prepare_arguments(provider, model, args)
        json_output = args.pop("json_output", False)

        supports_json_output = supports_response_schema(model=model, custom_llm_provider=provider)

        if json_output and supports_json_output:
            args["response_format"] = {"type": "json_object"}
        else:
            args["response_format"] = None

        return completion(model=model, messages=messages, stream=False, **args)

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

        # check engine_storage for api_key
        input_args.update({k: v for k, v in ml_engine_args.items()})
        input_args["target"] = target

        # validate args
        export_args = CompletionParameters(**input_args).model_dump()

        # store args
        self.model_storage.json_set("args", export_args)

    def predict(self, df: pd.DataFrame = None, args: dict = None):
        """
        Dispatch is getting args from model_storage, validating args and running completion
        """

        input_args = self.model_storage.json_get("args")

        # validate args
        args = CompletionParameters(**input_args).model_dump()

        target = args.pop("target")

        # build messages
        self._build_messages(args, df)

        # remove prompt_template from args
        args.pop("prompt_template", None)

        if len(args["messages"]) > 1:
            # if more than one message, use batch completion
            responses = batch_completion(**args)
            return pd.DataFrame({target: [response.choices[0].message.content for response in responses]})

        # run completion
        response = completion(**args)

        return pd.DataFrame({target: [response.choices[0].message.content]})

    @staticmethod
    def _prompt_to_messages(prompt: str, **kwargs) -> List[Dict]:
        """
        Convert a prompt to a list of messages
        """

        if kwargs:
            # if kwargs are passed in, format the prompt with kwargs
            prompt = prompt.format(**kwargs)

        return [{"content": prompt, "role": "user"}]

    def _build_messages(self, args: dict, df: pd.DataFrame):
        """
        Build messages for completion
        """

        prompt_kwargs = df.iloc[0].to_dict()

        if "prompt_template" in prompt_kwargs:
            # if prompt_template is passed in predict query, use it
            logger.info(
                "Using 'prompt_template' passed in SELECT Predict query. "
                "Note this will overwrite a 'prompt_template' passed in create MODEL query."
            )

            args["prompt_template"] = prompt_kwargs.pop("prompt_template")

        if "mock_response" in prompt_kwargs:
            # used for testing to save on real completion api calls
            args["mock_response"]: str = prompt_kwargs.pop("mock_response")

        if "messages" in prompt_kwargs and len(prompt_kwargs) > 1:
            # if user passes in messages, no other args can be passed in
            raise Exception("If 'messages' is passed in SELECT Predict query, no other args can be passed in.")

        # if user passes in messages, use those instead
        if "messages" in prompt_kwargs:
            logger.info("Using messages passed in SELECT Predict query. 'prompt_template' will be ignored.")

            args["messages"]: List = ast.literal_eval(df["messages"].iloc[0])

        else:
            # if user passes in prompt_template, use that to create messages
            if len(prompt_kwargs) == 1:
                args["messages"] = (
                    self._prompt_to_messages(args["prompt_template"], **prompt_kwargs)
                    if args["prompt_template"]
                    else self._prompt_to_messages(df.iloc[0][0])
                )

            elif len(prompt_kwargs) > 1:
                try:
                    args["messages"] = self._prompt_to_messages(args["prompt_template"], **prompt_kwargs)
                except KeyError as e:
                    raise Exception(
                        f"{e}: Please pass in either a prompt_template on create MODEL or "
                        f"a single where clause in predict query."
                        f""
                    )
