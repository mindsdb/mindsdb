import os
from typing import Dict, Optional

import pandas as pd
from anthropic import AI_PROMPT, HUMAN_PROMPT, Anthropic

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

from mindsdb.integrations.utilities.handler_utils import get_api_key

logger = log.getLogger(__name__)


class AnthropicHandler(BaseMLEngine):
    """
    Integration with the Anthropic LLM Python Library
    """

    name = "anthropic"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_chat_model = "claude-2"
        self.supported_chat_models = ["claude-1", "claude-2"]
        self.default_max_tokens = 100
        self.generative = True
        self.connection = None

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:

        if "using" not in args:
            raise Exception(
                "Anthropic engine requires a USING clause! Refer to its documentation for more details."
            )

        if "model" not in args["using"]:
            args["using"]["model"] = self.default_chat_model
        elif args["using"]["model"] not in self.supported_chat_models:
            raise Exception(
                f"Invalid chat model. Please use one of {self.supported_chat_models}"
            )

        if "max_tokens" not in args["using"]:
            args["using"]["max_tokens"] = self.default_max_tokens

        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:

        args = self.model_storage.json_get("args")
        api_key = get_api_key('anthropic', args["using"], self.engine_storage, strict=False)

        self.connection = Anthropic(
            api_key=api_key,
        )

        input_column = args["using"]["column"]

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        result_df = pd.DataFrame()

        result_df["predictions"] = df[input_column].apply(self.predict_answer)

        result_df = result_df.rename(columns={"predictions": args["target"]})

        return result_df

    def predict_answer(self, text):
        """
        connects with anthropic api to predict the answer for the particular question

        """

        args = self.model_storage.json_get("args")

        completion = self.connection.completions.create(
            model=args["using"]["model"],
            max_tokens_to_sample=args["using"]["max_tokens"],
            prompt=f"{HUMAN_PROMPT} {text} {AI_PROMPT}",
        )

        return completion.completion
