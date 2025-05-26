from typing import Dict, Optional

import pandas as pd
from anthropic import Anthropic

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key

logger = log.getLogger(__name__)


class AnthropicHandler(BaseMLEngine):
    """
    Integration with the Anthropic LLM Python Library
    """

    name = "anthropic"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_chat_model = "claude-2.1"
        self.supported_chat_models = ["claude-instant-1.2", "claude-2.1", "claude-3-opus-20240229", "claude-3-sonnet-20240229"]
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
        connects with anthropic messages api to predict the answer for the particular question

        """

        args = self.model_storage.json_get("args")

        message = self.connection.messages.create(
            model=args["using"]["model"],
            max_tokens=args["using"]["max_tokens"],
            messages=[
                {"role": "user", "content": text}
            ]
        )

        content_blocks = message.content

        # assuming that message.content contains one ContentBlock item
        # returning text value if type==text and content_blocks value if type!=text
        if isinstance(content_blocks, list) and len(content_blocks) > 0:
            content_block = content_blocks[0]
            if content_block.type == 'text':
                return content_block.text
            else:
                return content_blocks
        else:
            raise Exception(
                f"Invalid output: {content_blocks}"
            )
