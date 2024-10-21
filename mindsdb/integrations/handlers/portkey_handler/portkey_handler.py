from typing import Dict, Optional

import pandas as pd
from portkey_ai import Portkey

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key

logger = log.getLogger(__name__)

DEFAULT_METADATA = {
    "_source": "portkey-mindsdb-integration",
}


class PortkeyHandler(BaseMLEngine):
    """
    Integration with the Portkey LLM Python Library
    """

    name = "portkey"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:

        if "using" not in args:
            raise Exception(
                "Portkey engine requires a USING clause! Refer to its documentation for more details."
            )

        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:

        args = self.model_storage.json_get("args")
        api_key = get_api_key('portkey', args["using"], self.engine_storage, strict=False)

        self.client = Portkey(
            **self.engine_storage.get_connection_args(),
            api_key=api_key,
            metadata=DEFAULT_METADATA
        )

        result_df = pd.DataFrame()

        result_df["predictions"] = df["question"].apply(self._predict_answer)

        result_df = result_df.rename(columns={"predictions": args["target"]})

        return result_df

    def _predict_answer(self, text):
        """
        connects with portkey messages api to predict the answer for the particular question

        """

        model_args = self.model_storage.json_get("args")

        message = self.client.chat.completions.create(
            **model_args,
            messages=[
                {"role": "user", "content": text}
            ]
        )

        return message.choices[0].message.content
