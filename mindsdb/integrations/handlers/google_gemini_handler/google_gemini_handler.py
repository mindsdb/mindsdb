import os
from typing import Dict, Optional

import google.generativeai as genai
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)


class GoogleGeminiHandler(BaseMLEngine):
    """
    Integration with the Google generative AI Python Library
    """

    name = "google_gemini"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_chat_model = "gemini-pro"
        self.supported_chat_models = ["gemini-pro"]
        self.generative = True
        self.connection = None

    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:
        if "model" not in args["using"]:
            args["using"]["model"] = self.default_chat_model
        elif args["using"]["model"] not in self.supported_chat_models:
            raise Exception(
                f"Invalid chat model. Please use one of {self.supported_chat_models}"
            )

        api_key = self._get_google_gemini_api_key(args)

        try:
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(args["using"]["model"])
            model.generate_content("test")
        except Exception as e:
            raise Exception(
                f"{e}: Invalid api key please check your api key"
            )

        args["using"]["api_key"] = api_key

        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        args = self.model_storage.json_get("args")
        api_key = args["using"]["api_key"]
        genai.configure(api_key=api_key)

        input_column = args["using"]["column"]
        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        self.connection = genai.GenerativeModel(args["using"]["model"])
        result_df = pd.DataFrame()
        result_df["predictions"] = df[input_column].apply(self.predict_answer)
        result_df = result_df.rename(columns={"predictions": args["target"]})
        return result_df

    def _get_google_gemini_api_key(self, args, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. GOOGLE_GENAI_API_KEY env variable
            4. google_gemini.api_key setting in config.json
        """

        if "api_key" in args["using"]:
            return args["using"]["api_key"]
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if "api_key" in connection_args:
            return connection_args["api_key"]
        # 3
        api_key = os.getenv("GOOGLE_GENAI_API_KEY")
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        google_gemini_config = config.get("google_gemini", {})
        if "api_key" in google_gemini_config:
            return google_gemini_config["api_key"]

        if strict:
            raise Exception(
                'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )

    def predict_answer(self, text):
        """
        connects with google generative AI api to predict the answer for the particular question

        """

        completion = self.connection.generate_content(
            text
        )

        return completion.text
