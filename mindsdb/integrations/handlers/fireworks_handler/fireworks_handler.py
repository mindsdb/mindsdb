from typing import Dict, Optional


import pandas as pd
import fireworks.client
import requests

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.handlers.fireworks_handler.config import FireworksHandlerArgs

logger = log.getLogger(__name__)


class FireworksHandler(BaseMLEngine):
    """
    Integration with the Fireworks AI LLM Python Library
    """

    name = "fireworks"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_supported_mode = "conversational"
        self.supported_mode = ["conversational", "image", "embedding"]
        self.default_max_tokens = 100

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "fireworks_ai engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args["using"]

    def create(self, target, args=None, **kwargs):

        if "using" not in args:
            raise Exception(
                "fireworks_ai engine requires a USING clause! Refer to its documentation for more details."
            )

        args = args["using"]

        args_model = FireworksHandlerArgs(**args)
        args_model.target = target
        api_key = get_api_key("fireworks", args, self.engine_storage, strict=False)
        logger.error(f"args_model: {args_model}!")
        logger.error(f"api_key: {api_key}!")

        if not args_model.mode:
            args_model.mode = self.default_supported_mode
        elif args_model.mode not in self.supported_mode:
            raise Exception(
                f"Invalid operation mode. Please use one of {self.supported_mode}"
            )

        if not args_model.max_tokens:
            args_model.max_tokens = self.default_max_tokens

        logger.error(f"args_model: {args_model}!")

        self.model_storage.json_set("args", args_model.model_dump())

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:

        args_model = FireworksHandlerArgs(**self.model_storage.json_get("args"))

        api_key = get_api_key(
            "fireworks", args["using"], self.engine_storage, strict=False
        )

        self.connection = fireworks.client
        self.connection.api_key = api_key

        input_column = args_model.column
        target_column = args_model.target

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        result_df = pd.DataFrame()

        if args_model.mode == "conversational":

            result_df["predictions"] = df[input_column].apply(self.predict_text_answer)

        elif args_model.mode == "image":

            result_df["predictions"] = df[input_column].apply(self.predict_image_answer)

        elif args_model.mode == "embedding":

            result_df["predictions"] = df[input_column].apply(self.generate_embeddings)

        result_df = result_df.rename(columns={"predictions": target_column})

        return result_df

    def predict_text_answer(self, text):
        """
        connects with fireworks api to predict the answer for the particular question

        """

        args_model = FireworksHandlerArgs(**self.model_storage.json_get("args"))
        try:
            response = self.connection.ChatCompletion.create(
                model=f"accounts/fireworks/models/{args_model.model}",
                max_tokens=args_model.max_tokens,
                messages=[{"role": "user", "content": text}],
            )

            return response.choices[0].message.content
        except Exception as e:
            return e

    def predict_image_answer(self, image_url):
        """
        connects with fireworks api to predict the image description for the particular image url

        """

        args_model = FireworksHandlerArgs(**self.model_storage.json_get("args"))

        try:
            response = self.connection.ChatCompletion.create(
                model=f"accounts/fireworks/models/{args_model.model}",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Can you describe this image?",
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": image_url},
                            },
                        ],
                    }
                ],
            )

            return response.choices[0].message.content
        except Exception as e:
            return e

    def generate_embeddings(self, text):
        """
        connects with fireworks api to generate the text embeddings for the given text

        """

        args_model = FireworksHandlerArgs(**self.model_storage.json_get("args"))
        url = "https://api.fireworks.ai/inference/v1/embeddings"
        payload = {"input": text, "model": args_model.model}
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {self.connection.api_key}",
        }

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()["data"][0]["embedding"]

        except Exception as e:
            return e
