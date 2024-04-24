from typing import Dict, Optional

import pandas as pd
import fireworks.client

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.handlers.fireworks_handler import config

logger = log.getLogger(__name__)


class FireworksHandler(BaseMLEngine):
    """
    Integration with the Fireworks AI LLM Python Library
    """

    name = "fireworks"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_supported_task = "QueryTextModel"
        self.supported_task = [
            "QueryTextModel",
            "QueryVisionModel"
        ]
        self.default_max_tokens = 100

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        task = args["using"].get("task", "QueryTextModel")

        if task not in config.supported_task:
            raise Exception(
                f"Invalid task argument. Please use one of {config.supported_task.keys()}"
            )
        config_dict = config.supported_task[task]

        missing_keys = [key for key in config_dict if key not in args["using"]]
        if missing_keys:
            raise Exception(f"{task} requires {missing_keys} arguments")


    def create(
        self,
        target: str,
        df: Optional[pd.DataFrame] = None,
        args: Optional[Dict] = None,
    ) -> None:

        if "using" not in args:
            raise Exception(
                "fireworks_ai engine requires a USING clause! Refer to its documentation for more details."
            )

        if "task" not in args["using"]:
            args["using"]["task"] = self.default_supported_task
        elif args["using"]["task"] not in self.supported_task:
            raise Exception(
                f"Invalid operation mode. Please use one of {self.supported_task}"
            )
        
        if "max_tokens" not in args["using"]:
            args["using"]["max_tokens"] = self.default_max_tokens
        

        self.model_storage.json_set("args", args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:

        args = self.model_storage.json_get("args")
        self.create_validation(None, args)
        api_key = get_api_key('fireworks', args["using"], self.engine_storage, strict=False)

        self.connection = fireworks.client
        self.connection.api_key = api_key


        input_column = args["using"]["column"]

        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        result_df = pd.DataFrame()

        if args["using"]["task"] == "QueryTextModel":

            result_df["predictions"] = df[input_column].apply(self.predict_text_answer)

        elif args["using"]["task"] == "QueryVisionModel":

            result_df["predictions"] = df[input_column].apply(self.predict_image_answer)

        result_df = result_df.rename(columns={"predictions": args["target"]})

        return result_df

    def predict_text_answer(self, text):
        """
        connects with fireworks api to predict the answer for the particular question

        """

        args = self.model_storage.json_get("args")


        response = self.connection.ChatCompletion.create(
            model=f"accounts/fireworks/models/{args['using']['model']}",
            max_tokens=args["using"]["max_tokens"],
            messages=[
                {"role": "user", "content": text}
            ]
        )

        return response.choices[0].message.content
        
    
    def predict_image_answer(self, image_url):
        """
        connects with fireworks api to predict the image description for the particular image url

        """

        args = self.model_storage.json_get("args")


        response = self.connection.ChatCompletion.create(
            model=f"accounts/fireworks/models/{args['using']['model']}",
            messages = [{
                "role": "user",
                "content": [{
                "type": "text",
                "text": "Can you describe this image?",
                }, {
                "type": "image_url",
                "image_url": {
                    "url": image_url
                },
                }, ],
            }],
        )

        return response.choices[0].message.content


