from typing import Optional, Dict

import json
import pandas as pd
import requests

from config_parser import ConfigParser

from mindsdb.integrations.libs.base import BaseMLEngine


class HuggingFaceInferenceHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_inference'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details.")

        config = ConfigParser('config.yaml')
        config_args = config.get_config_dict()

        self.model_storage.json_set('args', args)
        self.model_storage.json_set('config_args', config_args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        pass

    def _query(self, api_url, api_token, inputs, parameters=None, options=None):
        headers = {
            "Authorization": f"Bearer {api_token}"
        }

        data = json.dumps(
            {
                "inputs": inputs,
                "parameters": parameters,
                "options": options
            }
        )

        response = requests.request("POST", api_url, headers=headers, data=data)
        return json.loads(response.content.decode("utf-8"))