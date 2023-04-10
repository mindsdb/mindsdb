from typing import Optional, Dict

import json
import dill
import pandas as pd
from type_infer.infer import infer_types
import requests

from mindsdb.integrations.libs.base import BaseMLEngine


class HuggingFaceInferenceHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_inference'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        pass

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