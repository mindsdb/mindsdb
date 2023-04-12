import os.path
from typing import Optional, Dict

import json
import pandas as pd
import requests

from .config_parser import ConfigParser

from mindsdb.integrations.libs.base import BaseMLEngine


class HuggingFaceInferenceAPIHandler(BaseMLEngine):
    """
    Integration with the Hugging Face Inference API.
    """

    name = 'huggingface_inference_api'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Hugging Face Inference engine requires a USING clause! Refer to its documentation for more details.")

        config = ConfigParser(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.yaml'))
        config_args = config.get_config_dict()

        self.model_storage.json_set('args', args)
        self.model_storage.json_set('config_args', config_args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = self.model_storage.json_get('args')
        config_args = self.model_storage.json_get('config_args')

        inputs = self._parse_inputs(df, args['using']['inputs'], args['using']['task'])

        response = self._query(
            f"{config_args['BASE_URL']}/{config_args['TASK_MODEL_MAP'][args['using']['task']]}",
            args['using']['api_key'],
            inputs,
            args['using']['parameters'] if 'parameters' in args['using'] else None,
            args['using']['options'] if 'options' in args['using'] else None
        )

        return self._parse_response(df, response, args['using']['task'], args['target'])

    def _query(self, api_url, api_token, inputs, parameters=None, options=None):
        headers = {
            "Authorization": f"Bearer {api_token}"
        }

        data = {
            "inputs": inputs
        }

        if parameters is not None:
            data['parameters'] = parameters

        if options is not None:
            data['options'] = options

        response = requests.request("POST", api_url, headers=headers, data=json.dumps(data))
        return json.loads(response.content.decode("utf-8"))

    def _parse_inputs(self, df, inputs, task):
        if task == 'text-classification':
            return df[inputs['column']].tolist()

    def _parse_response(self, df, response, task, target):
        if task == 'text-classification':
            df[target] = [item[0]['label'] for item in response]

        return df