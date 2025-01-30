import json
import requests
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine


class RayServeHandler(BaseMLEngine):
    """
    The Ray Serve integration engine needs to have a working connection to Ray Serve. For this:
        - A Ray Serve server should be running

    Example:

    """  # noqa
    name = 'ray_serve'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if not args.get('using'):
            raise Exception("Error: This engine requires some parameters via the 'using' clause. Please refer to the documentation of the Ray Serve handler and try again.")  # noqa
        if not args['using'].get('train_url'):
            raise Exception("Error: Please provide a URL for the training endpoint.")
        if not args['using'].get('predict_url'):
            raise Exception("Error: Please provide a URL for the prediction endpoint.")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        # TODO: use join_learn_process to notify users when ray has finished the training process
        args = args['using']  # ignore the rest of the problem definition
        args['target'] = target
        self.model_storage.json_set('args', args)
        try:
            resp = requests.post(args['train_url'],
                                 json={'df': df.to_json(orient='records'), 'target': target},
                                 headers={'content-type': 'application/json; format=pandas-records'})
        except requests.exceptions.InvalidSchema:
            raise Exception("Error: The URL provided for the training endpoint is invalid.")

        resp = resp.json()
        if not isinstance(resp.get('response'), list):
            raise Exception(f"Unexpected response structure: {response}")
        response_content = json.loads(resp['response'][0])  # Parse the string inside the list
        if response_content['status'] != 'ok':
            raise Exception("Error: Training failed: " + response_content['detail'])

    def predict(self, df, args=None):
        args = {**(self.model_storage.json_get('args')), **args}  # merge incoming args
        pred_args = args.get('predict_params', {})
        args = {**args, **pred_args}  # merge pred_args
        resp = requests.post(args['predict_url'],
                             json={'df': df.to_json(orient='records'), 'pred_args': pred_args},
                             headers={'content-type': 'application/json; format=pandas-records'})
        response = resp.json()
        if not isinstance(response.get('response'), list):
            raise Exception(f"Unexpected response structure: {response}")
        response_content = json.loads(response['response'][0])  # Parse the string inside the list
        if response_content['status'] != 'ok':
            raise Exception("Error: Prediction failed: " + response_content['detail'])
        response_content.pop('status')
        target = args['target']
        if target != 'prediction':
            # rename prediction to target
            response_content[target] = response_content.pop('prediction')

        predictions = pd.DataFrame(response_content)
        return predictions

    def describe(self, key: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        description = {
            'TRAIN_URL': [args['train_url']],
            'PREDICT_URL': [args['predict_url']],
            'TARGET': [args['target']],
        }
        return pd.DataFrame.from_dict(description)
