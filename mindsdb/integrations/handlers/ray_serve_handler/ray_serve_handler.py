import requests
from typing import Dict, Optional
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine


class RayServeHandler(BaseMLEngine):
    """
    The Ray Serve integration engine needs to have a working connection to Ray Serve.
    """
    name = 'ray_serve'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if not args.get('using'):
            raise Exception("Error: This engine requires some parameters via the 'using' clause.")
        if not args['using'].get('train_url'):
            raise Exception("Error: Please provide a URL for the training endpoint.")
        if not args['using'].get('predict_url'):
            raise Exception("Error: Please provide a URL for the prediction endpoint.")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = copy.copy(args['using'])
        train_url = args.pop('train_url', None)
        predict_url = args.pop('predict_url', None)

        if not train_url or not predict_url:
            raise Exception("Error: Both 'train_url' and 'predict_url' must be provided.")

        self.model_storage.json_set('args', {
            'train_url': train_url,
            'predict_url': predict_url,
            **args
        })

        payload = {
            'df': df.to_json(orient='records'),
            'target': target,
            'args': args
        }

        try:
            resp = requests.post(
                train_url,
                json=payload,
                headers={'content-type': 'application/json; format=pandas-records'}
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error: Failed to communicate with the training endpoint. Details: {str(e)}")

        response = resp.json()
        if response.get('status') != 'ok':
            raise Exception(f"Error: Training failed. Details: {response.get('status', 'Unknown error')}")

    def predict(self, df, args=None):
        args = self.model_storage.json_get('args')
        resp = requests.post(
            args['predict_url'],
            json={'df': df.to_json(orient='records')},
            headers={'content-type': 'application/json; format=pandas-records'}
        )
        response = resp.json()

        target = args['target']
        if target != 'prediction':
            response[target] = response.pop('prediction')

        predictions = pd.DataFrame(response)
        return predictions

    def describe(self, key: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        description = {
            'TRAIN_URL': [args['train_url']],
            'PREDICT_URL': [args['predict_url']],
            'TARGET': [args['target']],
        }
        return pd.DataFrame.from_dict(description)
