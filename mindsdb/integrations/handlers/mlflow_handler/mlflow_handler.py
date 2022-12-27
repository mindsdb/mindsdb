import requests
from datetime import datetime
from typing import Dict, Optional

import pandas as pd
from mlflow.tracking import MlflowClient

from mindsdb.integrations.libs.base import BaseMLEngine


class MLflowHandler(BaseMLEngine):
    """
    The MLflow integration engine needs to have a working connection to MLFlow. For this:
        - All models to use should be previously served
        - An MLflow server should be running, to access its model registry

    Example:
        1. Run `mlflow server -p 5001 --backend-store-uri sqlite:////path/to/mlflow.db --default-artifact-root ./artifacts --host 0.0.0.0`
        2. Run `mlflow models serve --model-uri ./model_path`
        3. Run MindsDB
    
    Note: above, `artifacts` is a folder to store artifacts for new experiments that do not specify an artifact store.
    """  # noqa

    name = 'mlflow'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = args['using']  # ignore the rest of the problem definition
        connection = MlflowClient(args['mlflow_server_url'], args['mlflow_server_path'])
        model_name = args['model_name']
        mlflow_models = [model.name for model in connection.search_registered_models()]

        if model_name not in mlflow_models:
            raise Exception(f"Error: model '{model_name}' not found in mlflow. Check serving and try again.")

        args['target'] = target
        self._check_model_url(args['predict_url'])
        self.model_storage.json_set('args', args)

    def predict(self, df, args=None):
        args = self.model_storage.json_get('args')  # override any incoming args for now
        self._check_model_url(args['predict_url'])
        resp = requests.post(args['predict_url'],
                             data=df.to_json(orient='records'),
                             headers={'content-type': 'application/json; format=pandas-records'})
        answer = resp.json()
        predictions = pd.DataFrame({args['target']: answer})
        return predictions

    def describe(self, key: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        connection = MlflowClient(args['mlflow_server_url'], args['self.mlflow_server_path'])
        models = {model.name: model for model in connection.search_registered_models()}
        model = models[key]
        latest_version = model.latest_versions[-1]
        description = {
            'NAME': [model.name],
            'USER_DESCRIPTION': [model.description],
            'LAST_STATUS': [latest_version.status],
            'CREATED_AT': [datetime.fromtimestamp(model.creation_timestamp//1000).strftime("%m/%d/%Y, %H:%M:%S")],
            'LAST_UPDATED': [datetime.fromtimestamp(model.last_updated_timestamp//1000).strftime("%m/%d/%Y, %H:%M:%S")],
            'TAGS': [model.tags],
            'LAST_RUN_ID': [latest_version.run_id],
            'LAST_SOURCE_PATH': [latest_version.source],
            'LAST_USER_ID': [latest_version.user_id],
            'LAST_VERSION': [latest_version.version],
        }
        return pd.DataFrame.from_dict(description)

    @staticmethod
    def _check_model_url(url):
        """ try post without data, check status code not in (not_found, method_not_allowed) """
        try:
            resp = requests.post(url)
            if resp.status_code in (404, 405):
                raise Exception(f'Model url is incorrect, status_code: {resp.status_code}')
        except requests.RequestException as e:
            raise Exception(f'Model url is incorrect: {str(e)}')
