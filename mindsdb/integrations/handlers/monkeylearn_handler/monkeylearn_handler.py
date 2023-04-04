from typing import Optional, Dict
import pandas as pd
import transformers
import requests

from monkeylearn import MonkeyLearn

from mindsdb.integrations.libs.base import BaseMLEngine


class monkeylearnHandler(BaseMLEngine):
    name = "MonkeyLearn"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = args['using']
        model_id = args['MODEL_ID']

        # if 'production_model' in args:
        #     raise Exception("Custom models are not supported currently")

        url = 'https://api.monkeylearn.com/v3/classifiers/'
        response = requests.get(url, headers={'Authorization': 'Token {}'.format(args['YOUR_API_KEY'])})
        if response.status_code == 200:
            models = response.json()
            models_list = [model['id'] for model in models]
        else:
            raise Exception(f"Server response {response.status_code}")

        if model_id not in models_list:
            raise Exception(f"Model_id not found {model_id} in MonkeyLearn pre-trained models")

        self.model_storage.json_set('args',args)
        self.engine_storage.folder_sync(model_id)

        def predict(self, df, args= None):
            args = self.model_storage.json_get('args')
            model_id = self.engine_storage.folder_get(args['model_id'], update=False)
            input_list = df[args['input_column']]


















