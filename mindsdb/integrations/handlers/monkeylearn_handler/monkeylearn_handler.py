from typing import Optional, Dict
import pandas as pd
import requests

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.utilities.handler_utils import get_api_key


class monkeylearnHandler(BaseMLEngine):
    name = "monkeylearn"

    @staticmethod
    def create_validations(self, args=None, **kwargs):

        if "using" in args:
            args = args["using"]

        if "monkeylearn_api_key" not in args:
            raise Exception("monkeylearn_api_key not found")
        api_key = get_api_key('monkeylearn', args["using"], self.engine_storage, strict=False)
        if "model_id" in args:
            if "cl_" not in args["model_id"]:
                raise Exception("Classifier tasks are only supported currently")
        else:
            raise Exception("Enter the model_id of model you want use")
        model_id = args["model_id"]
        # Check whether the model_id given by user exists in the user account or monkeylearn pre-trained models
        url = 'https://api.monkeylearn.com/v3/classifiers/'
        response = requests.get(url, headers={'Authorization': 'Token {}'.format(api_key)})
        if response.status_code == 200:
            models = response.json()
            models_list = [model['id'] for model in models]
        else:
            raise Exception(f"Server response {response.status_code}")

        if model_id not in models_list:
            raise Exception(f"Model_id {args['model_id']} not found in MonkeyLearn pre-trained models")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if "using" in args:
            args = args['using']

        self.model_storage.json_set('args', args)

    def predict(self, df, args=None):
        args = self.model_storage.json_get('args')
        input_column = args['input_column']
        if input_column not in df.columns:
            raise RuntimeError(f"input columns {input_column} not found ")
        input_list = df[input_column]
        if len(input_list) > 500:
            raise Exception("Classifier only supports 500 data elements in list")
        ml = get_api_key('monkeylearn', args["using"], self.engine_storage, strict=False)
        df_list = []
        for text in input_list:
            pred_dict = {}
            classifier_response = ml.classifiers.classify(args['model_id'], [text])
            for res_dict in classifier_response.body:
                if res_dict.get("error") is True:
                    raise Exception(res_dict["error_detail"])
                pred_dict['classification'] = res_dict['classifications']
                pred_dict['tag'] = res_dict['classifications'][0]['tag_name']
                df_list.append(pd.DataFrame([pred_dict]))
        pred_df = pd.concat(df_list)
        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        ml = get_api_key('monkeylearn', args["using"], self.engine_storage, strict=False)
        response = ml.classifiers.detail(args['model_id'])
        description = {}
        description['name'] = response.body['name']
        description['model_version'] = response.body['model_version']
        description['date_created'] = response.body['created']
        # pre-trained monkeylearn models guide about what industries they can be used
        description['industries'] = response.body['industries']
        des_df = pd.DataFrame([description])
        return des_df
