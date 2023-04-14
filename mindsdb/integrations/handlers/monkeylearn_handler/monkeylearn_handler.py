from typing import Optional, Dict
import pandas as pd
import requests

from monkeylearn import MonkeyLearn

from mindsdb.integrations.libs.base import BaseMLEngine


class monkeylearnHandler(BaseMLEngine):
    name = "MonkeyLearn"

    @staticmethod
    def create_validations(self,args=None,**kwargs):
        if "using" in args:
            args = args["using"]

        if "api_key" not in args:
            raise Exception("API_KEY not found")

        if "model_id" in args:
            if "cl_" not in args["model_id"]:
                raise Exception("Classifier tasks are only supported currently")
        else:
            raise Exception("Enter the model_id of model you want use")

        # Check whether the model_id given by user exists in the user account or monkeylearn pre-trained models
        url = 'https://api.monkeylearn.com/v3/classifiers/'
        response = requests.get(url, headers={'Authorization': 'Token {}'.format(args["api_key"])})
        if response.status_code == 200:
            models = response.json()
            models_list = [model['id'] for model in models]
        else:
            raise Exception(f"Server response {response.status_code}")

        if args["model_id"] not in models_list:
            raise Exception(f"Model_id {args['model_id']} not found in MonkeyLearn pre-trained models")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        args = args['using']
        self.model_storage.json_set('args', args)

    def predict(self, df, args=None):
        args = self.model_storage.json_get('args')
        input_list = df[args['input_column']]
        ml = MonkeyLearn(args['API_KEY'])
        classifier_response = ml.classifiers.classify(args['MODEL_ID'], input_list)
        df_dict = []
        for res_dict in classifier_response.body:
            if res_dict.get("error") is True:
                raise Exception(res_dict["error_detail"])
            text = res_dict['text']
            pred_dict = res_dict['classifications'][0]  # Only add the one which model is more confident about
            pred_dict['text'] = text
            df_dict.append(pred_dict)
        pred_df = pd.DataFrame(df_dict)
        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        ml = MonkeyLearn(args['API_KEY'])
        response = ml.classifiers.detail(args['MODEL_ID'])
        description = {}
        description['name'] = response.body['name']
        description['model_version'] = response.body['model_version']
        description['date_created'] = response.body['created']
        # pre-trained monkeylearn models guide about what industries they can be used
        description['industries'] = response.body['industries']
        # Extract dict inside a dict
        models_dict = response.body['model']
        tag = models_dict['tags']
        tag_names = [name['name'] for name in tag]
        description['tags'] = tag_names
        des_df = pd.DataFrame([description])
        return des_df