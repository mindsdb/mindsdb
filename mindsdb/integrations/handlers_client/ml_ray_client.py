import requests

import pandas as pd

def to_dataframe(data):
    df = pd.DataFrame(data['data'], columns=data['columns'])
    return df

def to_data(df):
    return df.to_dict('split', index=False)

class MLHandlerRayClient:
    def __init__(self, url, token):

        self._handler_info = None
        self._url = url
        self._token = token

    def _call(self, method_name, data):

        ret = requests.post(
            f'{self._url}/{method_name}',
            headers={'authorization': f'Bearer {self._token}'},
            json=data
        )
        return ret.json()

    def init_handler(self, class_path, company_id, integration_id, predictor_id):

        module_name, class_name = class_path
        self._handler_info = {
            'module_name': module_name,
            'class_name': class_name,
            'company_id': company_id,
            'integration_id': integration_id,
            'predictor_id': predictor_id
        }

    def close(self):
        self._handler_info = None

    def create(self, target, df, args):
        data = {
            'handler_info': self._handler_info,
            'target': target,
            'data': to_data(df),
            'args': args,
        }
        self._call('predict', data)

    def predict(self, df, args=None):
        data = {
            'handler_info': self._handler_info,
            'data': to_data(df),
            'args': args,
        }
        ret = self._call('predict', data)
        return to_dataframe(ret)



