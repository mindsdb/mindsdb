import requests
import json
import re
import datetime as dt

import pandas as pd

from mindsdb.utilities.json_encoder import CustomJSONEncoder

class Serializer:
    JSONEncoder = CustomJSONEncoder

    @staticmethod
    def dict_to_df(data):
        df = pd.DataFrame(data['data'], columns=data['columns'])
        return df

    @staticmethod
    def df_to_dict(df):
        data = df.to_dict('split')
        del data['index']
        return data

    @staticmethod
    def json_encode(data):
        return Serializer.JSONEncoder().encode(data)

    @staticmethod
    def json_decode(body):
        def datetime_hook(dct):
            DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
            RX_DATETIME = re.compile(r'[\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}.[\d]{1,6}')

            DATE_FORMAT = "%Y-%m-%d"
            RX_DATE = re.compile(r'[\d]{4}-[\d]{2}-[\d]{2}')

            for k, v in dct.items():
                if isinstance(v, str):
                    if RX_DATETIME.match(v):
                        try:
                            dct[k] = dt.datetime.strptime(v, DATETIME_FORMAT)
                        except Exception as ex:
                            pass
                    elif RX_DATE.match(v):
                        try:
                            d = dt.datetime.strptime(v, DATE_FORMAT)
                            dct[k] = dt.date(d.year, d.month, d.day)
                        except Exception as ex:
                            pass
            return dct

        return json.loads(
            body,
            object_hook=datetime_hook
        )


class MLHandlerRayClient:
    def __init__(self, url, token):

        self._handler_info = None
        self._url = url
        self._token = token

    def _call(self, method_name, data):

        ret = requests.post(
            f'{self._url}/{method_name}',
            headers={'authorization': f'Bearer {self._token}'},
            data=Serializer.json_encode(data)
        )
        if ret.status_code != 200:
            raise RuntimeError(ret.text)
        return Serializer.json_decode(ret.text)

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
            'data': Serializer.df_to_dict(df),
            'args': args,
        }
        self._call('create', data)

    def predict(self, df, args=None):
        data = {
            'handler_info': self._handler_info,
            'data': Serializer.df_to_dict(df),
            'args': args,
        }
        ret = self._call('predict', data)
        return Serializer.dict_to_df(ret)



