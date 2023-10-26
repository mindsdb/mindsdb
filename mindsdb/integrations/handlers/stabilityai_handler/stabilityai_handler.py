from typing import Optional, Dict
import pandas as pd

from mindsdb.integrations.handlers.stabilityai_handler.stabilityai import StabilityAPIClient

from mindsdb.integrations.libs.base import BaseMLEngine


class StabilityAIHandler(BaseMLEngine):
    name = "stabilityai"

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        args = args['using']
        
        if 'api_key' not in args:
            raise Exception('api_key has to be specified')

        if 'input_column' not in args:
            raise Exception('input_column has to be specified')
        
        if 'local_directory_path' not in args:
            raise Exception('local_directory_path has to be specified')
        
        client = StabilityAPIClient(args["api_key"], args["local_directory_path"])
        
        if 'stability_engine_id' in args:
            if not client._is_valid_engine(args["stability_engine_id"]):
                raise Exception("Unknown engine. The available engines are - " + list(client.available_engines.keys()))

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise Exception("Stability AI Inference engine requires a USING clause! Refer to its documentation for more details.")
        self.generative = True
        
        self.model_storage.json_set('args', args["using"])

    def predict(self, df, args=None):
        
        args = self.model_storage.json_get('args')
        api_key = self._get_stability_api_key(args)

        input_column = args['input_column']
        
        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')
        
        local_directory_path = args["local_directory_path"]
        stability_engine_id = args.get('stability_engine_id', "stable-diffusion-xl-1024-v1-0")
        
        client = StabilityAPIClient(api_key=api_key, dir_to_save=local_directory_path, engine=stability_engine_id)
        
        result_df = pd.DataFrame()
        
        result_df['predictions'] = df[input_column].apply(self.generate_image, client=client)
        
        result_df = result_df.rename(columns={'predictions': args['target']})
        
        return result_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        client = StabilityAPIClient(args["api_key"], "")
        stability_engine_id = args["stability_engine_id"]
        response = client.available_engines.get(stability_engine_id)
        description = {}
        description[''] = response.body['name']
        description['model_version'] = response.body['model_version']
        description['date_created'] = response.body['created']
        # pre-trained monkeylearn models guide about what industries they can be used
        description['industries'] = response.body['industries']
        des_df = pd.DataFrame([description])
        return des_df
    
    def _get_stability_api_key(self, args):
        if 'api_key' in args:
            return args['api_key']

        connection_args = self.engine_storage.get_connection_args()
        
        if 'api_key' in connection_args:
            return connection_args['api_key']

        raise Exception("Missing API key 'api_key'. Either re-create this ML_ENGINE specifying the `api_key` parameter\
                 or re-create this model and pass the API key with `USING` syntax.")
        
    def generate_image(self, text, client):
        return client.text_to_image(prompt=text)
