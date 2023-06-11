import replicate
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Dict, Optional
import os
import types
from mindsdb.utilities.config import Config


class ReplicateHandler(BaseMLEngine):
    name = "replicate"

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("Replicate engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if 'model_name' and 'version' not in args:
            raise Exception('Add model_name and version ')

            # Checking if passed model_name and version  are correct or not
        try:
            replicate.default_client.api_token = args['api_key']
            # os.environ['REPLICATE_API_TOKEN']= args['api_key']

            replicate.models.get(args['model_name']).versions.get(args['version'])
        except Exception as e:
            print(e)
            raise Exception("Check your model_name and version carefully")

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Saves a model inside the engine registry for later usage.
        Normally, an input dataframe is required to train the model.
        However, some integrations may merely require registering the model instead of training, in which case `df` can be omitted.
        Any other arguments required to register the model can be passed in an `args` dictionary.
        """
        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Calls a model with some input dataframe `df`, and optionally some arguments `args` that may modify the model behavior.
        The expected output is a dataframe with the predicted values in the target-named column.
        Additional columns can be present, and will be considered row-wise explanations if their names finish with `_explain`.
        """
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        replicate.default_client.api_token = self._get_replicate_api_key(args)

        output = df['prompt'].map(lambda x: replicate.run(
            f"{args['model_name']}:{args['version']}",
            input={'prompt': x, **pred_args}
        ))

        if isinstance(output, types.GeneratorType):
            output = [list(output)[-1]]
        output = pd.DataFrame(output[0])
        output.columns = [args['target']]
        return output

    def create_engine(self, connection_args: dict):
        #  self.engine_storage.json_set()
        pass

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        

        if attribute == "features":
            return self._get_schema()

        else:
            return self._get_schema()

    def _get_replicate_api_key(self, args, strict=True):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. REPLICATE_API_KEY env variable
            4. replicate.api_key setting in config.json
        """  # noqa
        # 1
        if 'api_key' in args:
            return args['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('REPLICATE_API_TOKEN')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        replicate_cfg = config.get('replicate', {})
        if 'api_key' in replicate_cfg:
            return replicate_cfg['api_key']

        if strict:
            raise Exception(f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')

    def _get_schema(self):
        '''Return features to populate '''
        args = self.model_storage.json_get('args')
        os.environ['REPLICATE_API_TOKEN'] = self._get_replicate_api_key(args)
        replicate.default_client.api_token = self._get_replicate_api_key(args)
        model = replicate.models.get(args['model_name'])
        version = model.versions.get(args['version'])
        schema = version.get_transformed_schema()['components']['schemas']['Input']['properties']

        for i in list(schema.keys()):
            for j in list(schema[i].keys()):
                if j not in ['default', 'description', 'type']:
                    schema[i].pop(j)

        print(schema)
        df = pd.DataFrame(schema).T
        df = df.reset_index().rename(columns={'index': 'columns'})
    
        return df
