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

        if 'model_name' not in args or 'version' not in args:
            raise Exception('Both model_name and version must be provided.')

            # Checking if passed model_name and version  are correct or not
        try:
            replicate.default_client.api_token = args['api_key']
            replicate.models.get(args['model_name']).versions.get(args['version'])

        except Exception as e:
            if e.args[0].startswith('Not found'):
                raise Exception(f"Could not retrieve version {args['version']} of model {args['model_name']}. Verify values are correct and try again.", e)

            elif e.args[0].startswith('Incorrect authentication token'):
                raise Exception("Provided api_key is Incorrect. Get your api_key here: https://replicate.com/account/api-tokens")

            else:
                raise Exception("Error occured.", e)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """Saves model details in storage to access it later
        """
        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """Using replicate makes the prediction according to your parameters

        Args:
            df (pd.DataFrame): The input DataFrame containing data to predict.
            args (Optional[Dict]): Additional arguments for prediction parameters.

        Returns:
            pd.DataFrame: The DataFrame containing the predicted results.
        """

        # Extracting prediction parameters from input arguments
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        model_name, version, target_col = args['model_name'], args['version'], args['target']

        # Check if '__mindsdb_row_id' column exists and drop it if present
        if '__mindsdb_row_id' in df.columns:
            df.drop(columns=['__mindsdb_row_id'], inplace=True)

        def get_data(conditions):
            # Run prediction using MindsDB's replicate library
            output = replicate.run(
                f"{args['model_name']}:{args['version']}",
                input={**conditions.to_dict(), **pred_args}         # Unpacking parameters inputted
            )
            # Process output based on the model type
            if isinstance(output, types.GeneratorType) and args.get('model_type') == 'LLM':
                output = ''.join(list(output))  # If model_type is LLM, make the stream a string
            elif isinstance(output, types.GeneratorType):
                output = list(output)[-1]  # Getting the final URL if output is a generator of frames URL
            elif isinstance(output, list) and len(output) > 0:
                output = output[-1]  # Returns generated image for controlNet models as it outputs filter and generated image
            return output

        # Check if any wrong parameters are given and raise an exception if necessary
        params_names = set(df.columns) | set(pred_args)
        available_params = self._get_schema(only_keys=True)
        wrong_params = []
        for i in params_names:
            if i not in available_params:
                wrong_params.append(i)

        if wrong_params:
            raise Exception(f"""'{wrong_params}' is/are not supported parameter for this model.
Use DESCRIBE PREDICTOR mindsdb.<model_name>.features; to know about available parameters. OR
Visit https://replicate.com/{model_name}/versions/{version} to check parameters.""")

        # Set the Replicate API token for communication with the server
        replicate.default_client.api_token = self._get_replicate_api_key(args)

        # Run prediction on the DataFrame rows and format the results into a DataFrame
        data = df.apply(get_data, axis=1)
        data = pd.DataFrame(data)
        data.columns = [target_col]

        return data

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        if attribute == "features":
            return self._get_schema()

        else:
            return pd.DataFrame(['features'], columns=['tables'])

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
            raise Exception('Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.')

    def _get_schema(self, only_keys=False):
        '''Return parameters list with its description, default value and type,
         which helps user to customize their prediction '''

        args = self.model_storage.json_get('args')
        os.environ['REPLICATE_API_TOKEN'] = self._get_replicate_api_key(args)
        replicate.default_client.api_token = self._get_replicate_api_key(args)
        model = replicate.models.get(args['model_name'])
        version = model.versions.get(args['version'])
        schema = version.openapi_schema['components']['schemas']['Input']['properties']

        # returns only list of parameter
        if only_keys:
            return schema.keys()

        for i in list(schema.keys()):
            for j in list(schema[i].keys()):
                if j not in ['default', 'description', 'type']:
                    schema[i].pop(j)

        df = pd.DataFrame(schema).T
        df = df.reset_index().rename(columns={'index': 'inputs'})
        return df.fillna('-')
