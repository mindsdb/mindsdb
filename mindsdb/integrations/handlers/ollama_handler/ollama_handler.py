import json
import requests
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts


class OllamaHandler(BaseMLEngine):
    name = "ollama"
    DEFAULT_SERVE_URL = "http://localhost:11434"

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("Ollama engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if 'model_name' not in args:
            raise Exception('`model_name` must be provided in the USING clause.')

        # check ollama service health
        connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
        status = requests.get(connection + '/api/tags').status_code
        if status != 200:
            raise Exception(f"Ollama service is not working (status `{status}`). Please double check it is running and try again.")  # noqa

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """ Pull LLM artifacts with Ollama API. """
        # arg setter
        args = args['using']
        args['target'] = target
        connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)

        def _model_check():
            """ Checks model has been pulled and that it works correctly. """
            responses = {}
            for endpoint in ['generate', 'embeddings']:
                try:
                    code = requests.post(
                        connection + f'/api/{endpoint}',
                        json={
                            'model': args['model_name'],
                            'prompt': 'Hello.',
                        }
                    ).status_code
                    responses[endpoint] = code
                except Exception:
                    responses[endpoint] = 500
            return responses

        # check model for all supported endpoints
        responses = _model_check()
        if 200 not in responses.values():
            # pull model (blocking operation) and serve
            # TODO: point to the engine storage folder instead of default location
            connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
            requests.post(connection + '/api/pull', json={'name': args['model_name']})
            # try one last time
            responses = _model_check()
            if 200 not in responses.values():
                raise Exception(f"Ollama model `{args['model_name']}` is not working correctly. Please try pulling this model manually, check it works correctly and try again.")  # noqa

        supported_modes = {k: True if v == 200 else False for k, v in responses.items()}

        # check if a mode has been provided and if it is valid
        runnable_modes = [mode for mode, supported in supported_modes.items() if supported]
        if 'mode' in args:
            if args['mode'] not in runnable_modes:
                raise Exception(f"Mode `{args['mode']}` is not supported by the model `{args['model_name']}`.")

        # if a mode has not been provided, check if the model supports only one mode
        # if it does, set it as the default mode
        # if it supports multiple modes, set the default mode to 'generate'
        else:
            if len(runnable_modes) == 1:
                args['mode'] = runnable_modes[0]
            else:
                args['mode'] = 'generate'

        self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Generate text completions with the local LLM.
            Args:
                df (pd.DataFrame): The input DataFrame containing data to predict.
                args (Optional[Dict]): Additional arguments for prediction parameters.
            Returns:
                pd.DataFrame: The DataFrame containing row-wise text completions.
        """
        # setup
        pred_args = args.get('predict_params', {})
        args = self.model_storage.json_get('args')
        model_name, target_col = args['model_name'], args['target']
        prompt_template = pred_args.get('prompt_template',
                                        args.get('prompt_template', 'Answer the following question: {{{{text}}}}'))

        # prepare prompts
        prompts, empty_prompt_ids = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts

        # setup endpoint
        endpoint = args.get('mode', 'generate')

        # call llm
        completions = []
        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
                raw_output = requests.post(
                    connection + f'/api/{endpoint}',
                    json={
                        'model': model_name,
                        'prompt': row['__mdb_prompt'],
                    }
                )
                lines = raw_output.content.decode().split('\n')  # stream of output tokens

                values = []
                for line in lines:
                    if line != '':
                        info = json.loads(line)
                        if 'response' in info:
                            token = info['response']
                            values.append(token)
                        elif 'embedding' in info:
                            embedding = info['embedding']
                            values.append(embedding)

                if endpoint == 'embeddings':
                    completions.append(values)
                else:
                    completions.append(''.join(values))
            else:
                completions.append('')

        # consolidate output
        data = pd.DataFrame(completions)
        data.columns = [target_col]
        return data

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        model_name, target_col = args['model_name'], args['target']
        prompt_template = args.get('prompt_template', 'Answer the following question: {{{{text}}}}')

        if attribute == "features":
            return pd.DataFrame([[target_col, prompt_template]], columns=['target_column', 'mindsdb_prompt_template'])

        # get model info
        else:
            connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
            model_info = requests.post(connection + '/api/show', json={'name': model_name}).json()
            return pd.DataFrame([[
                model_name,
                model_info.get('license', 'N/A'),
                model_info.get('modelfile', 'N/A'),
                model_info.get('parameters', 'N/A'),
                model_info.get('template', 'N/A'),
            ]],
                columns=[
                    'model_type',
                    'license',
                    'modelfile',
                    'parameters',
                    'ollama_base_template',
            ])
