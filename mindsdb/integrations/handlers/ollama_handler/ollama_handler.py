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
        self.model_storage.json_set('args', args)

        def _model_check():
            """ Checks model has been pulled and that it works correctly. """
            try:
                return requests.post(
                    connection + '/api/generate',
                    json={
                        'model': args['model_name'],
                        'prompt': 'Hello.',
                    }
                ).status_code
            except Exception:
                return 500

        # check model
        response = _model_check()
        if response != 200:
            # pull model (blocking operation) and serve
            # TODO: point to the engine storage folder instead of default location
            connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
            requests.post(connection + '/api/pull', json={'name': args['model_name']})
            # try one last time
            response = _model_check()
            if response != 200:
                raise Exception(f"Ollama model `{args['model_name']}` is not working correctly (`pull` status code: {response}). Please try pulling this model manually, check it works correctly and try again.")  # noqa

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
        # TODO v2: add support for overriding modelfile params (e.g. temperature)

        # prepare prompts
        prompts, empty_prompt_ids = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts

        # call llm
        completions = []
        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                connection = args.get('ollama_serve_url', OllamaHandler.DEFAULT_SERVE_URL)
                raw_output = requests.post(
                    connection + '/api/generate',
                    json={
                        'model': args['model_name'],
                        'prompt': row['__mdb_prompt'],
                    }
                )
                out_tokens = raw_output.content.decode().split('\n')  # stream of output tokens

                tokens = []
                for o in out_tokens:
                    if o != '':
                        # TODO v2: add support for storing `context` short conversational memory
                        info = json.loads(o)
                        if 'response' in info:
                            token = info['response']
                            tokens.append(token)

                completions.append(''.join(tokens))
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
