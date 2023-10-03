import json
import requests
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm_utils import get_completed_prompts


class OllamaHandler(BaseMLEngine):
    name = "ollama"
    SERVE_URL = 'http://localhost:11434'
    MODEL_LIST_URL = 'https://registry.ollama.ai/v2/_catalog'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("Ollama engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        # check model version is valid
        try:
            all_models = requests.get(OllamaHandler.MODEL_LIST_URL).json()['repositories']
        except Exception as e:
            raise Exception(f"Could not retrieve model list from Ollama registry: {e}")
        base_models = list(filter(lambda x: 'library/' in x, all_models))
        valid_models = [m.split('/')[-1] for m in base_models]

        if 'model_name' not in args:
            raise Exception('`model_name` must be provided in the USING clause.')
        elif args['model_name'] not in valid_models:
            raise Exception(f"The model `{args['model_name']}` is not yet supported by Ollama! Please choose one of the following: {valid_models}")  # noqa

        # check ollama service health
        status = requests.get(OllamaHandler.SERVE_URL + '/api/tags').status_code
        if status != 200:
            raise Exception(f"Ollama service is not working (status `{status}`). Please double check it is running and try again.")  # noqa

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """ Pull LLM artifacts with Ollama API. """
        # arg setter
        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

        # download model
        # TODO v2: point Ollama to the engine storage folder instead of their default location
        model_name = args['model_name']
        # blocking operation, finishes once model has been fully pulled and served
        requests.post(OllamaHandler.SERVE_URL + '/api/pull', json={'name': model_name})

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
                raw_output = requests.post(
                    OllamaHandler.SERVE_URL + '/api/generate',
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
            model_info = requests.post(OllamaHandler.SERVE_URL + '/api/show', json={'name': model_name}).json()
            return pd.DataFrame([[
                model_name,
                model_info['license'],
                model_info['modelfile'],
                model_info['parameters'],
                model_info['template'],
            ]],
                columns=[
                    'model_type',
                    'license',
                    'modelfile',
                    'parameters',
                    'ollama_base_template',
                ])
