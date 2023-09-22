import json
import requests
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine


class OllamaHandler(BaseMLEngine):
    name = "ollama"
    MODEL_LIST = [  # TODO: replace with this API call:
        'llama2',
        'llama2-uncensored',
        'codellama',
        'codeup',
        'everythinglm',
        'falcon',
        'llama2-chinese',
        'medllama2',
        'nous-hermes',
        'open-orca-platypus2',
        'orca-mini',
        'phind-codellama',
        'stable-beluga',
        'vicuna',
        'wizard-math',
        'wizard-vicuna',
        'wizard-vicuna-uncensored',
        'wizardcoder',
        'wizardllm',
        'wizardllm-uncensored',
    ]

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("Ollama engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        # check model version is valid
        if 'model_name' not in args:
            raise Exception('`model_name` must be provided in the USING clause.')
        elif args['model_name'] not in OllamaHandler.MODEL_LIST:
            raise Exception(f"The model `{args['model_name']}` is not yet supported by Ollama! Please choose one of the following: {OllamaHandler.MODEL_LIST}")  # noqa

        # check ollama service health
        status = requests.get('http://localhost:11434/api/tags').status_code
        if status != 200:
            raise Exception(f"Ollama service is not working (status `{status}`). Please double check it is running and try again.")  # noqa

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """ Pull LLM artifacts with Ollama API. """
        # argument setting
        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

        # download model
        # TODO: Ollama should let us point to the engine storage folder for this. For now, we use their default
        model_name = args['model_name']
        # sync operation, finishes once model has been fully pulled
        requests.post('http://localhost:11434/api/pull', json={'name': model_name})

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Generate text completions with the local LLM.
            Args:
                df (pd.DataFrame): The input DataFrame containing data to predict.
                args (Optional[Dict]): Additional arguments for prediction parameters.
            Returns:
                pd.DataFrame: The DataFrame containing row-wise text completions.
        """
        args = self.model_storage.json_get('args')
        model_name, target_col = args['model_name'], args['target']

        completions = []
        for i, row in df.iterrows():
            raw_output = requests.post(
                'http://localhost:11434/api/generate',
                json={
                    'model': args['model_name'],
                    'prompt': row['prompt'],  # TODO: make this user-configurable from `input_col` or similar
                }
            )
            out_tokens = raw_output.content.decode().split('\n')  # stream of output tokens

            tokens = []
            for o in out_tokens:
                if o != '':
                    info = json.loads(o)
                    if 'response' in info:
                        token = info['response']
                        tokens.append(token)

            completions.append(''.join(tokens))

        # consolidate output
        data = pd.DataFrame(completions)
        data.columns = [target_col]
        return data

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError()
        # if attribute == "features":
        #     return self._get_schema()
        #
        # else:
        #     return pd.DataFrame(['features'], columns=['tables'])
