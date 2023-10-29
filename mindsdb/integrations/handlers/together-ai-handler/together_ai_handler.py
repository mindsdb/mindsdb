import os
from typing import Optional, Dict

import together
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm_utils import get_completed_prompts


class TogetherAIHandler(BaseMLEngine):
    """
    Integration with the Together AI Inference API.
    """

    name = 'together_ai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = 'togethercomputer/RedPajama-INCITE-7B-Chat'
        self.default_max_tokens = 128

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise ValueError("Together AI engine requires a USING clause! Refer to its documentation for more details.")
        # set api key
        api_key = self._get_together_ai_api_key(args['using'])
        together.api_key = api_key

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'predict_params' not in args and 'input_column' not in args['predict_params']:
            raise Exception("Together AI engine requires an input column! Refer to its documentation for more details.")
        
        params = args['predict_params']

        if 'prompt_template' in params:
            df['prompts'], empty_prompt_ids = get_completed_prompts(params['prompt_template'], df)
        else:
            df['prompts'] = df[params['input_column']]

        new_column = params['output_column'] if 'output_column' in params else 'prediction'

        with ThreadPoolExecutor(max_workers=5) as executor:
            df[new_column] = list(executor.map(lambda x: together.Complete.create(
                prompt = x,
                model = params['model_name'] if 'model_name' in params else self.default_model,
                max_tokens = params['max_tokens'] if 'max_tokens' in params else self.default_max_tokens,
                temperature = params['temperature'] if 'temperature' in params else 0.5,
                top_p = params['top_p'] if 'top_p' in params else None,
                top_k = params['top_k'] if 'top_k' in params else None,
                repetition_penalty = params['repetition_penalty'] if 'repetition_penalty' in params else None,
                logprobs = params['logprobs'] if 'logprobs' in params else None
            )['output']['choices'][0]['text'], df['prompts']))

        return df

    def _get_together_ai_api_key(self, args, strict=True):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. TOGETHER_AI_API_KEY env variable
            4. together_ai.api_key setting in config.json
        """  # noqa
        # 1
        if 'api_key' in args:
            return args['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('TOGETHER_AI_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get('together_ai', {})
        if 'api_key' in openai_cfg:
            return openai_cfg['api_key']

        if strict:
            raise Exception(f'Missing API key "api_key". Re-create this model and pass the API key with `USING` syntax.')
        