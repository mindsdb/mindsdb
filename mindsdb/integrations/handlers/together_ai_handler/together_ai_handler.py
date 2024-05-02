import os
from typing import Optional, Dict

import together
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm_utils import get_completed_prompts
from mindsdb.integrations.utilities.handler_utils import get_api_key


class TogetherAIHandler(BaseMLEngine):
    """
    Integration with the Together AI Inference API.
    """

    name = 'together_ai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_max_tokens = 128
        self.default_temperature = 0.9
        self.default_max_workers = 10

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'using' not in args:
            raise ValueError("Together AI engine requires a USING clause! Refer to its documentation for more details.")
        # set api key
        api_key = get_api_key('together_ai', args['using'], self.engine_storage)
        together.api_key = api_key

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if 'predict_params' not in args and 'input_column' not in args['predict_params']:
            raise Exception("Together AI engine requires an input column! Refer to its documentation for more details.")
        params = args['predict_params']
        if 'model_name' not in params:
            raise Exception("Together AI engine requires a model name! Refer to its documentation for more details.")

        if 'prompt_template' in params:
            df['prompt'], empty_prompt_ids = get_completed_prompts(params['prompt_template'], df)
        else:
            df['prompt'] = df[params['input_column']]

        new_column = params['output_column'] if 'output_column' in params else 'prediction'

        with ThreadPoolExecutor(max_workers=params['workers'] if 'workers' in params else self.default_max_workers) as executor:
            df[new_column] = list(executor.map(lambda x: together.Complete.create(
                prompt=x,
                model=params['model_name'],
                max_tokens=params['max_tokens'] if 'max_tokens' in params else self.default_max_tokens,
                temperature=params['temperature'] if 'temperature' in params else self.default_temperature,
                top_p=params['top_p'] if 'top_p' in params else None,
                top_k=params['top_k'] if 'top_k' in params else None,
                repetition_penalty=params['repetition_penalty'] if 'repetition_penalty' in params else None,
                logprobs=params['logprobs'] if 'logprobs' in params else None
            )['output']['choices'][0]['text'], df['prompt']))

        return df
