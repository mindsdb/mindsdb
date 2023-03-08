import re
import asyncio
from typing import Optional, Dict

import numpy as np
import pandas as pd

from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class LangChainHandler(OpenAIHandler):
    name = 'langchain'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stops = []

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if len(set(args.keys()) & {'prompt_template'}) == 0:
            raise Exception('Please provide a `prompt_template` for this engine.')

    def predict(self, df, args=None):
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        df = df.reset_index(drop=True)

        if 'prompt_template' not in args and 'prompt_template' not in pred_args:
            raise Exception(f"This model expects a prompt template, please provide one.")

        # api argument validation
        model_name = args.get('model_name', self.default_model)

        # TODO: enable other LLM backends (AI21, Anthropic, etc.)
        if 'stops' in pred_args:
            self.stops = pred_args['stops']

        model_kwargs = {
            'model_name': model_name,
            'temperature': min(1.0, max(0.0, args.get('temperature', 0.0))),
            'max_tokens': pred_args.get('max_tokens', args.get('max_tokens', self.default_max_tokens)),
            'top_p': pred_args.get('top_p', None),
            'frequency_penalty': pred_args.get('frequency_penalty', None),
            'presence_penalty': pred_args.get('presence_penalty', None),
            'n': pred_args.get('n', None),
            'best_of': pred_args.get('best_of', None),
            'openai_api_key': self._get_api_key(args),
            'request_timeout': pred_args.get('request_timeout', None),  # TODO value?
            'logit_bias': pred_args.get('logit_bias', None),
        }
        # filter out None values
        model_kwargs = {k: v for k, v in model_kwargs.items() if v is not None}
        model = OpenAI(**model_kwargs)

        # TODO abstract into a common utility method
        if pred_args.get('prompt_template', False):
            base_template = pred_args['prompt_template']  # override with predict-time template if available
        else:
            base_template = args['prompt_template']

        input_variables = []
        matches = list(re.finditer("{{(.*?)}}", base_template))

        for m in matches:
            input_variables.append(m[0].replace('{', '').replace('}', ''))

        empty_prompt_ids = np.where(df[input_variables].isna().all(axis=1).values)[0]

        base_template = base_template.replace('{{', '{').replace('}}', '}')
        prompts = []

        for i, row in df.iterrows():
            if i not in empty_prompt_ids:
                prompt = PromptTemplate(input_variables=input_variables, template=base_template)
                kwargs = {}
                for col in input_variables:
                    kwargs[col] = row[col] if row[col] is not None else ''  # add empty quote if data is missing
                prompts.append(prompt.format(**kwargs))

        completion = self._completion(model, prompts, args)

        # add null completion for empty prompts
        for i in sorted(empty_prompt_ids):
            completion.insert(i, None)

        pred_df = pd.DataFrame(completion, columns=[args['target']])

        return pred_df

    def _completion(self, model, prompts, args):
        def _tidy(completions):
            return [c.generations[0][0].text.strip('\n') for c in completions]

        completion = [model.generate([prompt]) for prompt in prompts]
        return _tidy(completion)

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        raise NotImplementedError('Update is not implemented for LangChain models')

    def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        raise NotImplementedError('Update is not implemented for LangChain models')
