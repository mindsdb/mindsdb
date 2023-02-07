import os
import re
import math
import concurrent.futures
import numpy as np
import pandas as pd
from typing import Optional

import openai

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities.config import Config
from mindsdb.integrations.handlers.openai_handler.helpers import retry_with_exponential_backoff


class OpenAIHandler(BaseMLEngine):
    name = 'openai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = 'text-davinci-002'
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 20

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("OpenAI engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if 'question_column' not in args and 'prompt_template' not in args:
            raise Exception(f'Either of `question_column` or `prompt_template` are required.')

        if 'prompt_template' in args and 'question_column' in args:
            raise Exception('Please provide either 1) a `prompt_template` or 2) a `question_column` and an optional `context_column`, but not both.')  # noqa

    def create(self, target, args=None, **kwargs):
        args = args['using']

        args['target'] = target
        self.model_storage.json_set('args', args)

    def _get_api_key(self, args):
        # API_KEY preference order:
        #   1. provided at model creation
        #   2. provided at engine creation
        #   3. OPENAI_API_KEY env variable
        #   4. openai.api_key setting in config.json

        # 1
        if 'api_key' in args:
            return args['api_key']
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if 'api_key' in connection_args:
            return connection_args['api_key']
        # 3
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        openai_cfg = config.get('openai', {})
        if 'api_key' in openai_cfg:
            return openai_cfg['api_key']

        raise Exception('Missing API key. Either re-create this ML_ENGINE with your key in the `api_key` parameter,\
             or re-create this model and pass the API key it with `USING` syntax.')  # noqa

    def predict(self, df, args=None):
        """
        If there is a prompt template, we use it. Otherwise, we use the concatenation of `context_column` (optional) and `question_column` to ask for a completion.
        """ # noqa
        # TODO: support for edits, embeddings and moderation

        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        df = df.reset_index(drop=True)

        if args.get('question_column', False) and args['question_column'] not in df.columns:
            raise Exception(f"This model expects a question to answer in the '{args['question_column']}' column.")

        if args.get('context_column', False) and args['context_column'] not in df.columns:
            raise Exception(f"This model expects context in the '{args['context_column']}' column.")

        # api argument validation
        model_name = args.get('model_name', self.default_model)
        api_args = {
            'max_tokens': pred_args.get('max_tokens', args.get('max_tokens', self.default_max_tokens)),
            'temperature': min(1.0, max(0.0, args.get('temperature', 0.0))),
            'top_p': pred_args.get('top_p', None),
            'n': pred_args.get('n', None),
            'stop': pred_args.get('stop', None),
            'presence_penalty': pred_args.get('presence_penalty', None),
            'frequency_penalty': pred_args.get('frequency_penalty', None),
            'best_of': pred_args.get('best_of', None),
            'logit_bias': pred_args.get('logit_bias', None),
            'user': pred_args.get('user', None),
        }

        if args.get('prompt_template', False):
            if pred_args.get('prompt_template', False):
                base_template = pred_args['prompt_template']  # override with predict-time template if available
            else:
                base_template = args['prompt_template']
            columns = []
            spans = []
            matches = list(re.finditer("{{(.*?)}}", base_template))

            first_span = matches[0].start()
            last_span = matches[-1].end()

            for m in matches:
                columns.append(m[0].replace('{', '').replace('}', ''))
                spans.extend((m.start(), m.end()))

            spans = spans[1:-1]
            template = [base_template[s:e] for s, e in zip(spans, spans[1:])]
            template.insert(0, base_template[0:first_span])
            template.append(base_template[last_span:])

            empty_prompt_ids = np.where(df[columns].isna().all(axis=1).values)[0]

            df['__mdb_prompt'] = ''
            for i in range(len(template)):
                atom = template[i]
                if i < len(columns):
                    col = df[columns[i]].replace(to_replace=[None], value='')  # add empty quote if data is missing
                    df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom) + col
                else:
                    df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom)
            prompts = list(df['__mdb_prompt'])

        elif args.get('context_column', False):
            empty_prompt_ids = np.where(df[[args['context_column'],
                                           args['question_column']]].isna().all(axis=1).values)[0]
            contexts = list(df[args['context_column']].apply(lambda x: str(x)))
            questions = list(df[args['question_column']].apply(lambda x: str(x)))
            prompts = [f'Context: {c}\nQuestion: {q}\nAnswer: ' for c, q in zip(contexts, questions)]

        else:
            empty_prompt_ids = np.where(df[[args['question_column']]].isna().all(axis=1).values)[0]
            prompts = list(df[args['question_column']].apply(lambda x: str(x)))

        # remove prompts without signal from completion queue
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        api_key = self._get_api_key(args)
        api_args = {k: v for k, v in api_args.items() if v is not None}  # filter out non-specified api args
        completion = self._completion(model_name, prompts, api_key, api_args, args)

        # add null completion for empty prompts
        for i in sorted(empty_prompt_ids):
            completion.insert(i, None)

        pred_df = pd.DataFrame(completion, columns=[args['target']])
        return pred_df

    def _completion(self, model_name, prompts, api_key, api_args, args, parallel=True):
        """
        Handles completion for an arbitrary amount of rows.

        There are a couple checks that should be done when calling OpenAI's API:
          - account max batch size, to maximize batch size first
          - account rate limit, to maximize parallel calls second

        Additionally, single completion calls are done with exponential backoff to guarantee all prompts are processed,
        because even with previous checks the tokens-per-minute limit may apply.
        """
        @retry_with_exponential_backoff
        def _submit_completion(model_name, prompts, api_key, api_args, args):
            return openai.Completion.create(
                model=model_name,
                prompt=prompts,
                api_key=api_key,
                organization=args.get('api_organization'),
                **api_args
            )

        def _tidy(comp):
            return [c['text'].strip('\n').strip('') for c in comp['choices']]

        try:
            # check if simple completion works
            completion = _submit_completion(
                model_name,
                prompts,
                api_key,
                api_args,
                args
            )
            return _tidy(completion)  
        except openai.error.InvalidRequestError as e:
            # else, we get the max batch size
            e = e.user_message
            if 'you can currently request up to at most a total of' in e:
                pattern = 'a total of'
                max_batch_size = int(e[e.find(pattern) + len(pattern):].split(').')[0])
            else:
                max_batch_size = self.max_batch_size  # guards against changes in the API message

        if not parallel:
            completion = None
            for i in range(math.ceil(len(prompts) / max_batch_size)):
                partial = _submit_completion(model_name,
                                             prompts[i * max_batch_size:(i + 1) * max_batch_size],
                                             api_key,
                                             api_args,
                                             args)
                if not completion:
                    completion = partial
                else:
                    completion['choices'].extend(partial['choices'])
                    for field in ('prompt_tokens', 'completion_tokens', 'total_tokens'):
                        completion['usage'][field] += partial['usage'][field]
        else:
            promises = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for i in range(math.ceil(len(prompts) / max_batch_size)):
                    print(f'{i * max_batch_size}:{(i+1) * max_batch_size}/{len(prompts)}')
                    future = executor.submit(_submit_completion,
                                             model_name,
                                             prompts[i * max_batch_size:(i + 1) * max_batch_size],
                                             api_key,
                                             api_args,
                                             args)
                    promises.append({"choices": future})
            completion = None
            for p in promises:
                if not completion:
                    completion = p['choices'].result()
                else:
                    completion['choices'].extend(p['choices'].result()['choices'])

        return _tidy(completion)

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        api_key = self._get_api_key(args)

        model_name = args.get('model_name', self.default_model)
        meta = openai.Model.retrieve(model_name, api_key=api_key)

        return pd.DataFrame([[meta['id'], meta['object'], meta['owned_by'], meta['permission'], args]],
                            columns=['id', 'object', 'owned_by', 'permission', 'model_args'])
