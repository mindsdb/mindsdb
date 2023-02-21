import os
import re
import math
import json
import shutil
import tempfile
import datetime
import textwrap
import subprocess
import concurrent.futures
from typing import Optional, Dict

import openai
import numpy as np
import pandas as pd

from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.openai_handler.helpers import retry_with_exponential_backoff


class OpenAIHandler(BaseMLEngine):
    name = 'openai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = 'text-davinci-002'
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("OpenAI engine requires a USING clause! Refer to its documentation for more details.")
        else:
            args = args['using']

        if len(set(args.keys()) & {'question_column', 'prompt_template', 'json_struct'}) == 0:
            raise Exception('One of `question_column`, `prompt_template` or `json_struct` is required for this engine.')

        keys_collection = [
            ['prompt_template'],
            ['question_column', 'context_column'],
            ['json_struct']
        ]
        for keys in keys_collection:
            if keys[0] in args and any(x[0] in args for x in keys_collection if x != keys):
                raise Exception(textwrap.dedent('''\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `json_struct`
                '''))

    def create(self, target, args=None, **kwargs):
        args = args['using']

        args['target'] = target
        if not args.get('model_name'):
            args['model_name'] = self.default_model

        self.model_storage.json_set('args', args)

    def _get_api_key(self, args):
        """ 
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. OPENAI_API_KEY env variable
            4. openai.api_key setting in config.json
        """  # noqa
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

        elif args.get('json_struct', False):
            empty_prompt_ids = np.where(df[[args['input_text']]].isna().all(axis=1).values)[0]
            prompts = []
            for i in df.index:
                if 'json_struct' in df.columns:
                    if isinstance(df['json_struct'][i], str):
                        df['json_struct'][i] = json.loads(df['json_struct'][i])
                    json_struct = ', '.join(df['json_struct'][i].values())
                else:
                    json_struct = ', '.join(args['json_struct'].values())
                p = textwrap.dedent(f'''\
                    From sentence below generate a one-dimensional array with data in it:
                    {json_struct}.
                    The sentence is:
                    {{{{{args['input_text']}}}}}
                ''')
                for column in df.columns:
                    if column == 'json_struct':
                        continue
                    p = p.replace(f'{{{{{column}}}}}', df[column][i])
                prompts.append(p)

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

        # restore json struct
        if args.get('json_struct', False):
            for i in pred_df.index:
                try:
                    if 'json_struct' in df.columns:
                        json_keys = df['json_struct'][i].keys()
                    else:
                        json_keys = args['json_struct'].keys()
                    pred_df[args['target']][i] = {
                        key: val for key, val in zip(
                            json_keys,
                            json.loads(pred_df[args['target']][i])
                        )
                    }
                except Exception:
                    pred_df[args['target']][i] = None

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
        @retry_with_exponential_backoff()
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
        # TODO: Update to use update() artifacts
        args = self.model_storage.json_get('args')
        api_key = self._get_api_key(args)

        model_name = args.get('model_name', self.default_model)
        meta = openai.Model.retrieve(model_name, api_key=api_key)

        return pd.DataFrame([[meta['id'], meta['object'], meta['owned_by'], meta['permission'], args]],
                            columns=['id', 'object', 'owned_by', 'permission', 'model_args'])

    def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Fine-tune OpenAI GPT models. Steps are roughly:
          - Analyze input data and modify it according to suggestions made by the OpenAI utility tool
          - Get a training and validation file
          - Determine base model to use
          - Submit a fine-tuning job via the OpenAI API
          - Monitor progress with exponential backoff (which has been modified for greater control given a time budget in hours), 
          - Gather stats once fine-tuning finishes
          - Modify model metadata so that the new version triggers the fine-tuned version of the model (stored in the user's OpenAI account)

        Caveats: 
          - As base fine-tuning models, OpenAI only supports the original GPT ones: `ada`, `babbage`, `curie`, `davinci`. This means if you adjust successively more than once, any fine-tuning other than the most recent one is lost.
        """  # noqa

        args = args if args else {}
        using_args = args.pop('using') if 'using' in args else {}
        prompt_col = using_args.get('prompt_column', 'prompt')
        completion_col = using_args.get('completion_column', 'completion')

        for col in [prompt_col, completion_col]:
            if col not in set(df.columns):
                raise Exception(f"To fine-tune this OpenAI model, please format your select data query to have a `{prompt_col}` column and a `{completion_col}` column first.")  # noqa

        args = {**using_args, **args}
        prev_model_name = self.base_model_storage.json_get('args').get('model_name', '')

        openai.api_key = self._get_api_key(args)
        adjust_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        temp_storage_path = tempfile.mkdtemp()
        temp_file_name = f"ft_{adjust_time}"
        temp_model_storage_path = f"{temp_storage_path}/{temp_file_name}.jsonl"
        df.to_json(temp_model_storage_path, orient='records', lines=True)

        # TODO avoid subprocess usage once OpenAI enables non-CLI access
        subprocess.run(
            [
                "openai", "tools", "fine_tunes.prepare_data",
                "-f", temp_model_storage_path,                  # from file
                '-q'                                            # quiet mode (accepts all suggestions)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        file_names = [f'{temp_file_name}.jsonl',
                      f'{temp_file_name}_prepared_train.jsonl',
                      f'{temp_file_name}_prepared_valid.jsonl']
        returns = []
        for file_name in file_names:
            if os.path.isfile(os.path.join(temp_storage_path, file_name)):
                returns.append(openai.File.create(
                    file=open(f"{temp_storage_path}/{file_name}", "rb"),
                    purpose='fine-tune')
                )
            else:
                returns.append(None)

        train_file_id = returns[1].id if isinstance(returns[1], openai.File) else returns[0].id
        val_file_id = returns[2].id if isinstance(returns[2], openai.File) else None

        def _get_model_type(model_name: str):
            for model_type in ['ada', 'curie', 'babbage', 'davinci']:
                if model_type in model_name.lower():
                    return model_type
            return 'ada'

        # `None` values are internally imputed by OpenAI to `null` or default values
        ft_params = {
            'training_file': train_file_id,
            'validation_file': val_file_id,
            'model': _get_model_type(prev_model_name),
            'suffix': 'mindsdb',
            'n_epochs': using_args.get('n_epochs', None),
            'batch_size': using_args.get('batch_size', None),
            'learning_rate_multiplier': using_args.get('learning_rate_multiplier', None),
            'prompt_loss_weight': using_args.get('prompt_loss_weight', None),
            'compute_classification_metrics': using_args.get('compute_classification_metrics', None),
            'classification_n_classes': using_args.get('classification_n_classes', None),
            'classification_positive_class': using_args.get('classification_positive_class', None),
            'classification_betas': using_args.get('classification_betas', None),
        }

        start_time = datetime.datetime.now()
        ft_result = openai.FineTune.create(**{k: v for k, v in ft_params.items() if v is not None})

        @retry_with_exponential_backoff(hour_budget=args.get('hour_budget', 8))
        def _check_ft_status(model_id):
            ft_retrieved = openai.FineTune.retrieve(id=model_id)
            if ft_retrieved['status'] in ('succeeded', 'failed'):
                return ft_retrieved
            else:
                raise openai.error.OpenAIError('Fine-tuning still pending!')

        ft_stats = _check_ft_status(ft_result.id)
        ft_model_name = ft_stats['fine_tuned_model']

        if ft_stats['status'] != 'succeeded':
            raise Exception(f"Fine-tuning did not complete successfully (status: {ft_stats['status']}). Error message: {ft_stats['events'][-1]['message']}")  # noqa

        end_time = datetime.datetime.now()
        runtime = end_time - start_time

        result_file_id = openai.FineTune.retrieve(id=ft_result.id)['result_files'][0].id
        name_extension = openai.File.retrieve(id=result_file_id).filename
        result_path = f'{temp_storage_path}/ft_{adjust_time}_result_{name_extension}'
        with open(result_path, 'wb') as f:
            f.write(openai.File.download(id=result_file_id))

        train_stats = pd.read_csv(result_path)
        if 'validation_token_accuracy' in train_stats.columns:
            train_stats = train_stats[train_stats['validation_token_accuracy'].notnull()]

        args['model_name'] = ft_model_name
        args['ft_api_info'] = ft_stats.to_dict_recursive()
        args['ft_result_stats'] = train_stats.to_dict()
        args['runtime'] = runtime.total_seconds()

        self.model_storage.json_set('args', args)
        shutil.rmtree(temp_storage_path)
