import os
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
from openai import OpenAI, NotFoundError, AuthenticationError
import numpy as np
import pandas as pd

from mindsdb.utilities.hooks import before_openai_query, after_openai_query
from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.openai_handler.helpers import (
    retry_with_exponential_backoff,
    truncate_msgs_for_token_limit,
    get_available_models,
    PendingFT,
)
from mindsdb.integrations.handlers.openai_handler.constants import (
    CHAT_MODELS,
    IMAGE_MODELS,
    FINETUNING_MODELS,
    OPENAI_API_BASE,
)
from mindsdb.integrations.utilities.handler_utils import get_api_key
from mindsdb.integrations.libs.llm_utils import get_completed_prompts

logger = log.getLogger(__name__)


class OpenAIHandler(BaseMLEngine):
    name = 'openai'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True
        self.default_model = 'gpt-3.5-turbo'
        self.default_image_model = 'dall-e-2'
        self.default_mode = (
            'default'  # can also be 'conversational' or 'conversational-full'
        )
        self.supported_modes = [
            'default',
            'conversational',
            'conversational-full',
            'image',
            'embedding',
        ]
        self.rate_limit = 60  # requests per minute
        self.max_batch_size = 20
        self.default_max_tokens = 100
        self.chat_completion_models = CHAT_MODELS
        self.supported_ft_models = FINETUNING_MODELS # base models compatible with finetuning

    def create_engine(self, connection_args):
        '''check api key if provided
        '''
        connection_args = {k.lower(): v for k, v in connection_args.items()}
        api_key = connection_args.get('openai_api_key')
        if api_key is not None:
            api_base = connection_args.get('api_base', os.environ.get('OPENAI_API_BASE', OPENAI_API_BASE))
            org = connection_args.get('api_organization')
            client = self._get_client(api_key=api_key, base_url=api_base, org=org)
            OpenAIHandler._check_client_connection(client)

    @staticmethod
    def _check_client_connection(client: OpenAI):
        '''try to connect to api

        Args:
            client (OpenAI):

        Raises:
            Exception: if there is AuthenticationError
        '''
        try:
            client.models.retrieve('test')
        except NotFoundError:
            pass
        except AuthenticationError as e:
            if e.body['code'] == 'invalid_api_key':
                raise Exception('Invalid api key')
            raise Exception(f'Something went wrong: {e}')

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        if 'using' not in args:
            raise Exception(
                "OpenAI engine requires a USING clause! Refer to its documentation for more details."
            )
        else:
            args = args['using']

        if (
            len(
                set(args.keys())
                & {'question_column', 'prompt_template', 'json_struct', 'prompt'}
            )
            == 0
        ):
            raise Exception(
                'One of `question_column`, `prompt_template` or `json_struct` is required for this engine.'
            )

        keys_collection = [
            ['prompt_template'],
            ['question_column', 'context_column'],
            ['prompt', 'user_column', 'assistant_column'],
            ['json_struct'],
        ]
        for keys in keys_collection:
            if keys[0] in args and any(
                x[0] in args for x in keys_collection if x != keys
            ):
                raise Exception(
                    textwrap.dedent(
                        '''\
                    Please provide one of
                        1) a `prompt_template`
                        2) a `question_column` and an optional `context_column`
                        3) a `json_struct`
                        4) a `prompt' and 'user_column' and 'assistant_column`
                '''
                    )
                )

        # for all args that are not expected, raise an error
        known_args = set()
        # flatten of keys_collection
        for keys in keys_collection:
            known_args = known_args.union(set(keys))

        # TODO: need a systematic way to maintain a list of known args
        known_args = known_args.union(
            {
                "target",
                "model_name",
                "mode",
                "predict_params",
                "input_text",
                "ft_api_info",
                "ft_result_stats",
                "runtime",
                "max_tokens",
                "temperature",
                "openai_api_key",
                "api_organization",
                "api_base"
            }
        )

        unknown_args = set(args.keys()) - known_args
        if unknown_args:
            # return a list of unknown args as a string
            raise Exception(
                f"Unknown arguments: {', '.join(unknown_args)}.\n Known arguments are: {', '.join(known_args)}"
            )

        engine_storage = kwargs['handler_storage']
        api_key = get_api_key('openai', args, engine_storage=engine_storage)
        api_base = args.get('api_base', os.environ.get('OPENAI_API_BASE', OPENAI_API_BASE))
        org = args.get('api_organization')
        client = OpenAIHandler._get_client(api_key=api_key, base_url=api_base, org=org)
        OpenAIHandler._check_client_connection(client)

    def create(self, target, args=None, **kwargs):
        args = args['using']
        args['target'] = target
        try:
            api_key = get_api_key(self.name, args, self.engine_storage)
            available_models = get_available_models(api_key)

            if not args.get('mode'):
                args['mode'] = self.default_mode
            elif args['mode'] not in self.supported_modes:
                raise Exception(
                    f"Invalid operation mode. Please use one of {self.supported_modes}"
                )

            if not args.get('model_name'):
                if args['mode'] == 'image':
                    args['model_name'] = self.default_image_model
                else:
                    args['model_name'] = self.default_model
            elif args['model_name'] not in available_models:
                raise Exception(f"Invalid model name. Please use one of {available_models}")
        finally:
            self.model_storage.json_set('args', args)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        If there is a prompt template, we use it. Otherwise, we use the concatenation of `context_column` (optional) and `question_column` to ask for a completion.
        """  # noqa
        # TODO: support for edits, embeddings and moderation

        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        args['api_base'] = pred_args.get(
                    'api_base',
                    args.get(
                        'api_base', os.environ.get('OPENAI_API_BASE', OPENAI_API_BASE)
                    ))
        if pred_args.get('api_organization'):
            args['api_organization'] = pred_args['api_organization']
        df = df.reset_index(drop=True)

        if pred_args.get('mode'):
            if pred_args['mode'] in self.supported_modes:
                args['mode'] = pred_args['mode']
            else:
                raise Exception(
                    f"Invalid operation mode. Please use one of {self.supported_modes}."
                )  # noqa

        if pred_args.get('prompt_template', False):
            base_template = pred_args[
                'prompt_template'
            ]  # override with predict-time template if available
        elif args.get('prompt_template', False):
            base_template = args['prompt_template']
        else:
            base_template = None

        # Embedding Mode
        if args.get('mode', self.default_mode) == 'embedding':
            api_args = {
                'question_column': pred_args.get('question_column', None),
                'model': pred_args.get('model_name', 'text-embedding-ada-002'),
            }
            model_name = 'embedding'
            if args.get('question_column'):
                prompts = list(df[args['question_column']].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(
                    df[[args['question_column']]].isna().all(axis=1).values
                )[0]
            else:
                raise Exception('Embedding mode needs a question_column')

        # Image mode

        elif args.get('mode', self.default_mode) == 'image':
            api_args = {
                'n': pred_args.get('n', None),
                'size': pred_args.get('size', None),
                'response_format': pred_args.get('response_format', None),
            }
            api_args = {
                k: v for k, v in api_args.items() if v is not None
            }  # filter out non-specified api args
            model_name = args.get('model_name', 'dall-e-2')

            if args.get('question_column'):
                prompts = list(df[args['question_column']].apply(lambda x: str(x)))
                empty_prompt_ids = np.where(
                    df[[args['question_column']]].isna().all(axis=1).values
                )[0]
            elif args.get('prompt_template'):
                prompts, empty_prompt_ids = get_completed_prompts(base_template, df)
            else:
                raise Exception(
                    'Image mode needs either `prompt_template` or `question_column`.'
                )

        # Chat or normal completion mode
        else:
            if (
                args.get('question_column', False)
                and args['question_column'] not in df.columns
            ):
                raise Exception(
                    f"This model expects a question to answer in the '{args['question_column']}' column."
                )

            if (
                args.get('context_column', False)
                and args['context_column'] not in df.columns
            ):
                raise Exception(
                    f"This model expects context in the '{args['context_column']}' column."
                )

            # api argument validation
            model_name = args.get('model_name', self.default_model)
            api_args = {
                'max_tokens': pred_args.get(
                    'max_tokens', args.get('max_tokens', self.default_max_tokens)
                ),
                'temperature': min(
                    1.0,
                    max(
                        0.0, pred_args.get('temperature', args.get('temperature', 0.0))
                    ),
                ),
                'top_p': pred_args.get('top_p', None),
                'n': pred_args.get('n', None),
                'stop': pred_args.get('stop', None),
                'presence_penalty': pred_args.get('presence_penalty', None),
                'frequency_penalty': pred_args.get('frequency_penalty', None),
                'best_of': pred_args.get('best_of', None),
                'logit_bias': pred_args.get('logit_bias', None),
                'user': pred_args.get('user', None),
            }

            if (
                args.get('mode', self.default_mode) != 'default'
                and model_name not in self.chat_completion_models
            ):
                raise Exception(
                    f"Conversational modes are only available for the following models: {', '.join(self.chat_completion_models)}"
                )  # noqa

            if args.get('prompt_template', False):
                prompts, empty_prompt_ids = get_completed_prompts(base_template, df)

            elif args.get('context_column', False):
                empty_prompt_ids = np.where(
                    df[[args['context_column'], args['question_column']]]
                    .isna()
                    .all(axis=1)
                    .values
                )[0]
                contexts = list(df[args['context_column']].apply(lambda x: str(x)))
                questions = list(df[args['question_column']].apply(lambda x: str(x)))
                prompts = [
                    f'Context: {c}\nQuestion: {q}\nAnswer: '
                    for c, q in zip(contexts, questions)
                ]

            elif args.get('json_struct', False):
                empty_prompt_ids = np.where(
                    df[[args['input_text']]].isna().all(axis=1).values
                )[0]
                prompts = []
                for i in df.index:
                    if 'json_struct' in df.columns:
                        if isinstance(df['json_struct'][i], str):
                            df['json_struct'][i] = json.loads(df['json_struct'][i])
                        json_struct = ''
                        for ind, val in enumerate(df['json_struct'][i].values()):
                            json_struct = json_struct + f'{ind}. {val}\n'
                    else:
                        json_struct = ''
                        for ind, val in enumerate(args['json_struct'].values()):
                            json_struct = json_struct + f'{ind + 1}. {val}\n'

                    p = textwrap.dedent(
                        f'''\
                        Using text starting after 'The text is:', give exactly {len(args['json_struct'])} answers to the questions:
                        {{{{json_struct}}}}

                        Answers should be in the same order as the questions.
                        Each answer should start with a question number.
                        Each answer must end with new line.
                        If there is no answer to the question in the text, put a -.
                        Answers should be as short as possible, ideally 1-2 words (unless otherwise specified).

                        The text is:
                        {{{{{args['input_text']}}}}}
                    '''
                    )
                    p = p.replace('{{json_struct}}', json_struct)
                    for column in df.columns:
                        if column == 'json_struct':
                            continue
                        p = p.replace(f'{{{{{column}}}}}', str(df[column][i]))
                    prompts.append(p)
            elif 'prompt' in args:
                empty_prompt_ids = []
                prompts = list(df[args['user_column']])
            else:
                empty_prompt_ids = np.where(
                    df[[args['question_column']]].isna().all(axis=1).values
                )[0]
                prompts = list(df[args['question_column']].apply(lambda x: str(x)))

        # remove prompts without signal from completion queue
        prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]

        api_key = get_api_key(self.name, args, self.engine_storage)
        api_args = {
            k: v for k, v in api_args.items() if v is not None
        }  # filter out non-specified api args
        completion = self._completion(model_name, prompts, api_key, api_args, args, df)

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
                    responses = pred_df[args['target']][i].split('\n')
                    responses = [x[3:] for x in responses]  # del question index

                    pred_df[args['target']][i] = {
                        key: val for key, val in zip(json_keys, responses)
                    }
                except Exception:
                    pred_df[args['target']][i] = None

        return pred_df

    def _completion(
        self, model_name, prompts, api_key, api_args, args, df, parallel=True
    ):
        """
        Handles completion for an arbitrary amount of rows.

        There are a couple checks that should be done when calling OpenAI's API:
          - account max batch size, to maximize batch size first
          - account rate limit, to maximize parallel calls second

        Additionally, single completion calls are done with exponential backoff to guarantee all prompts are processed,
        because even with previous checks the tokens-per-minute limit may apply.
        """

        @retry_with_exponential_backoff()
        def _submit_completion(model_name, prompts, api_args, args, df):
            kwargs = {
                'model': model_name,
            }
            if model_name in IMAGE_MODELS:
                return _submit_image_completion(kwargs, prompts, api_args)
            elif model_name == 'embedding':
                return _submit_embedding_completion(kwargs, prompts, api_args)
            elif model_name in self.chat_completion_models:
                return _submit_chat_completion(
                    kwargs,
                    prompts,
                    api_args,
                    df,
                    mode=args.get('mode', 'conversational'),
                )
            else:
                return _submit_normal_completion(kwargs, prompts, api_args)

        def _log_api_call(params, response):
            after_openai_query(params, response)

            params2 = params.copy()
            params2.pop('api_key', None)
            params2.pop('user', None)
            logger.debug(f'>>>openai call: {params2}:\n{response}')

        def _submit_normal_completion(kwargs, prompts, api_args):
            def _tidy(comp):
                tidy_comps = []
                for c in comp.choices:
                    if hasattr(c,'text'):
                        tidy_comps.append(c.text.strip('\n').strip(''))
                return tidy_comps

            kwargs['prompt'] = prompts
            kwargs = {**kwargs, **api_args}

            before_openai_query(kwargs)
            resp = _tidy(client.completions.create(**kwargs))
            _log_api_call(kwargs, resp)
            return resp

        def _submit_embedding_completion(kwargs, prompts, api_args):
            def _tidy(comp):
                tidy_comps = []
                for c in comp.data:
                    if hasattr(c,'embedding'):
                        tidy_comps.append([c.embedding])
                return tidy_comps

            kwargs['input'] = prompts
            kwargs = {**kwargs, **api_args}

            before_openai_query(kwargs)
            resp = _tidy(client.embeddings.create(**kwargs))
            _log_api_call(kwargs, resp)
            return resp

        def _submit_chat_completion(
            kwargs, prompts, api_args, df, mode='conversational'
        ):
            def _tidy(comp):
                tidy_comps = []
                for c in comp.choices:
                    if hasattr(c,'message'):
                        tidy_comps.append(c.message.content.strip('\n').strip(''))
                return tidy_comps

            completions = []
            if mode != 'conversational':
                initial_prompt = {
                    "role": "system",
                    "content": "You are a helpful assistant. Your task is to continue the chat.",
                }  # noqa
            else:
                # get prompt from model
                initial_prompt = {"role": "system", "content": args['prompt']}  # noqa

            kwargs['messages'] = [initial_prompt]
            last_completion_content = None

            for pidx in range(len(prompts)):
                if mode != 'conversational':
                    kwargs['messages'].append(
                        {'role': 'user', 'content': prompts[pidx]}
                    )
                else:
                    question = prompts[pidx]
                    if question:
                        kwargs['messages'].append({'role': 'user', 'content': question})
                    answer = df.iloc[pidx][args.get('assistant_column')]
                    if answer:
                        kwargs['messages'].append(
                            {'role': 'assistant', 'content': answer}
                        )

                if mode == 'conversational-full' or (
                    mode == 'conversational' and pidx == len(prompts) - 1
                ):
                    kwargs['messages'] = truncate_msgs_for_token_limit(
                        kwargs['messages'], kwargs['model'], api_args['max_tokens']
                    )
                    pkwargs = {**kwargs, **api_args}

                    before_openai_query(kwargs)
                    resp = _tidy(client.chat.completions.create(**pkwargs))
                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                elif mode == 'default':
                    kwargs['messages'] = [initial_prompt] + [kwargs['messages'][-1]]
                    pkwargs = {**kwargs, **api_args}

                    before_openai_query(kwargs)
                    resp = _tidy(client.chat.completions.create(**pkwargs))
                    _log_api_call(pkwargs, resp)

                    completions.extend(resp)
                else:
                    # in "normal" conversational mode, we request completions only for the last row
                    last_completion_content = None
                    if args.get('answer_column') in df.columns:
                        # insert completion if provided, which saves redundant API calls
                        completions.extend([df.iloc[pidx][args.get('answer_column')]])
                    else:
                        completions.extend([''])

                if args.get('answer_column') in df.columns:
                    kwargs['messages'].append(
                        {
                            'role': 'assistant',
                            'content': df.iloc[pidx][args.get('answer_column')],
                        }
                    )
                elif last_completion_content:
                    # interleave assistant responses with user input
                    kwargs['messages'].append(
                        {'role': 'assistant', 'content': last_completion_content[0]}
                    )

            return completions

        def _submit_image_completion(kwargs, prompts, api_args):
            def _tidy(comp):
                return [
                    c.url if hasattr(c,'url')  else c.b64_json
                    for c in comp
                ]

            completions = [
                client.images.generate(**{'prompt': p, **kwargs, **api_args}).data[0]
                for p in prompts
            ]
            return _tidy(completions)
        

        client = self._get_client(
            api_key=api_key,
            base_url=args.get('api_base'),
            org=args.pop('api_organization') if 'api_organization' in args else None,
            )
        try:
            # check if simple completion works
            completion = _submit_completion(
                model_name, prompts, api_args, args, df
            )
            return completion
        except Exception as e:
            # else, we get the max batch size
            if 'you can currently request up to at most a total of' in str(e):
                pattern = 'a total of'
                max_batch_size = int(e[e.find(pattern) + len(pattern) :].split(').')[0])
            else:
                max_batch_size = (
                    self.max_batch_size
                )  # guards against changes in the API message

        if not parallel:
            completion = None
            for i in range(math.ceil(len(prompts) / max_batch_size)):
                partial = _submit_completion(
                    model_name,
                    prompts[i * max_batch_size : (i + 1) * max_batch_size],
                    api_args,
                    args,
                    df,
                )
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
                    logger.debug(
                        f'{i * max_batch_size}:{(i+1) * max_batch_size}/{len(prompts)}'
                    )
                    future = executor.submit(
                        _submit_completion,
                        model_name,
                        prompts[i * max_batch_size : (i + 1) * max_batch_size],
                        api_args,
                        args,
                        df,
                    )
                    promises.append({"choices": future})
            completion = None
            for p in promises:
                if not completion:
                    completion = p['choices'].result()
                else:
                    completion.extend(p['choices'].result())

        return completion

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        # TODO: Update to use update() artifacts

        args = self.model_storage.json_get('args')
        api_key = get_api_key(self.name, args, self.engine_storage)
        if attribute == 'args':
            return pd.DataFrame(args.items(), columns=['key', 'value'])
        elif attribute == 'metadata':
            model_name = args.get('model_name', self.default_model)
            try:
                client= self._get_client(
                    api_key=api_key,
                    base_url=args.get('api_base'),
                    org=args.get('api_organization')
                )
                meta = client.models.retrieve(model_name)
            except Exception as e:
                meta = {'error': str(e)}
            return pd.DataFrame(dict(meta).items(), columns=['key', 'value'])
        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])

    def finetune(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> None:
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
          - As base fine-tuning models, OpenAI only supports the original GPT ones: `ada`, `babbage`, `curie`, `davinci`. This means if you fine-tune successively more than once, any fine-tuning other than the most recent one is lost.
          - A bunch of helper methods exist to be overridden in other handlers that follow the OpenAI API, e.g. Anyscale
        """  # noqa

        args = args if args else {}

        api_key = get_api_key(self.name, args, self.engine_storage)

        using_args = args.pop('using') if 'using' in args else {}
        prompt_col = using_args.get('prompt_column', 'prompt')
        completion_col = using_args.get('completion_column', 'completion')
        
        api_base = using_args.get('api_base', os.environ.get('OPENAI_API_BASE', OPENAI_API_BASE))
        org = using_args.get('api_organization')
        client = self._get_client(api_key=api_key, base_url=api_base, org=org)


        args = {**using_args, **args}
        prev_model_name = self.base_model_storage.json_get('args').get('model_name', '')

        if prev_model_name not in self.supported_ft_models:
            # base model may be already FTed, check prefixes
            for model in self.supported_ft_models:
                if model in prev_model_name:
                    break
            else:
                raise Exception(
                    f"This model cannot be finetuned. Supported base models are {self.supported_ft_models}."
                )

        finetune_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        temp_storage_path = tempfile.mkdtemp()
        temp_file_name = f"ft_{finetune_time}"
        temp_model_storage_path = f"{temp_storage_path}/{temp_file_name}.jsonl"

        file_names = self._prepare_ft_jsonl(
            df, temp_storage_path, temp_file_name, temp_model_storage_path
        )

        jsons = {k: None for k in file_names.keys()}
        for split, file_name in file_names.items():
            if os.path.isfile(os.path.join(temp_storage_path, file_name)):
                jsons[split] = client.files.create(file=open(f"{temp_storage_path}/{file_name}", "rb"),
                purpose='fine-tune')

        if type(jsons['train']) is openai.types.FileObject:
            train_file_id = jsons['train'].id
        else:
            train_file_id = jsons['base'].id

        if type(jsons['val']) is openai.types.FileObject:
            val_file_id = jsons['val'].id
        else:
            val_file_id = None

        # `None` values are internally imputed by OpenAI to `null` or default values
        ft_params = {
            'training_file': train_file_id,
            'validation_file': val_file_id,
            'model': self._get_ft_model_type(prev_model_name),
        }
        ft_params = self._add_extra_ft_params(ft_params, using_args)

        start_time = datetime.datetime.now()

        ft_stats, result_file_id = self._ft_call(ft_params, client, args.get('hour_budget', 8))
        ft_model_name = ft_stats.fine_tuned_model

        end_time = datetime.datetime.now()
        runtime = end_time - start_time
        name_extension = client.files.retrieve(file_id=result_file_id).filename
        result_path = f'{temp_storage_path}/ft_{finetune_time}_result_{name_extension}'

        try:
            client.files.content(file_id=result_file_id).stream_to_file(result_path)
            if '.csv' in name_extension:
                # legacy endpoint
                train_stats = pd.read_csv(result_path)
                if 'validation_token_accuracy' in train_stats.columns:
                    train_stats = train_stats[
                        train_stats['validation_token_accuracy'].notnull()
                    ]
                args['ft_api_info'] = ft_stats.dict()
                args['ft_result_stats'] = train_stats.to_dict()

            elif '.json' in name_extension:
                train_stats = pd.read_json(
                    path_or_buf=result_path, lines=True
                )  # new endpoint
                args['ft_api_info'] = args['ft_result_stats'] = train_stats.to_dict()

        except Exception:
            logger.info(f'Error retrieving fine-tuning results. Please check manually for information on job {ft_stats.id} (result file {result_file_id}).')

        args['model_name'] = ft_model_name
        args['runtime'] = runtime.total_seconds()
        args['mode'] = self.base_model_storage.json_get('args').get(
            'mode', self.default_mode
        )

        self.model_storage.json_set('args', args)
        shutil.rmtree(temp_storage_path)

    @staticmethod
    def _prepare_ft_jsonl(df, _, temp_filename, temp_model_path):
        df.to_json(temp_model_path, orient='records', lines=True)

        # TODO avoid subprocess usage once OpenAI enables non-CLI access, or refactor to use our own LLM utils instead
        subprocess.run(
            [
                "openai",
                "tools",
                "fine_tunes.prepare_data",
                "-f",
                temp_model_path,  # from file
                '-q',  # quiet mode (accepts all suggestions)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )

        file_names = {
            'original': f'{temp_filename}.jsonl',
            'base': f'{temp_filename}_prepared.jsonl',
            'train': f'{temp_filename}_prepared_train.jsonl',
            'val': f'{temp_filename}_prepared_valid.jsonl',
        }
        return file_names

    def _get_ft_model_type(self, model_name: str):
        for model_type in self.supported_ft_models:
            if model_type in model_name.lower():
                return model_type
        return 'babbage-002'

    @staticmethod
    def _add_extra_ft_params(ft_params, using_args):
        extra_params = {
            'n_epochs': using_args.get('n_epochs', None),
            'batch_size': using_args.get('batch_size', None),
            'learning_rate_multiplier': using_args.get(
                'learning_rate_multiplier', None
            ),
            'prompt_loss_weight': using_args.get('prompt_loss_weight', None),
            'compute_classification_metrics': using_args.get(
                'compute_classification_metrics', None
            ),
            'classification_n_classes': using_args.get(
                'classification_n_classes', None
            ),
            'classification_positive_class': using_args.get(
                'classification_positive_class', None
            ),
            'classification_betas': using_args.get('classification_betas', None),
        }
        return {**ft_params, **extra_params}

    def _ft_call(self, ft_params, client, hour_budget):
        """
        Separate method to account for both legacy and new endpoints.
        Currently, `OpenAIHandler` uses the legacy endpoint.
        Others, like `AnyscaleEndpointsHandler`, use the new endpoint.
        """
        ft_result = client.fine_tuning.jobs.create(
            **{k: v for k, v in ft_params.items() if v is not None}
        )

        @retry_with_exponential_backoff(
            hour_budget=hour_budget,
        )
        def _check_ft_status(model_id):
            ft_retrieved = client.fine_tuning.jobs.retrieve(fine_tuning_job_id=model_id)
            if ft_retrieved.status in ('succeeded', 'failed', 'cancelled'):
                return ft_retrieved
            else:
                raise PendingFT('Fine-tuning still pending!')

        ft_stats = _check_ft_status(ft_result.id)

        if ft_stats.status != 'succeeded':
            err_message = ft_stats.events[-1].message if hasattr(ft_stats, 'events') else 'could not retrieve!'
            ft_status = ft_stats.status if hasattr(ft_stats, 'status') else 'N/A'
            raise Exception(
                f"Fine-tuning did not complete successfully (status: {ft_status}). Error message: {err_message}"
            )  # noqa

        result_file_id = client.fine_tuning.jobs.retrieve(fine_tuning_job_id=ft_result.id).result_files[0]
        if hasattr(result_file_id, 'id'):
            result_file_id = result_file_id.id  # legacy endpoint

        return ft_stats, result_file_id
    
    @staticmethod
    def _get_client(api_key, base_url=OPENAI_API_BASE, org=None):
        return OpenAI(api_key=api_key, base_url=base_url, organization=org)
