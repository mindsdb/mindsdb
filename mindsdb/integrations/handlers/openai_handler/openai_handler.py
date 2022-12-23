import os
import re
import pandas as pd
from typing import Optional

import openai

from mindsdb.integrations.libs.base import BaseMLEngine


class OpenAIHandler(BaseMLEngine):
    name = 'openai'

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

        if not args.get('api_key', False) or args['api_key'].lower() == 'env':
            args['api_key'] = os.getenv('OPENAI_API_KEY')

        # TODO: find a way to pass a prompt template from here (to avoid repeated definition at prediction time)
        #  and inject in predict()... maybe just regex-match the pattern and replace?
        # if not args.get('prompt_template', False): args['prompt_template'] = 'Q: {input_col}\nA: {output_col}'

        args['target'] = target
        self.model_storage.json_set('args', args)

    def predict(self, df, args=None):
        """
        If there is a prompt template, we use it. Otherwise, we use the concatenation of `context_column` (optional) and `question_column` to ask for a completion.
        """ # noqa
        # TODO: support for edits, embeddings and moderation

        def _tidy(comp):
            return [c['text'].strip('\n').strip('') for c in comp['choices']]

        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')

        if args.get('question_column', False) and args['question_column'] not in df.columns:
            raise Exception(f"This model expects a question to answer in the '{args['question_column']}' column.")

        if args.get('context_column', False) and args['context_column'] not in df.columns:
            raise Exception(f"This model expects context in the '{args['context_column']}' column.")

        model_name = args.get('model_name', 'text-davinci-002')
        temperature = min(1.0, max(0.0, args.get('temperature', 0.0)))
        max_tokens = pred_args.get('max_tokens', args.get('max_tokens', 20))

        openai.api_key = args['api_key']
        if args.get('api_organization', False):
            openai.organization = args['api_organization']

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

            df['__mdb_prompt'] = ''
            for i in range(len(template)):
                atom = template[i]
                if i < len(columns):
                    col = df[columns[i]]
                    df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom + col)
                else:
                    df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom)
            prompts = list(df['__mdb_prompt'])

        elif args.get('context_column', False):
            contexts = list(df[args['context_column']].apply(lambda x: str(x)))
            questions = list(df[args['question_column']].apply(lambda x: str(x)))
            prompts = [f'Context: {c}\nQuestion: {q}\nAnswer: ' for c, q in zip(contexts, questions)]

        else:
            prompts = list(df[args['question_column']].apply(lambda x: str(x)))

        completion = openai.Completion.create(
            model=model_name,
            prompt=prompts,
            max_tokens=max_tokens,
            temperature=temperature
        )

        output = _tidy(completion)
        pred_df = pd.DataFrame(output, columns=[args['target']])
        return pred_df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get('args')
        meta = openai.Model.retrieve(args['model_name'])

        return pd.DataFrame([[meta['id'], meta['object'], meta['owned_by'], meta['permission'], args]],
                            columns=['id', 'object', 'owned_by', 'permission', 'model_args'])
