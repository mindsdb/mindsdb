import os
from typing import Dict, Optional

from PIL import Image
import requests
import numpy as np
from io import BytesIO
import json
import textwrap
import google.generativeai as genai
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.integrations.libs.llm_utils import generate_llm_prompts, validate_args, pred_time_args
import concurrent.futures




logger = log.getLogger(__name__)


class GoogleGeminiHandler(BaseMLEngine):
    """
    Integration with the Google generative AI Python Library
    """

    name = "google_gemini"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_model = "gemini-pro"
        self.default_embedding_model = 'models/embedding-001'
        self.generative = True
        self.mode = 'default'
        self.supported_modes = [
            'default',
            # 'conversational',
            # 'conversational-full',
            'image',
            'embedding',
        ]

    # Similiar to openai handler
    @staticmethod
    def create_validation(target, args=None, **kwargs):
        required_keys = ['img_url', 'input_text', 'question_column', 'prompt_template', 'json_struct', 'prompt']
        extra_keys= {"target","model_name","mode",'title_column',"predict_params","type","max_tokens","temperature","api_key",}
        key_collection = [
            ['prompt_template'],
            ['question_column', 'context_column'],
            ['prompt', 'user_column', 'assistant_column'],
            ['json_struct', 'input_text'],
            ['img_url', 'ctx_column']
        ]

        # Call the validation function
        validate_args(args, required_keys, key_collection, extra_keys)

    def create(self, target, args=None, **kwargs):
        args = args['using']
        args['target'] = target
        self.model_storage.json_set('args', args)

    def predict(
        self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None
    ) -> pd.DataFrame:
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get('args')
        df = df.reset_index(drop=True)

        # same as opeani handler for getting prompt template and mode
        args = pred_time_args(args, pred_args, self.supported_modes)
        base_template = args.get('prompt_template', None)

        # Embedding Mode
        if args.get('mode') == 'embedding':
            args['type'] = pred_args.get('type', 'query')
            return self.embedding_worker(args, df)

        elif args.get('mode') == 'vision':
            return self.vision_worker(args, df)

        elif args.get('mode') == 'conversational':
            # Enable chat mode using
            # https://ai.google.dev/tutorials/python_quickstart#chat_conversations
            # OR
            # https://github.com/google/generative-ai-python?tab=readme-ov-file#developers-who-use-the-palm-api
            pass

        else:
            if args.get('json_struct', False):
                json_prompt = textwrap.dedent(
                        f'''\
                        Using text starting after 'The text is:', give exactly {len(args['json_struct'])} answers to the questions:
                        {{{{json_struct}}}}

                        Answers should be in the same order as the questions.
                        Answer should be in form of one JSON Object eg. {"{'key':'value',..}"} where key=question and value=answer.
                        If there is no answer to the question in the text, put a -.
                        Answers should be as short as possible, ideally 1-2 words (unless otherwise specified).

                        The text is:
                        {{{{{args['input_text']}}}}}
                    '''
                    )
            else:
                json_prompt=None
            prompts, _ = generate_llm_prompts(df, args, base_template, json_prompt)
            
        api_key = self._get_google_gemini_api_key(args)
        genai.configure(api_key=api_key)

        # called gemini model withinputs
        model = genai.GenerativeModel(
            args.get('model_name', self.default_model)
        )
        results = []
        for m in prompts:
            results.append(model.generate_content(m).text)

        pred_df = pd.DataFrame(results, columns=[args['target']])
        return pred_df

    def _get_google_gemini_api_key(self, args, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. GOOGLE_GENAI_API_KEY env variable
            4. google_gemini.api_key setting in config.json
        """

        if "api_key" in args:
            return args["api_key"]
        # 2
        connection_args = self.engine_storage.get_connection_args()
        if "api_key" in connection_args:
            return connection_args["api_key"]
        # 3
        api_key = os.getenv("GOOGLE_GENAI_API_KEY")
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        google_gemini_config = config.get("google_gemini", {})
        if "api_key" in google_gemini_config:
            return google_gemini_config["api_key"]

        if strict:
            raise Exception(
                'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )

    def embedding_worker(self, args: Dict, df: pd.DataFrame):
        if args.get('question_column'):
            prompts = list(df[args['question_column']].apply(lambda x: str(x)))
            if args.get('title_column', None):
                titles = list(df[args['title_column']].apply(lambda x: str(x)))
            else:
                titles = None

            api_key = self._get_google_gemini_api_key(args)
            genai.configure(api_key=api_key)
            model_name = args.get('model_name', self.default_embedding_model)
            task_type = args.get('type')
            task_type = f'retrieval_{task_type}'

            if task_type == 'retrieval_query':
                results = [str(genai.embed_content(model=model_name, content=query, task_type=task_type)['embedding']) for query in prompts]
            elif titles:
                results = [str(genai.embed_content(model=model_name, content=doc, task_type=task_type, title=title)['embedding']) for title, doc in zip(titles, prompts)]
            else:
                results = [str(genai.embed_content(model=model_name, content=doc, task_type=task_type)['embedding']) for doc in prompts]

            pred_df = pd.DataFrame(results, columns=[args['target']])
            return pred_df
        else:
            raise Exception('Embedding mode needs a question_column')

    def vision_worker(self, args: Dict, df: pd.DataFrame):
        def get_img(url):
            # URL Validation
            response = requests.get(url)
            if response.status_code == 200 and response.headers.get('content-type', '').startswith('image/'):
                return Image.open(BytesIO(response.content))
            else:
                raise Exception(f"{url} is not vaild image URL..")
                
        if args.get('img_url'):
            urls = list(df[args['img_url']].apply(lambda x: str(x)))

        else:
            raise Exception('Vision mode needs a img_url')

        prompts = None
        if args.get('ctx_column'):
            prompts = list(df[args['ctx_column']].apply(lambda x: str(x)))

        api_key = self._get_google_gemini_api_key(args)
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-pro-vision')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Download images concurrently using ThreadPoolExecutor
            imgs = list(executor.map(get_img, urls))
        # imgs = [Image.open(BytesIO(requests.get(url).content)) for url in urls]
        if prompts:
            results = [model.generate_content([img, text]).text for img, text in zip(imgs, prompts)]
        else:
            results = [model.generate_content(img).text for img in imgs]

        pred_df = pd.DataFrame(results, columns=[args['target']])

        return pred_df

    # Disclaimer: The following code has been adapted from the OpenAI handler.
    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get('args')

        if attribute == 'args':
            return pd.DataFrame(args.items(), columns=['key', 'value'])
        elif attribute == 'metadata':
            api_key = self._get_google_gemini_api_key(args)
            genai.configure(api_key=api_key)
            model_name = args.get('model_name', self.default_model)

            meta = genai.get_model(f'models/{model_name}').__dict__
            return pd.DataFrame(meta.items(), columns=['key', 'value'])
        else:
            tables = ['args', 'metadata']
            return pd.DataFrame(tables, columns=['tables'])
