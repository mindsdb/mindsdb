import os
import json
import contextlib
from pandas.core.api import DataFrame as DataFrame
import requests
import time
from mindsdb_sql.parser import ast
from typing import Dict, Optional
from mindsdb_sql.planner.utils import query_traversal
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.libs.llm_utils import get_completed_prompts

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)

LEONARDO_API_BASE = 'https://cloud.leonardo.ai/api/rest/v1'

class LeonardoAIHandler(BaseMLEngine):
    """
    Integration with Leonardo AI
    """
    
    name = "leonardoai"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_models = []
        self.default_model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3'
        self.base_api = LEONARDO_API_BASE
        self.rate_limit = 50
        self.max_batch_size = 5
            
    def create(self, target: str, args=None, **kwargs):
        
        # args = args['using']
        # args['target'] = target
        
        print("args: ", args)
        if "using" not in args:
            raise Exception(
                "Leornardo Engine requires a USING clause!"
            )
    
        # elif args["using"]["model"] not in self._get_platform_model(args):
        #     raise Exception("Leornardo Engine requires a valid model")
        print("ARGS B: ", args)
        self.model_storage.json_set("args", args)
        
        available_models = self._get_platform_model(args)
        print("ARGS: ", args)
        
        if not args['using']['model']:
            args['using']['model'] = self.default_model
        elif args['using']['model'] not in available_models:
            raise Exception(f"Invalid model name. Please use a valid Model")
        
    @staticmethod
    @contextlib.contextmanager
    def create_validation(self, args=None, **kwargs):
        if 'using' not in args:
            raise Exception("LangChain engine requires a USING clause!")
        else:
            args = args['using']
    
    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None, **kwargs) -> pd.DataFrame:
        
        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get("args")
        api_key = self._get_leonardo_api_key(args, self.engine_storage)
        print("PRED_ARGS: ", pred_args)
        prompt_template = pred_args.get('prompt_template', args.get('prompt_template', 'Generate a picture of {{{{text}}}}'))
        
        # prepare prompts
        prompts, empty_prompt_id = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts
        print(args.values())
        
        print("PROMPTS: ", prompts)
        
        # input_column = args["using"]["text"]
        # print("Testing print: ", input_column)
        
        self.connection = requests.get(
            "https://cloud.leonardo.ai/api/rest/v1/me", 
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {api_key}"
            }
        )
        
        print(self.connection.json)
        
        result_df = pd.DataFrame(self.predict_answer(prompts))
        
        # result_df['predictions'] = self.predict_answer(args)
        
        # input_column = args["using"]["text"]
        
        # if input_column not in df.columns:
        #     raise RuntimeError(f'Column "{input_column}" not found in input data')

        # result_df = pd.DataFrame()
        
        # result_df["predictions"] = df[input_column].apply(self.predict_answer)
        
        # result_df = result_df.rename(columns={"predictions": args["target"]})
        print(result_df)
        return result_df
        
    def _get_leonardo_api_key(self, args, engine_storage:HandlerStorage, strict=True):
        """
        API_KEY preference order:
            1. provided at model creation
            2. provided at engine creation
            3. LEONARDO_API_KEY env variable
            4. leonardo.api_key setting in config.json
        """

        # 1
        if "api_key" in args["using"]:
            return args["using"]["api_key"]
        # 2
        connection_args = engine_storage.get_connection_args()
        if "api_key" in connection_args:
            return connection_args["api_key"]
        # 3
        api_key = os.getenv("LEONARDO_API_KEY")
        if api_key is not None:
            return api_key
        # 4
        config = Config()
        leonardo_cfg = config.get("leonardo", {})
        if "api_key" in leonardo_cfg:
            return leonardo_cfg["api_key"]

        if strict:
            raise Exception(
                f'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )
            
    def _get_platform_model(self, args):
        model_ids = []
        
        args = self.model_storage.json_get('args')
        api_key = self._get_leonardo_api_key(args, self.engine_storage)
        
        self.connection = requests.get(
            "https://cloud.leonardo.ai/api/rest/v1/platformModels", 
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {api_key}"
            }
        )
        print(self.connection.json())
        
        models = self.connection.json()
        
        model_ids = [model['id'] for model in models['custom_models']]
        
        return model_ids
            
    def predict_answer(self, prompts, **kwargs):
        
        args = self.model_storage.json_get('args')
        print(f"Args: {args}")
        api_key = self._get_leonardo_api_key(args, self.engine_storage)
        print(f"API key: {api_key}")
        generation_id = ''
        model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3',
        # prompt = 'Oil Painting of a dog' # text
        # Endpoint URLs
        generation_url = "https://cloud.leonardo.ai/api/rest/v1/generations"
        
        post_headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {api_key}"
        }
        
        get_headers = {
            "accept": "application/json",
            "authorization": f"Bearer {api_key}"
        }
        
        generation_payload = {
            "height": 512,
            "modelId": args['using']['model'],
            "prompt": f"{prompts}",
            "width": 512
        }
        
        print(generation_payload)
        
        # Make a POST request to generate the image
        response_generation = requests.post(generation_url, json=generation_payload, headers=post_headers)
        print(response_generation.text)
        generation_data = response_generation.json()
        print(generation_data)
        
        # Wait for 5 seconds
        
        # Set the desired duration in seconds
        duration = 15

        # Record the start time
        start_time = time.time()

        # Run a busy loop for the specified duration
        while time.time() - start_time < duration:
            # Perform some computation or operation here
            # Perform some lightweight computation to keep the program busy
            # For example, calculate the sum of numbers
            result = 0
            for i in range(100000):
                result += i
        
        generation_id = generation_data['sdGenerationJob']['generationId']
        print(generation_id)
        retrieve_url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"
        print(retrieve_url)
        response_retrieve = requests.get(retrieve_url, headers=get_headers)
        print(response_retrieve.json())
        retrieve_data = response_retrieve.json()
        print(retrieve_data)
        print(response_retrieve.raw)
        print(response_retrieve.text)
        
        generated_images = retrieve_data["generations_by_pk"]["generated_images"]
        print(generated_images)
        image_urls = [image["url"] for image in generated_images]
        
        url_dicts = []
        
        for url in image_urls:
            url_dicts.append({'url': url})
            print(url)
            
        print("\nDict : ", url_dicts)
        img_urls = pd.DataFrame(url_dicts, columns=['url'])
        return img_urls
    
    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        model, target = args['model'], args['target']
        prompt_template = args.get('prompt_template', 'Generate a picture of {{{{text}}}}')
        
        if attribute == "features":
            return pd.DataFrame([[target, prompt_template]], columns=['target_column', 'mindsdb_prompt_template'])
        
        else:
            pass