import os
import json
import contextlib
import requests
import time
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
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
        
    # @staticmethod
    # @contextlib.contextmanager
    # def _leonardo_base_api(key='LEONARDO_API_BASE'):
    #     os.environ['LEONARDO_API_BASE'] = LEONARDO_API_BASE
    #     try:
    #         yield
    #     except KeyError:
    #         logger.exception('Error getting API key')
            
    def create(self, target: str, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "Leornardo Engine requires a USING clause!"
            )
            
        if "model" not in args["using"]:
            args["using"]["model"] = self.default_model
        
        self.model_storage.json_set("args", args)
        
    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        
        args = self.model_storage.json_get("args")
        api_key = self._get_leonardo_api_key(args)
        
        self.connection = requests.get(
            "https://cloud.leonardo.ai/api/rest/v1/me", 
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {api_key}"
            }
        )
        
        print(self.connection.status_code)
        
        result_df = pd.DataFrame()
        
        result_df['predictions'] = self.predict_answer
        
        # input_column = args["using"]["text"]
        
        # if input_column not in df.columns:
        #     raise RuntimeError(f'Column "{input_column}" not found in input data')

        # result_df = pd.DataFrame()
        
        # result_df["predictions"] = df[input_column].apply(self.predict_answer)
        
        # result_df = result_df.rename(columns={"predictions": args["target"]})

        return result_df
        
    def _get_leonardo_api_key(self, args, strict=True):
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
        connection_args = self.engine_storage.get_connection_args()
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
            
    def predict_answer(self):
        
        args = self.model_storage.json_get("args")
        
        api_key = self._get_leonardo_api_key(args)
        generation_id = ''
        model = args["using"]["model"],
        prompt = 'cute cat picture' # text
        
        # Endpoint URLs
        generation_url = "https://cloud.leonardo.ai/api/rest/v1/generations"
        retrieve_url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"
        
        post_headers = {
            "accept": "application/json",
            "authorization": f"Bearer {api_key}",
            "content-type": "application/json",
        }
        
        get_headers = {
            "accept": "application/json",
            "authorization": f"Bearer {api_key}"
        }
        
        generation_payload = {
            "height": 512,
            "modelId": f"{model}",
            "prompt": f"{prompt}",
            "width": 512,
        }
        
        # Make a POST request to generate the image
        response_generation = requests.post(generation_url, headers=post_headers, json=generation_payload)
        generation_data = response_generation.json()
        
        # Wait for 5 seconds
        
        time.sleep(5)
        
        generation_id = generation_data.get("generationId")
        response_retrieve = requests.get(retrieve_url, headers=get_headers)
        retrieve_data = response_retrieve.json()
        
        generated_images = retrieve_data["generations_by_pk"]["generated_images"]
        
        image_urls = [image["url"] for image in generated_images]
        
        for url in image_urls:
            return url