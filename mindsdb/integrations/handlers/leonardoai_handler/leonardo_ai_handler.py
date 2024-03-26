import os
import requests
import time
from typing import Dict, Optional

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.libs.llm.utils import get_completed_prompts
from mindsdb.utilities import log
from mindsdb.interfaces.storage.model_fs import HandlerStorage
from mindsdb.utilities.config import Config

logger = log.getLogger(__name__)

LEONARDO_API_BASE = 'https://cloud.leonardo.ai/api/rest/v1'


class LeonardoAIHandler(BaseMLEngine):
    """
    This integration seamlessly combines MindsDB and Leonardo AI to create a powerful
    AI-driven solution for creative content generation.

    Content Generation with Leonardo AI: Harness the power of advanced generative
    models for creative content production. From realistic images to artistic text, Leonardo
    AI opens up new possibilities for content creators.
    """
    name = "leonardo_ai"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_models = []
        self.default_model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3'
        self.base_api = LEONARDO_API_BASE
        self.rate_limit = 50
        self.max_batch_size = 5  # default value

    def create(self, target: str, args=None, **kwargs):

        args['target'] = target

        if "using" not in args:
            raise Exception(
                "Leornardo Engine requires a USING clause!"
            )

        self.model_storage.json_set("args", args)
        api_key = self._get_leonardo_api_key(args, self.engine_storage)  # fetch api key

        # check if API key is valid
        self.connection = requests.get(
            "https://cloud.leonardo.ai/api/rest/v1/me",
            headers={
                "accept": "application/json",
                "authorization": f"Bearer {api_key}"
            }
        )

        # if valid, check if the model is valid
        try:
            if (self.connection.status_code == 200):
                # get all the available models
                available_models = self._get_platform_model(args)
                if not args['using']['model']:
                    args['using']['model'] = self.default_model
                elif args['using']['model'] not in available_models:    # if invalid model_id is provided
                    raise Exception("Invalid Model ID. Please use a valid Model")
            else:
                raise Exception("Unable to make connection, please verify the API key.")
        except Exception:
            raise Exception("Auth Connection Error, please the modelId or API key")

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None, **kwargs) -> pd.DataFrame:

        pred_args = args['predict_params'] if args else {}
        args = self.model_storage.json_get("args")

        prompt_template = pred_args.get('prompt_template', args.get('prompt_template', 'Generate a picture of {{{{text}}}}'))

        # prepare prompts
        prompts, empty_prompt_id = get_completed_prompts(prompt_template, df)
        df['__mdb_prompt'] = prompts

        # generate picture based on the given prompt
        result_df = pd.DataFrame(self.predict_answer(prompts))
        return result_df

    def _get_leonardo_api_key(self, args, engine_storage: HandlerStorage, strict=True):
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
                'Missing API key "api_key". Either re-create this ML_ENGINE specifying the `api_key` parameter,\
                 or re-create this model and pass the API key with `USING` syntax.'
            )

    def _get_platform_model(self, args):
        """
        Returns a list of available model based on the API key provided
        """
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

        models = self.connection.json()

        # extract the model ids from the response
        model_ids = [model['id'] for model in models['custom_models']]

        return model_ids

    def predict_answer(self, prompts, **kwargs):
        """
        Generates pictures based on the prompts and returns URLs with few variations.

        Request Flow:
            - POST request with a prompt is sent
            - `generation_id` is created for the request
            - POST request will take couple of seconds to generate the picture, till then the process will be kept busy with a simple math calculation.
            - New GET request with the `generation_id` will fetch the generated pictures as URLs
        """
        args = self.model_storage.json_get('args')
        tmp_args = args['using']
        height = tmp_args.get('height', 512)
        width = tmp_args.get('width', 512)
        api_key = self._get_leonardo_api_key(args, self.engine_storage)  # fetch API key
        generation_id = ''

        # Endpoint URL
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

        # payload
        generation_payload = {
            "height": height,
            "modelId": args['using']['model'],
            "prompt": f"{prompts}",
            "width": width,
        }

        # Make a POST request to generate the image
        response_generation = requests.post(generation_url, json=generation_payload, headers=post_headers)
        generation_data = response_generation.json()

        # Wait for 15 seconds

        time.sleep(15)

        # extract generationID from the response
        generation_id = generation_data['sdGenerationJob']['generationId']

        # ENDPOINT GET URL
        retrieve_url = f"https://cloud.leonardo.ai/api/rest/v1/generations/{generation_id}"

        # GET request to retrieve image URLs
        response_retrieve = requests.get(retrieve_url, headers=get_headers)
        retrieve_data = response_retrieve.json()

        # extract URLs from the response
        generated_images = retrieve_data["generations_by_pk"]["generated_images"]
        image_urls = [image["url"] for image in generated_images]

        url_dicts = []

        for url in image_urls:
            url_dicts.append({'url': url})

        img_urls = pd.DataFrame(url_dicts, columns=['url'])
        return img_urls

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        model, target = args['using']['model'], args['target']
        prompt_template = args.get('prompt_template', 'Generate a picture of {{{{text}}}}')

        if attribute == "features":
            return pd.DataFrame([[target, prompt_template]], columns=['target_column', 'mindsdb_prompt_template'])
        elif attribute == "metadata":
            api_key = self._get_leonardo_api_key(args, self.engine_storage)
            return pd.DataFrame([[target, api_key, model]], columns=['target', 'api_key', 'model_name'])
        else:
            tables = ['args', 'api_key']
            return pd.DataFrame(tables, columns=['tables'])
