import os
import json
import contextlib
import requests
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
    
    name = "leonardo ai"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_models = []
        self.default_model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3'
        self.base_api = LEONARDO_API_BASE
        self.rate_limit = 50
        self.max_batch_size = 5
        
    @staticmethod
    @contextlib.contextmanager
    def _leonardo_base_api(key='LEONARDO_API_BASE'):
        os.environ['LEONARDO_API_BASE'] = LEONARDO_API_BASE
        try:
            yield
        except KeyError:
            logger.exception('Error getting API key')
            
    def create(self, target: str, args=None, **kwargs):
        if "using" not in args:
            raise Exception(
                "Leornardo Engine requires a USING clause!"
            )
            
        if "model" not in args["using"]:
            args["using"]["model"] = self.default_model
            
            # TODO: if using does not contain valid model throw exception
        elif args["using"]["model"] not in self.supported_chat_models:
            raise Exception(
                f"Invalid model. Please use one of {self.supported_chat_models}"
            )
        
        self.model_storage.json_set("args", args)
        
    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        
        args = self.model_storage.json_get("args")
        api_key = self._get_leonardo_api_key(args)
        
        self.connection = requests.get(
            "https://cloud.leornardo.ai/api/rest/v1/me", 
            headers={"accept: application/json"}
        )
        
        input_column = args["using"]["column"]
        
        if input_column not in df.columns:
            raise RuntimeError(f'Column "{input_column}" not found in input data')

        result_df = pd.DataFrame()
        
        result_df["predictions"] = df[input_column].apply(self.predict_answer)
        
        result_df = result_df.rename(columns={"predictions": args["target"]})

        return result_df
        