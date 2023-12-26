import os
import json
import contextlib
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
        pass
            
        