import pandas as pd
from typing import Optional, Dict

from mindsdb.utilities import log
from mindsdb.integrations.libs.base import BaseMLEngine


logger = log.getLogger(__name__)


class AmazonBedrockHandler(BaseMLEngine):
    """
    This handler handles connection and inference with the Amazon Bedrock API.
    """

    name = 'bedrock'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    @staticmethod
    def create_validation(target: str, args: Dict = None, **kwargs: Dict) -> None:
        pass

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        pass