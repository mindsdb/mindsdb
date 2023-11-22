import json
import os
from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine


class TwelveLabsHandler(BaseMLEngine):
    """
    Integration with the Twelve Labs API.
    """

    name = 'twelve_labs'

    @staticmethod
    def create_validation(target, args=None, **kwargs):
        pass

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass