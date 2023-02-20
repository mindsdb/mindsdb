import sys
import json
import copy
from typing import Optional, Dict
from datetime import datetime
from typing import Optional
import pandas as pd
from typing import Optional

import dill
import numpy as np

import mindsdb.interfaces.storage.db as db

from mindsdb.utilities.functions import cast_row_types
# from mindsdb.utilities.hooks import after_predict as after_predict_hook
from mindsdb.interfaces.model.functions import get_model_record
from mindsdb.interfaces.storage.json import get_json_storage
from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log

from mindsdb.integrations.libs.base import BaseMLEngine

from autogluon.tabular import TabularDataset, TabularPredictor


class AutoGluonHandler(BaseMLEngine):
    name = 'autogluon'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args=None, **kwargs) -> None:
        if 'using' in args:
            args = args['using']

        model_name = args['model_name']

        # store and persist in model folder
        self.model_storage.json_set('args', args)

        # persist changes to handler folder
        self.engine_storage.folder_sync(model_name)

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:
        pass

    def update(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        pass


    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:
        args = self.model_storage.json_get('args')
        return pd.DataFrame([[args]], columns=['model_args'])
    def create_engine(self, connection_args: dict):
        pass
