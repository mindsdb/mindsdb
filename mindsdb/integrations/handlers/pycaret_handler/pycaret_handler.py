from typing import Optional, Dict

import pandas as pd
from pycaret.classification import setup, compare_models, evaluate_model, predict_model, finalize_model, save_model, load_model
# from pycaret.regression import setup, compare_models, evaluate_model, predict_model, finalize_model

from mindsdb.integrations.libs.base import BaseMLEngine


class PyCaretHandler(BaseMLEngine):
    name = 'pycaret'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None):
        grid = setup(data=df, target=target)

        best_model = compare_models()

        final_model = finalize_model(best_model)

        model_storage_path = self.engine_storage.folder_get('pycaret_model')
        save_model(final_model, f"{model_storage_path}/classification_model")

        self.model_storage.json_set('model_storage_path', "model_storage_path")

        self.engine_storage.folder_sync('pycaret_model')

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None):
        model_storage_path = self.model_storage.json_get('model_storage_path')

        model = load_model(f"{model_storage_path}/classification_model")

        return predict_model(model, df)