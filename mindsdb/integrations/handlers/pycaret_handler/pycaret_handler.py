from typing import Optional, Dict

import dill
import pandas as pd
from pycaret.classification import setup, compare_models, predict_model, finalize_model

from mindsdb.integrations.libs.base import BaseMLEngine


class PyCaretHandler(BaseMLEngine):
    """
    Integration with the PyCaret ML library.
    """

    name = 'pycaret'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        grid = setup(data=df, target=target)

        best_model = compare_models()

        final_model = finalize_model(best_model)

        self.model_storage.file_set('model', dill.dumps(final_model))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model = dill.loads(self.model_storage.file_get('model'))

        return predict_model(model, df)