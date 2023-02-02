from typing import Optional, Dict

import pandas as pd
from pycaret.classification import setup, compare_models, evaluate_model, predict_model, finalize_model, save_model, load_model


class PyCaretHandler(BaseMLEngine):
    """
    Integration with the PyCaret ML library.
    """

    name = 'pycaret'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        pass