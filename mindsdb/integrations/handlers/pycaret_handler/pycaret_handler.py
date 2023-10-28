from typing import Optional, Dict

import pandas as pd

from mindsdb.integrations.libs.base import BaseMLEngine


class PyCaretHandler(BaseMLEngine):
    name = 'pycaret'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Saves a model inside the engine registry for later usage.
        Normally, an input dataframe is required to train the model.
        However, some integrations may merely require registering the model instead of training, in which case `df` can be omitted.
        Any other arguments required to register the model can be passed in an `args` dictionary.
        """
        pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> pd.DataFrame:
        """
        Calls a model with some input dataframe `df`, and optionally some arguments `args` that may modify the model behavior.
        The expected output is a dataframe with the predicted values in the target-named column.
        Additional columns can be present, and will be considered row-wise explanations if their names finish with `_explain`.
        """
        pass

    def finetune(self, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        """
        Optional.
        Used to fine-tune a pre-existing model without resetting its internal state (e.g. weights).
        Availability will depend on underlying integration support, as not all ML models can be partially updated.
        """
        pass

    def describe(self, key: Optional[str] = None) -> pd.DataFrame:
        """
        Optional.
        When called, this method provides global model insights, e.g. framework-level parameters used in training.
        """
        pass
