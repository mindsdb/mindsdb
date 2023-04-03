from typing import Optional, Dict

import dill
import pandas as pd
import autosklearn.classification as automl
from autosklearn.metrics import accuracy

from mindsdb.integrations.libs.base import BaseMLEngine


class AutoSklearnHandler(BaseMLEngine):
    """
    Integration with the Auto-Sklearn ML library.
    """

    name = 'autosklearn'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        if args['using']['task'] == 'classification':
            model = automl.AutoSklearnClassifier(
                time_left_for_this_task=60,
                per_run_time_limit=360,
                n_jobs=-1,
                metric=accuracy
            )
            model.fit(df.drop(target, axis=1), df[target])

        self.model_storage.file_set('model', dill.dumps(model))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model = dill.loads(self.model_storage.file_get('model'))
        df = df.drop('__mindsdb_row_id', axis=1)

        predictions = model.predict(df)

        args = self.model_storage.json_get('args')
        df[args['target']] = predictions

        return df