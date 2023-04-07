from typing import Optional, Dict

import dill
import pandas as pd
import autosklearn.classification as automl_classification
import autosklearn.regression as automl_regression

from .config import ClassificationConfig, RegressionConfig

from mindsdb.integrations.libs.base import BaseMLEngine


class AutoSklearnHandler(BaseMLEngine):
    """
    Integration with the Auto-Sklearn ML library.
    """

    name = 'autosklearn'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        config_args = {key: val for key, val in args['using'].items() if key != 'task'}

        if args['using']['task'] == 'classification':
            config = ClassificationConfig(**config_args)

            model = automl_classification.AutoSklearnClassifier(**vars(config))

        elif args['using']['task'] == 'regression':
            config = RegressionConfig(**config_args)

            model = automl_regression.AutoSklearnRegressor(**vars(config))

        else:
            raise Exception('This task is not supported!')

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