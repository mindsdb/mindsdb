from typing import Optional, Dict

import dill
import pandas as pd
import sklearn
import autosklearn.regression

from mindsdb.integrations.libs.base import BaseMLEngine


class AutoSklearnHandler(BaseMLEngine):
    name = 'auto_sklearn'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None):
        X = df.copy()
        y = X.pop(target)

        X_train, X_test, y_train, y_test = sklearn.model_selection.train_test_split(
            X,
            y,
            random_state=1
        )

        regressor = autosklearn.regression.AutoSklearnRegressor(
            time_left_for_this_task=120,
            per_run_time_limit=30,
            tmp_folder="tmp/",
        )

        regressor.fit(X_train, y_train)

        self.model_storage.file_set('model', dill.dumps(regressor))

        # args['model_storage_path'] = model_storage_path
        # self.model_storage.json_set('args', args)

        self.engine_storage.folder_sync('auto_sklearn_model')

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None):
        # args = self.model_storage.json_get('args')
        # model_storage_path = args['model_storage_path']

        regressor = dill.loads(self.model_storage.file_get('model'))

        return regressor.predict(df)