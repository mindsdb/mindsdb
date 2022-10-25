from typing import Optional, Dict

import pickle
import pandas as pd
import sklearn
import autosklearn.regression

from mindsdb.integrations.libs.base import BaseMLEngine


class AutoSklearnHandler(BaseMLEngine):
    name = 'auto_sklearn'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None):
        y = df.pop(target)
        X = df.copy()

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

        model_storage_path = self.engine_storage.folder_get('auto_sklearn_model')

        with open(f"{model_storage_path}/regression_model.pkl", 'wb') as f:
            pickle.dump(regressor, f)

        args['model_storage_path'] = model_storage_path
        self.model_storage.json_set('args', args)

        self.engine_storage.folder_sync('auto_sklearn_model')

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None):
        args = self.model_storage.json_get('args')
        model_storage_path = args['model_storage_path']

        with open(f"{model_storage_path}/regression_model.pkl", 'rb') as f:
            regressor = pickle.load(f)

        return regressor.predict(df)