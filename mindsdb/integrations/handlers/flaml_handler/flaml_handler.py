import dill
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Dict, Optional
from type_infer.api import infer_types
from flaml import AutoML


class FLAMLHandler(BaseMLEngine):
    name = "FLAML"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if args is None:
            args = {}

        if df is not None:
            target_dtype = infer_types(df, 0).to_dict()["dtypes"][target]
            model = AutoML(verbose=0)

            if target_dtype in ['binary', 'categorical', 'tags']:
                model.fit(X_train=df.drop(columns=[target]),
                          y_train=df[target],
                          task='classification',
                          **args.get('using'))

            elif target_dtype in ['integer', 'float', 'quantity']:
                model.fit(X_train=df.drop(columns=[target]),
                          y_train=df[target],
                          task='regression',
                          **args.get('using'))

            self.model_storage.json_set('args', args)
            self.model_storage.file_set('model', dill.dumps(model))

        else:
            raise Exception(
                "Data is empty!!"
            )

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:

        model = dill.loads(self.model_storage.file_get("model"))
        target = self.model_storage.json_get('args').get("target")

        results = pd.DataFrame(model.predict(df), columns=[target])

        return results
