from typing import Optional

import dill
import pandas as pd

from autogluon.tabular import TabularPredictor
from type_infer.api import infer_types

from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.utilities import log
from .config import ClassificationConfig, RegressionConfig


logger = log.getLogger(__name__)


class AutoGluonHandler(BaseMLEngine):
    name = "autogluon"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        config_args = args['using']

        target_dtype = infer_types(df).to_dict()["dtypes"][target]

        if target_dtype in ['binary', 'categorical', 'tags']:
            config = ClassificationConfig(**config_args)

            model = TabularPredictor(label=target, )
        elif target_dtype in ['integer', 'float', 'quantity']:
            config = RegressionConfig(**config_args)

            model = TabularPredictor(label=target)

        else:
            raise Exception('This task is not supported!')

        model.fit(df, **vars(config))
        self.model_storage.file_set('model', dill.dumps(model))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model = dill.loads(self.model_storage.file_get('model'))
        df = df.drop('__mindsdb_row_id', axis=1)

        predictions = model.predict(df)

        args = self.model_storage.json_get('args')
        df[args['target']] = predictions

        return df

    def describe(self, attribute: Optional[str] = None) -> pd.DataFrame:

        args = self.model_storage.json_get("args")

        if attribute == "args":
            return pd.DataFrame(args.items(), columns=["key", "value"])
