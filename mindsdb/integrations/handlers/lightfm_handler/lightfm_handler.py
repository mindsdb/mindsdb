from typing import Optional, Dict

import dill
import pandas as pd
from lightfm import LightFM
from helpers import preprocess
from mindsdb.integrations.libs.base import BaseMLEngine


class LightFMHandler(BaseMLEngine):
    """
    Integration with the lightfm Recommender library.
    """

    name = 'lightfm'

    def create(self, data: pd.DataFrame = None, meta_data: dict = None, args: Optional[dict] = None) -> None:

        preprocess(data, **meta_data)

        model = LightFM(learning_rate=0.05, loss='warp')
        model.fit(data, epochs=10)

        self.model_storage.file_set('model', dill.dumps(model))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model = dill.loads(self.model_storage.file_get('model'))
        df = df.drop('__mindsdb_row_id', axis=1)

        predictions = model.predict(df)

        args = self.model_storage.json_get('args')
        df[args['target']] = predictions

        return df

