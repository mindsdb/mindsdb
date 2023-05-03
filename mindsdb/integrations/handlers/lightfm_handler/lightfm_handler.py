from typing import Optional, Dict

import dill
import pandas as pd
from lightfm import LightFM
from mindsdb.integrations.handlers.lightfm_handler.helpers import RecommenderPreprocessor
from mindsdb.integrations.libs.base import BaseMLEngine


class LightFMHandler(BaseMLEngine):
    """
    Integration with the lightfm Recommender library.
    """

    name = 'lightfm'

    def create(self, interaction_data: pd.DataFrame = None, item_data: pd.DataFrame = None, meta_data: Dict = None, args: Optional[Dict] = None) -> None:

        rec_preprocessor = RecommenderPreprocessor(
            interaction_data=interaction_data,
            item_data=item_data,
            **meta_data
        )

        preprocessed_data = rec_preprocessor.preprocess()

        model = LightFM(learning_rate=0.05, loss='warp')
        model.fit(preprocessed_data.interaction_matrix, epochs=10)

        # todo sort model storage
        self.model_storage.file_set('model', dill.dumps(model))
        self.model_storage.json_set('args', args)

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        model = dill.loads(self.model_storage.file_get('model'))
        df = df.drop('__mindsdb_row_id', axis=1)

        predictions = model.predict(df)

        args = self.model_storage.json_get('args')
        df[args['target']] = predictions

        return df

