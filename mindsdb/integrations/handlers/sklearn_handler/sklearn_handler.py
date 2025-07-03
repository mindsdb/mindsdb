from typing import Optional
import dill
import pandas as pd
from type_infer.api import infer_types

from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

from mindsdb.integrations.libs.base import BaseMLEngine


class SklearnHandler(BaseMLEngine):
    """
    MindsDB ML engine using scikit-learn.
    Automatically selects LogisticRegression or LinearRegression
    based on the target column type.
    """
    name = 'sklearn'

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> None:
        target_dtype = infer_types(df).to_dict()["dtypes"][target]

        is_classification = target_dtype in ['binary', 'categorical', 'tags']
        model = LogisticRegression() if is_classification else LinearRegression()

        X = df.drop(columns=[target])
        y = df[target]

        cat_columns = X.select_dtypes(include=['object', 'category']).columns.tolist()

        preprocessor = ColumnTransformer(
            transformers=[('cat', OneHotEncoder(handle_unknown='ignore'), cat_columns)],
            remainder='passthrough'
        )

        pipeline = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('model', model)
        ])

        pipeline.fit(X, y)

        self.model_storage.file_set('model', dill.dumps(pipeline))

        try:
            self.model_storage.json_set('args', args)
        except Exception:
            pass

    def predict(self, df: Optional[pd.DataFrame] = None, args: Optional[dict] = None) -> pd.DataFrame:
        pipeline = dill.loads(self.model_storage.file_get('model'))
        df = df.drop('__mindsdb_row_id', axis=1, errors='ignore')

        predictions = pipeline.predict(df)

        try:
            args = self.model_storage.json_get('args')
        except Exception:
            args = {'target': 'target'}

        df[args['target']] = predictions
        return df
