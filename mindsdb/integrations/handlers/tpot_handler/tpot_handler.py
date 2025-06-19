import dill
import pandas as pd
from mindsdb.integrations.libs.base import BaseMLEngine
from typing import Dict, Optional
from type_infer.api import infer_types
from tpot import TPOTClassifier, TPOTRegressor
from sklearn.preprocessing import LabelEncoder


class TPOTHandler(BaseMLEngine):
    name = "TPOT"

    def create(self, target: str, df: Optional[pd.DataFrame] = None, args: Optional[Dict] = None) -> None:
        if args is None:
            args = {}
        type_of_cols = infer_types(df, 0).dtypes
        target_dtype = type_of_cols[target]

        if target_dtype in ['binary', 'categorical', 'tags']:
            model = TPOTClassifier(generations=args.get('generations', 10),
                                   population_size=args.get('population_size', 100),
                                   verbosity=0,
                                   max_time_mins=args.get('max_time_mins', None),
                                   n_jobs=args.get('n_jobs', -1))

        elif target_dtype in ['integer', 'float', 'quantity']:
            model = TPOTRegressor(generations=args.get('generations', 10),
                                  population_size=args.get('population_size', 100),
                                  verbosity=0,
                                  max_time_mins=args.get('max_time_mins', None),
                                  n_jobs=args.get('n_jobs', -1))

        if df is not None:
            # Separate out the categorical and non-categorical columns
            categorical_cols = [col for col, type_col in type_of_cols.items() if type_col in ('categorical', 'binary')]

            # Fit a LabelEncoder for each categorical column and store it in a dictionary
            le_dict = {}
            for col in categorical_cols:
                le = LabelEncoder()
                le.fit(df[col])
                le_dict[col] = le

                # Encode the categorical column using the fitted LabelEncoder
                df[col] = le.transform(df[col])

            model.fit(df.drop(columns=[target]), df[target])
            self.model_storage.json_set('args', args)
            self.model_storage.file_set('le_dict', dill.dumps(le_dict))
            self.model_storage.file_set('model', dill.dumps(model.fitted_pipeline_))
        else:
            raise Exception(
                "Data is empty!!"
            )

    def predict(self, df: pd.DataFrame, args: Optional[Dict] = None) -> pd.DataFrame:

        model = dill.loads(self.model_storage.file_get("model"))
        le_dict = dill.loads(self.model_storage.file_get("le_dict"))
        target = self.model_storage.json_get('args').get("target")

        # Encode the categorical columns in the input DataFrame using the saved LabelEncoders
        for col, le in le_dict.items():
            if col in df.columns:
                df[col] = le.transform(df[col])

        # Make predictions using the trained TPOT model
        results = pd.DataFrame(model.predict(df), columns=[target])

        # Decode the predicted categorical values back into their original values
        for col, le in le_dict.items():
            if col in results.columns:
                results[col] = le.inverse_transform(results[col])

        return results
