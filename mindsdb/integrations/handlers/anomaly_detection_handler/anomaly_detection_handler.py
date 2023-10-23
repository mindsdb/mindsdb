from mindsdb.integrations.libs.base import BaseMLEngine
from mindsdb.integrations.handlers.anomaly_detection_handler.utils import train_unsupervised, train_supervised, train_semisupervised
from joblib import dump, load
import pandas as pd
import numpy as np


class AnomalyDetectionHandler(BaseMLEngine):
    """Integration with the PyOD and CatBoost libraries for
    anomaly detection. Both supervised and unsupervised.
    """

    name = "anomaly_detection"

    # Write an init method that sets the generative attribute
    # to True if the model type is unsupervised, and False otherwise.

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create(self, target, df, args={}):
    # Save the column names of all numeric columns before transforming any categorical columns into dummies
        using_args = args["using"]
        target = "outlier" if target is None else target  # give it a name for unsupervised learning
        if target in df.columns:
            training_df = df.drop(target, axis=1)
        else:
            training_df = df[:]
        training_df = pd.get_dummies(training_df)
        model = train_unsupervised(training_df)
        # Save the model
        save_fp = 'model.joblib'
        dump(model, save_fp)
        model_args = {
            'model_path': save_fp,
            "target": target
        }
        self.model_storage.json_set('model_args', model_args)

    def predict(self, df, args={}):
        if "__mindsdb_row_id" in df.columns:
            df = df.drop("__mindsdb_row_id", axis=1)
        model_args = self.model_storage.json_get('model_args')
        model = load(model_args['model_path'])
        results = model.predict(df)
        return pd.DataFrame({model_args["target"]: results})
