from mindsdb.integrations.libs.base import BaseMLEngine
from pyod.models.ecod import ECOD  # unsupervised default
from pyod.models.xgbod import XGBOD  # semi-supervised default
from catboost import CatBoostClassifier  # supervised default
import pandas as pd


def choose_model(df, supervised_threshold=3000):
    if len(df) > supervised_threshold:
        return CatBoostClassifier()
    else:
        return XGBOD()


def preprocess_data(df):
    # one-hot encode categorical columns
    categorical_columns = list(df.select_dtypes(include=["object"]).columns.values)
    df[categorical_columns] = df[categorical_columns].astype("category")
    df[categorical_columns] = df[categorical_columns].apply(lambda x: x.cat.codes)
    df = pd.get_dummies(df, columns=categorical_columns)
    # scale numeric columns to have mean 0 and std 1
    numeric_columns = list(df.select_dtypes(include=["float64", "int64"]).columns.values)
    df[numeric_columns] = (df[numeric_columns] - df[numeric_columns].mean()) / df[numeric_columns].std()
    return df


class AnomalyDetectionHandler(BaseMLEngine):
    """Integration with the PyOD and CatBoost libraries for
    anomaly detection. Both supervised and unsupervised.
    """

    name = "anomaly_detection"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generative = True

    def create(self):
        pass

    def predict(self):
        pass
