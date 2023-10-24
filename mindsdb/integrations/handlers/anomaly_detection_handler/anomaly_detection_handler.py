from mindsdb.integrations.libs.base import BaseMLEngine
import pandas as pd
from mindsdb.integrations.handlers.anomaly_detection_handler.utils import (
    train_unsupervised,
    train_supervised,
    train_semisupervised,
)
from joblib import dump, load


def choose_model(df, model_type=None, target=None, supervised_threshold=3000):
    """Choose the best model based on the size of the dataset and the model type"""
    training_df = preprocess_data(df)
    if target is not None:
        X_train = training_df.drop(target, axis=1)
        y_train = training_df[target].astype(int)
    else:
        return train_unsupervised(training_df)

    # If data length is longer than threshold, choose supervised model
    if model_type is None:
        model_type = "supervised" if len(X_train) > supervised_threshold else "semi-supervised"
    assert model_type in ["supervised", "semi-supervised"], "model type must be 'supervised' or 'semi-supervised'"

    if model_type == "supervised":
        return train_supervised(X_train, y_train)
    elif model_type == "semi-supervised":
        return train_semisupervised(X_train, y_train)


def preprocess_data(df):
    """Preprocess the data by one-hot encoding categorical columns and scaling numeric columns"""
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
        self.generative = True  # makes unsupervised learning work

    def create(self, target, df, args={}):
        """Train a model and save it to the model storage"""
        using_args = args["using"]
        model_type = using_args["type"] if "type" in using_args else None
        model = choose_model(df, model_type=model_type, target=target)
        target = "outlier" if target is None else target  # output column name for unsupervised learning

        save_path = "model.joblib"
        dump(model, save_path)
        model_args = {"model_path": save_path, "target": target, "model_name": model.__class__.__name__}
        self.model_storage.json_set("model_args", model_args)

    def predict(self, df, args={}):
        """Load a model from the model storage and use it to make predictions"""
        model_args = self.model_storage.json_get("model_args")

        if "__mindsdb_row_id" in df.columns:
            df = df.drop("__mindsdb_row_id", axis=1)
        if model_args["target"] in df.columns:
            df = df.drop(model_args["target"], axis=1)
        predict_df = preprocess_data(df).astype(float)

        model = load(model_args["model_path"])
        results = model.predict(predict_df)
        return pd.DataFrame({model_args["target"]: results})

    def describe(self, attribute="model"):
        model_args = self.model_storage.json_get("model_args")
        if attribute == "model":
            return pd.DataFrame({k: [model_args[k]] for k in ["model_name", "target"]})
